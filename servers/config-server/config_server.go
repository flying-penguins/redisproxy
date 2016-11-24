// Copyright 2016 The Redis-cloud Authors. All rights reserved.
// E-mail: liuyun827@foxmail.com
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"

	"redisproxy/helper"
	"redisproxy/logger"
	"redisproxy/redis"
	"redisproxy/yamlcfg"
)

var (
	globalConf *yamlcfg.Config = nil
	configFile *string         = flag.String("c", "./etc_config.yaml", "config file, Usage<-c file>")
)

func init() {
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	var err error
	if globalConf, err = yamlcfg.ParseConfigFile(*configFile); err != nil {
		fmt.Printf("parse config file failed: %s %s", *configFile, err)
		return
	}

	logger.Config(globalConf.LogFile, globalConf.LogLevel)

	if server := NewServer(&globalConf.Cluster); server == nil {
		logger.Panicf("new server failed: %v", globalConf)
	} else {
		defer server.Close()
	}

	helper.StartHttpPProfRoutine()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM, os.Kill)
	sign := <-quit

	logger.Warnf("Exit signal found(%s), bye bye...:", sign)
}

type ChgGroupVote struct {
	addr string
	ver  string
	vote string
}

type Server struct {
	role         string
	migrating    bool
	serverClosed bool
	listener     net.Listener

	wait sync.WaitGroup
	lock sync.RWMutex
	conf *yamlcfg.ClusterCfg

	proxyMgr *ProxyManager
	slotKeys map[int]int
	voteChan chan ChgGroupVote

	mastersStatus map[string]map[string]string
}

func NewServer(conf *yamlcfg.ClusterCfg) *Server {
	listener, err := net.Listen("tcp", conf.Addr)
	if err != nil {
		logger.Panicf("open listener failed: %s", err)
		return nil
	}

	s := &Server{conf: conf, listener: listener}
	s.proxyMgr = NewProxyManager()
	s.voteChan = make(chan ChgGroupVote, 2048)
	s.slotKeys = make(map[int]int)
	s.mastersStatus = make(map[string]map[string]string)

	go s.timerUpdateSlotKeys()
	go s.timerCheckMasterStatus()
	go s.timerConfigServerKeepalive(s.conf.CfgServers[0])

	go s.startupMigrateKeys(s.CopyConfigGroups(true))

	s.wait.Add(1)
	go func() {
		defer s.wait.Done()
		for {
			if s.serverClosed {
				logger.Noticef("server closed exit")
				break
			}

			client, err := s.listener.Accept()
			if err != nil {
				logger.Noticef("accept err: %s", err)
				continue
			}

			go s.handleConn(client)
		}
	}()

	return s
}

func (s *Server) handleConn(client net.Conn) {
	logger.Infof("role: %s; client conn: %s", s.role, client.RemoteAddr().String())
	conn := redis.Accept(client)
	defer conn.Close()

	clientReq, err := conn.Read()
	if err != nil || clientReq == nil {
		logger.Infof("client conn: %s, read err: %s", client.RemoteAddr().String(), err)
		return
	}

	if s.role == "master" && clientReq.Type == redis.TypeInt && string(clientReq.Val) == "cmd" {
		err = s.enterTelnetMode(client)
	} else if s.role == "master" && clientReq.Type == redis.TypeInt && string(clientReq.Val) == "proxy" {
		err = s.enterProxyMode(conn)
	} else if clientReq.Type == redis.TypeInt && string(clientReq.Val) == "config" {
		err = s.enterConfigMode(conn)
	} else {
		_, err = client.Write([]byte(fmt.Sprintf("role: %s, %c", s.role, clientReq.Type)))
	}

	logger.Noticef("role: %s;client conn: %s, %s mode quit err: %s", s.role, client.RemoteAddr().String(), string(clientReq.Val), err)
}

func (s *Server) startupMigrateKeys(groups []yamlcfg.GroupCfg) {
	s.lock.Lock()
	defer s.lock.Unlock()

	needToMigrate := false
	s.migrating = true
	for i := 0; i < len(groups); i++ {
		if groups[i].Migrate == "" {
			continue
		}

		s.lock.Unlock()
		migrateGroupKeys(groups[i], helper.DevNullFile)
		s.lock.Lock()

		groups[i].Migrate = ""
		needToMigrate = true
	}
	s.migrating = false

	if !needToMigrate {
		logger.Noticef("startup not need to migrate")
		return
	}

	if err := broadcastGroupsToProxy(s, helper.DevNullFile, groups, helper.FuncArgc0RetErrNil); err != nil {
		logger.Errorf("startup migrate broadcast groups err: %s", err)
		return
	}

	logger.Noticef("Start Up Migrate Over, Success")
}

func (s *Server) Close() {
	s.listener.Close()
	s.serverClosed = true

	s.wait.Wait()
}

func (s *Server) CopyConfigGroups(needLock bool) []yamlcfg.GroupCfg {
	if needLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	return yamlcfg.CopyGroupCfgSlice(s.conf.Groups)
}
