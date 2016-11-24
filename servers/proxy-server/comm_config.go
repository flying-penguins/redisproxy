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
	"bytes"
	"encoding/gob"
	"fmt"
	"net"
	"time"

	"redisproxy/helper"
	"redisproxy/logger"
	"redisproxy/redis"
	"redisproxy/yamlcfg"
)

func (s *Server) configServerTask(cfgAddr []string) {
	tasks := make(chan *redis.Proto, 1024)
	defer close(tasks)

	logger.Infof("start configServerTask: %v", cfgAddr)
	for k := 0; ; k++ {
		err := s.configServerLoop(cfgAddr[k%len(cfgAddr)], tasks)
		if err == nil {
			break
		}

		for i := 0; i < len(tasks); i++ {
			<-tasks
		}

		logger.Noticef("[%d]configServerTask restart(%v),err: %s", k, cfgAddr, err)
		time.Sleep(time.Millisecond * 1000)
	}
	logger.Infof("configServerTask(%s) stop and exit", cfgAddr)
}

func (s *Server) configServerLoop(addr string, tasks chan *redis.Proto) error {
	client, err := net.DialTimeout("tcp", addr, 1*time.Second)
	if err != nil {
		return err
	}

	conn := redis.Accept(client)
	defer conn.Close()

	heartBeatTimer := time.NewTicker(5 * time.Second)
	reportStatTimer := time.NewTicker(10 * time.Second)
	defer heartBeatTimer.Stop()
	defer reportStatTimer.Stop()

	tasks <- redis.NewInt([]byte("proxy"))
	tasks <- s.createHeartBeatMsg()
	for {
		select {
		case r := <-tasks:
			conn.WriterTimeout = time.Second * 1
			if err := conn.Write(r, true); err != nil {
				return fmt.Errorf("Write Err: %s", err)
			}

		case <-heartBeatTimer.C:
			if msg := s.createHeartBeatMsg(); msg != nil {
				tasks <- msg
			}

		case <-reportStatTimer.C:
			if msg := s.createReportStatMsg(); msg != nil {
				tasks <- msg
			}

		default:
			conn.ReaderTimeout = time.Millisecond * 100
			req, err := conn.Read()
			if e, ok := err.(*net.OpError); ok && e.Timeout() {
				logger.Debugf("no config server cmd")
				continue
			}

			if err != nil || req == nil || req.Type != redis.TypeArray || len(req.Array) <= 0 {
				return fmt.Errorf("recv from config server err: %s", err)
			}

			if string(req.Array[0].Val) == "CHGGROUPS" && len(req.Array) >= 4 {
				if string(req.Array[2].Val) == "commit" {
					s.handleGroupsPrepareAndCommit(string(req.Array[1].Val), req.Array[3].Val)
				} else {
					s.handleGroupsPrepare(conn, string(req.Array[1].Val), req.Array[3].Val)
				}
			}
		}
	}

	return nil
}

func (s *Server) handleGroupsPrepareAndCommit(ver string, data []byte) {
	logger.Noticef("handle groups %s-%s", s.conf.GroupsVer, ver)
	groups, err := yamlcfg.UnmarshalGroups(data)
	if err != nil {
		logger.Errorf("unmarshal groups err: %s", err)
		return
	}

	if err := s.router.Update(groups); err != nil {
		logger.Errorf("update router info failed: %s", err)
		return
	}

	s.router.Commit(true)
	s.conf.GroupsVer = ver
	s.conf.Groups = groups
	err = yamlcfg.SaveConfigToFile(globalConf, *configFile)
	logger.Noticef("SaveConfigToFile: err: %s %s, %+v", err, *configFile, globalConf)
}

func (s *Server) handleGroupsPrepare(conn *redis.Conn, ver string, data []byte) {
	logger.Noticef("handle groups %s-%s", s.conf.GroupsVer, ver)
	groups, err := yamlcfg.UnmarshalGroups(data)
	if err != nil {
		logger.Errorf("unmarshal groups err: %s", err)
		conn.Write(createVoteGroupsMsg(ver, "reject"), true)
		return
	}

	if err := s.router.Update(groups); err != nil {
		conn.Write(createVoteGroupsMsg(ver, "reject"), true)
		logger.Errorf("update router info failed: %s, reject: %s", err, ver)
		return
	}

	conn.Write(createVoteGroupsMsg(ver, "accept"), true)
	logger.Noticef("handle groups accept: %s", ver)

	conn.ReaderTimeout = time.Millisecond * 3000
	req, err := conn.Read()
	if err != nil || req == nil || req.Type != redis.TypeArray || len(req.Array) < 3 || string(req.Array[2].Val) != "done" {
		logger.Warnf("wait config server commmit msg err: %s %+v", err, req)
		s.router.Commit(false)
		return
	}

	s.router.Commit(true)
	s.conf.GroupsVer = ver
	s.conf.Groups = groups
	err = yamlcfg.SaveConfigToFile(globalConf, *configFile)
	logger.Noticef("SaveConfigToFile: %s, %+v, err: %s", *configFile, globalConf, err)
}

func (s *Server) createHeartBeatMsg() *redis.Proto {
	msg := redis.NewArray([]*redis.Proto{
		redis.NewBulk([]byte("HEARTBEAT")),
		redis.NewBulk([]byte(s.conf.Addr)),
		redis.NewBulk([]byte(s.conf.GroupsVer)),
	})

	return msg
}

func createVoteGroupsMsg(ver, vote string) *redis.Proto { //accept reject
	msg := redis.NewArray([]*redis.Proto{
		redis.NewBulk([]byte("CHGGROUPS")),
		redis.NewBulk([]byte(ver)),
		redis.NewBulk([]byte(vote)),
	})

	return msg
}

func (s *Server) createReportStatMsg() *redis.Proto {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	s.proxyStat.Clients = int(s.clients)
	s.proxyStat.StatTime = helper.MicroSecond()
	if err := encoder.Encode(&s.proxyStat); err != nil {
		logger.Errorf("enode stat err: %s", err)
		return nil
	}

	msg := redis.NewArray([]*redis.Proto{
		redis.NewBulk([]byte("SLOTSTAT")),
		redis.NewBulk(buf.Bytes()),
	})

	return msg
}
