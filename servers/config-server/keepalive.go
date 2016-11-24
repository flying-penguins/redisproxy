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
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"time"

	"redisproxy/helper"
	"redisproxy/logger"
	"redisproxy/redis"
	"redisproxy/yamlcfg"
)

func (s *Server) enterConfigMode(conn *redis.Conn) error {
	defer conn.Close()

	conn.WriterTimeout = time.Second * 2
	conn.ReaderTimeout = time.Second * 60
	for {
		req, err := conn.Read()
		if e, ok := err.(*net.OpError); ok && e.Timeout() {
			continue
		}

		if err != nil || req == nil || req.Type != redis.TypeArray || len(req.Array) <= 0 {
			return fmt.Errorf("recv from proxy server err: %s", err)
		}

		if string(req.Array[0].Val) == "KEEPALIVE" && len(req.Array) >= 4 {
			s.handleConfigKeepaliveReq(conn, string(req.Array[1].Val), string(req.Array[2].Val), req.Array[3].Val)
		}
	}

	return nil
}

func (s *Server) handleConfigKeepaliveReq(conn *redis.Conn, peerRole, peerVer string, peerGroups []byte) {
	logger.Noticef("self role: %s, ver: %s; peer role:%s, ver: %s, ", s.role, s.conf.GroupsVer, peerRole, peerVer)
	if err := conn.Write(s.createKeepaliveMsg(), true); err != nil {
		logger.Warnf("send err: %s", err)
	}
}

func (s *Server) timerConfigServerKeepalive(peerAddr string) {
	logger.Noticef("config server peer addr: %s", peerAddr)
	if peerAddr == "" {
		s.role = "master"
		return
	}

	recentCommSuccTime := helper.MicroSecond()
	for {
		if helper.MicroSecond()-recentCommSuccTime > int64(s.conf.KeepaliveTime*1000000) {
			logger.Noticef("long time can't connect to peer: %s, promote to master(%s)", peerAddr, s.role)
			s.role = "master"
		}

		time.Sleep(time.Second)
		conn, err := redis.Connect(peerAddr, time.Second*2)
		if err != nil {
			logger.Warnf("connect to config peer err: %s", err)
			continue
		}
		conn.WriterTimeout = time.Second * 2
		conn.ReaderTimeout = time.Second * 2
		conn.Write(redis.NewInt([]byte("config")), true)

		randNum := rand.New(rand.NewSource(time.Now().UnixNano()))
		for {
			time.Sleep(time.Second * time.Duration(randNum.Intn(30)+1))
			if err := conn.Write(s.createKeepaliveMsg(), true); err != nil {
				logger.Warnf("send err: %s", err)
				break
			}

			rsp, err := conn.Read()
			if err != nil || rsp == nil || rsp.Type != redis.TypeArray || len(rsp.Array) <= 0 {
				logger.Warnf("recv from config server err: %s", err)
				break
			}

			recentCommSuccTime = helper.MicroSecond()
			if string(rsp.Array[0].Val) == "KEEPALIVE" && len(rsp.Array) >= 4 {
				s.handleConfigKeepaliveRsp(string(rsp.Array[1].Val), string(rsp.Array[2].Val), rsp.Array[3].Val)
			}
		}

		conn.Close()
	}
}

func (s *Server) handleConfigKeepaliveRsp(peerRole, peerVer string, peerGroups []byte) {
	s.lock.Lock()
	defer s.lock.Unlock()

	logger.Noticef("self role: %s, ver: %s; peer role:%s, ver: %s", s.role, s.conf.GroupsVer, peerRole, peerVer)
	groups, err := yamlcfg.UnmarshalGroups(peerGroups)
	if err != nil {
		logger.Errorf("unmarshal groups err: %s", err)
		return
	}

	peerVerInt, _ := strconv.ParseUint(peerVer, 10, 64)
	selfVerInt, _ := strconv.ParseUint(s.conf.GroupsVer, 10, 64)
	if selfVerInt == peerVerInt {
		if peerRole == "master" {
			s.role = "slave"
		} else {
			s.role = "master"
		}
	} else if selfVerInt < peerVerInt {
		s.role = "slave"
		s.conf.Groups = groups
		s.conf.GroupsVer = peerVer
		err := yamlcfg.SaveConfigToFile(globalConf, *configFile)
		logger.Noticef("SaveConfigToFile: ret: %s %s, %+v", err, *configFile, globalConf)
	} else if selfVerInt > peerVerInt {
		s.role = "master"
	}
	logger.Noticef("result self role: %s, ver: %s; peer role:%s, ver: %s", s.role, s.conf.GroupsVer, peerRole, peerVer)

	if s.role == "master" {
		return
	}

	for k, v := range s.proxyMgr.Proxys {
		v.LinkChan <- nil
		logger.Noticef("self role: %s, close proxy: %s", s.role, k)
	}
}

func (s *Server) createKeepaliveMsg() *redis.Proto {
	s.lock.Lock()
	defer s.lock.Unlock()

	data, err := yamlcfg.MarshalGroups(s.conf.Groups)
	if err != nil {
		logger.Errorf("MarshalGroups err: %s", err)
		return nil
	}

	msg := redis.NewArray([]*redis.Proto{
		redis.NewBulk([]byte("KEEPALIVE")),
		redis.NewBulk([]byte(s.role)),
		redis.NewBulk([]byte(s.conf.GroupsVer)),
		redis.NewBulk(data)})

	return msg
}
