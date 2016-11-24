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
	"io"
	"net"
	"strconv"
	"time"

	"redisproxy/helper"
	"redisproxy/logger"
	"redisproxy/redis"
	"redisproxy/stats"
	"redisproxy/yamlcfg"
)

func (s *Server) enterProxyMode(conn *redis.Conn) error {
	proxyAddr := ""
	tasks := make(chan *redis.Proto, 1024)

	defer func() {
		s.lock.Lock()
		close(tasks)
		conn.Close()
		s.proxyMgr.Delete(proxyAddr)
		s.lock.Unlock()
	}()

	for {
		select {
		case r := <-tasks:
			conn.WriterTimeout = time.Second * 2
			if err := conn.Write(r, true); err != nil {
				return fmt.Errorf("Write Err: %s", err)
			}

		default:
			conn.ReaderTimeout = time.Millisecond * 100
			req, err := conn.Read()
			if e, ok := err.(*net.OpError); ok && e.Timeout() {
				logger.Debugf("no config server cmd")
				continue
			}

			if err != nil || req == nil || req.Type != redis.TypeArray || len(req.Array) <= 0 {
				return fmt.Errorf("recv from proxy server err: %s", err)
			}

			if string(req.Array[0].Val) == "HEARTBEAT" && len(req.Array) >= 3 {
				proxyAddr = string(req.Array[1].Val)
				s.handleProxyHeartBeat(conn, proxyAddr, string(req.Array[2].Val), tasks)
			} else if string(req.Array[0].Val) == "SLOTSTAT" && len(req.Array) >= 2 {
				s.handleProxySlotStat(conn, proxyAddr, req.Array[1].Val)
			} else if string(req.Array[0].Val) == "CHGGROUPS" && len(req.Array) >= 3 {
				s.handleProxyChgGroupsVote(conn, proxyAddr, string(req.Array[1].Val), string(req.Array[2].Val))
			}
		}
	}

	return nil
}

func (s *Server) handleProxyHeartBeat(conn *redis.Conn, addr string, ver string, link chan *redis.Proto) {
	s.lock.Lock()
	defer s.lock.Unlock()

	//logger.Infof("proxy addr: %s group config version: (%s-%s)", addr, s.conf.GroupsVer, ver)
	s.proxyMgr.HeartBeat(addr, ver, link)
	if ver != s.conf.GroupsVer {
		if msg := createPrepareGroupsMsg(s.conf.Groups, s.conf.GroupsVer, "commit"); msg != nil {
			conn.Write(msg, true)
		}
	}
}

func (s *Server) handleProxySlotStat(conn *redis.Conn, addr string, statBytes []byte) {
	s.lock.Lock()
	defer s.lock.Unlock()

	var stat stats.ProxyStat
	buf := bytes.NewBuffer(statBytes)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&stat); err != nil || addr == "" {
		logger.Warnf("proxy addr: %s, decode slot err: %s", addr, err)
		return
	}

	s.proxyMgr.SlotStat(addr, stat)
}

func (s *Server) handleProxyChgGroupsVote(conn *redis.Conn, addr, ver, vote string) {
	select {
	case s.voteChan <- ChgGroupVote{addr: addr, ver: ver, vote: vote}:
	case <-time.After(time.Millisecond * 10):
	}

	logger.Noticef("CHGGROUPS recv vote proxy:%s ver:%s(%s)", addr, ver, vote)
}

func broadcastGroupsToProxy(s *Server, client io.Writer, groups []yamlcfg.GroupCfg, voteSuccCb func() error) error {
	/************************ Phase I **********************/
	versionUint, _ := strconv.ParseUint(s.conf.GroupsVer, 10, 64)
	versionStr := strconv.FormatUint(versionUint+1, 10)
	msg := createPrepareGroupsMsg(groups, versionStr, "prepare")
	if msg == nil {
		client.Write([]byte(CtlNewline + "Marshal slots msg error"))
		return fmt.Errorf("Marshal slots msg error")
	}

	proxyCount := len(s.proxyMgr.Proxys)
	voteResult := collectAndCountVote(s.voteChan, client, versionStr, proxyCount)
	for addr, proxy := range s.proxyMgr.Proxys {
		proxy.LinkChan <- msg
		logger.Noticef("CHGGROUPS update to proxy:%s ver:%s", addr, versionStr)
		client.Write([]byte(CtlNewline + fmt.Sprintf("update to:  %s, ver:%s", addr, versionStr)))
	}

	/************************ Vote Result **********************/
	acceptCount := <-voteResult
	rejectCount := <-voteResult

	/************************ Commit **********************/
	commitAction := "abort"
	if acceptCount == proxyCount && voteSuccCb() == nil {
		commitAction = "done"
	}
	msg = createCommitGroupsMsg(versionStr, commitAction)
	resultInfo := fmt.Sprintf("ver:%s(%s) [total:%d,accept:%d,reject:%d]", versionStr, commitAction, proxyCount, acceptCount, rejectCount)
	for addr, proxy := range s.proxyMgr.Proxys {
		proxy.LinkChan <- msg
		logger.Noticef("CHGGROUPS commit to proxy:%s, %s", addr, resultInfo)
		client.Write([]byte(CtlNewline + fmt.Sprintf("commit to:  %s, %s", addr, resultInfo)))
	}

	if commitAction != "done" {
		return fmt.Errorf("abort %s", resultInfo)
	}

	s.conf.Groups = groups
	s.conf.GroupsVer = versionStr
	err := yamlcfg.SaveConfigToFile(globalConf, *configFile)
	logger.Noticef("CHGGROUPS save config: %s, %+v, err: %s", *configFile, globalConf, err)

	return nil
}

func collectAndCountVote(voteChan chan ChgGroupVote, client io.Writer, ver string, proxyCount int) chan int {
	for i := 0; i < len(voteChan); i++ {
		<-voteChan
	}

	result := make(chan int, 128)
	go func() {
		defer close(result)

		acceptCount := 0
		rejectCount := 0
		beginTime := helper.MicroSecond()
		for acceptCount+rejectCount != proxyCount {
			select {
			case rsp := <-voteChan:
				if rsp.vote == "accept" {
					acceptCount++
				} else {
					rejectCount++
				}
				logger.Noticef("CHGGROUPS count vote proxy:%s ver:%s %s", rsp.addr, rsp.ver, rsp.vote)
				client.Write([]byte(CtlNewline + fmt.Sprintf("count vote: %s, ver:%s,%s", rsp.addr, rsp.ver, rsp.vote)))

			case <-time.After(time.Millisecond * 100):
				//do nothing, just for the next loop
			}

			if helper.MicroSecond()-beginTime > 3*1000000 {
				break
			}
		}

		result <- acceptCount
		result <- rejectCount
	}()

	return result
}

func createPrepareGroupsMsg(groups []yamlcfg.GroupCfg, version, action string) *redis.Proto { // prepare commit
	data, err := yamlcfg.MarshalGroups(groups)
	if err != nil {
		logger.Errorf("MarshalGroups: (%+v), err: %s", groups, err)
		return nil
	}

	msg := redis.NewArray([]*redis.Proto{
		redis.NewBulk([]byte("CHGGROUPS")),
		redis.NewBulk([]byte(version)),
		redis.NewBulk([]byte(action)),
		redis.NewBulk(data)})

	return msg
}

func createCommitGroupsMsg(ver, commit string) *redis.Proto { //done abort
	msg := redis.NewArray([]*redis.Proto{
		redis.NewBulk([]byte("CHGGROUPS")),
		redis.NewBulk([]byte(ver)),
		redis.NewBulk([]byte(commit))})

	return msg
}
