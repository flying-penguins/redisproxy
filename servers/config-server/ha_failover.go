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
	"sort"
	"strconv"
	"time"

	"redisproxy/helper"
	"redisproxy/logger"
	"redisproxy/redis"
)

func (s *Server) timerCheckMasterStatus() {
	var checkStatusTime int64 = 0
	const checkStatusInterval int64 = 5 * 1000000

	for i := 0; ; i++ {
		time.Sleep(time.Second)
		now := helper.MicroSecond()
		if s.role != "master" {
			continue
		}

		/****************************************************/
		if now-checkStatusTime > checkStatusInterval {
			checkStatusTime = helper.MicroSecond()
			groups := s.CopyConfigGroups(true)
			for _, group := range groups {
				if infoMap, err := getReplicationInfo(group.Master); err == nil {
					infoMap["alivetime"] = strconv.FormatInt(helper.MicroSecond(), 10)
					s.lock.Lock()
					s.mastersStatus[group.Master] = infoMap
					s.lock.Unlock()
				} else {
					logger.Warnf("%s: get Info Replication err: %s", group.Master, err)
				}
			}
		}

		s.checkToFailover()
	}
}

func (s *Server) checkToFailover() {
	s.lock.Lock()
	defer s.lock.Unlock()

	groups := s.CopyConfigGroups(false)
	for i := 0; i < len(groups); i++ {
		masterStatus, exist := s.mastersStatus[groups[i].Master]
		if !exist {
			continue
		}

		alivetime, _ := strconv.ParseInt(masterStatus["alivetime"], 10, 64)
		if helper.MicroSecond()-alivetime <= s.conf.RedisDownTime*1000000 {
			continue
		}

		if s.migrating {
			logger.Errorf("migrate is in progress..., abandon failover")
			return
		}

		slaves := parseSlaveInfoAndSort(groups[i].Master, masterStatus)
		logger.Noticef("failover master: %s, status: %+v, slaves: %+v", groups[i].Master, masterStatus, slaves)
		for _, slave := range slaves {
			if err := redisSlaveof(slave.Addr, "NO", "ONE"); err == nil {
				groups[i].Master = slave.Addr
				if err := broadcastGroupsToProxy(s, helper.DevNullFile, groups, helper.FuncArgc0RetErrNil); err == nil {
					delete(s.mastersStatus, groups[i].Master)
				} else {
					logger.Errorf("broadcastGroupsToProxy err: %s", err)
				}
				return
			}
		}
	}
}

func redisSlaveof(addr, ip, port string) error {
	conn, err := redis.Connect(addr, 1*time.Second)
	if err != nil {
		return fmt.Errorf("connect to redis err[%s]: %s", addr, err)
	}
	conn.ReaderTimeout = 2 * time.Second
	conn.WriterTimeout = 1 * time.Second
	defer conn.Close()

	slaveCmd := redis.NewArray([]*redis.Proto{
		redis.NewBulk([]byte("SLAVEOF")),
		redis.NewBulk([]byte(ip)),
		redis.NewBulk([]byte(port))})

	reply, err := conn.Cmd(slaveCmd)
	if err == nil && reply != nil && reply.Type == redis.TypeStatus && string(reply.Val) == "OK" {
		return nil
	}

	return fmt.Errorf("%s slaveof %s:%s err: %s", addr, ip, port, err)
}

func parseSlaveInfoAndSort(master string, masterStatus map[string]string) []SlaveInfo {
	if masterStatus == nil {
		return nil
	}

	slaves := make([]SlaveInfo, 0)
	for i := 0; ; i++ {
		slaveIndex := "slave" + strconv.FormatInt(int64(i), 10)
		v, ok := masterStatus[slaveIndex]
		if !ok {
			break
		}

		slaveInfoMap := helper.SegmentToMap(v, ",", "=")
		addr := slaveInfoMap["ip"] + ":" + slaveInfoMap["port"]
		offset, _ := strconv.ParseInt(slaveInfoMap["offset"], 10, 64)
		slaves = append(slaves, SlaveInfo{Addr: addr, Offset: offset})
	}
	sort.Sort(SlaveInfoSlice(slaves))

	return slaves
}

func getReplicationInfo(addr string) (map[string]string, error) {
	conn, err := redis.Connect(addr, 1*time.Second)
	if err != nil {
		return nil, fmt.Errorf("connect to redis err[%s]: %s", addr, err)

	}
	conn.ReaderTimeout = 1 * time.Second
	conn.WriterTimeout = 1 * time.Second
	defer conn.Close()

	infoCmd := redis.NewArray([]*redis.Proto{
		redis.NewBulk([]byte("INFO")),
		redis.NewBulk([]byte("Replication"))})

	reply, err := conn.Cmd(infoCmd)
	if err != nil || reply == nil || reply.Type != redis.TypeBulk {
		return nil, fmt.Errorf("info Replication cmd err: %s", err)
	}

	return helper.SegmentToMap(string(reply.Val), "\r\n", ":"), nil
}

func findAddrInSlaveInfoSlice(infos []SlaveInfo, addr string) (SlaveInfo, bool) {
	for i := 0; i < len(infos); i++ {
		if infos[i].Addr == addr {
			return infos[i], true
		}
	}

	return SlaveInfo{"", 0}, false
}

type SlaveInfo struct {
	Addr   string
	Offset int64
}

type SlaveInfoSlice []SlaveInfo

func (a SlaveInfoSlice) Len() int {
	return len(a)
}
func (a SlaveInfoSlice) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a SlaveInfoSlice) Less(i, j int) bool {
	return a[j].Offset < a[i].Offset
}
