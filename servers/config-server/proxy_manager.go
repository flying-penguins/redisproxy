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
	"redisproxy/helper"
	"redisproxy/redis"
	"redisproxy/stats"
)

type ProxyInfo struct {
	CfgVersion string
	AliveTime  int64
	LinkChan   chan *redis.Proto
}

type proxySlotsReqStat struct {
	Clients    int
	AliveTime  int64
	StatTime   int64
	Interval   int64
	CfgVersion string
	SlotStat   stats.RequestStat
}

type ProxyManager struct {
	Proxys map[string]ProxyInfo
	stat1  map[string]stats.ProxyStat
	stat2  map[string]stats.ProxyStat
}

func NewProxyManager() *ProxyManager {
	return &ProxyManager{Proxys: make(map[string]ProxyInfo),
		stat1: make(map[string]stats.ProxyStat),
		stat2: make(map[string]stats.ProxyStat)}
}

func (s *ProxyManager) HeartBeat(addr string, ver string, link chan *redis.Proto) {
	s.Proxys[addr] = ProxyInfo{CfgVersion: ver, AliveTime: helper.MicroSecond(), LinkChan: link}

	s1, ok1 := s.stat1[addr]
	s2, ok2 := s.stat2[addr]
	if !ok1 {
		s.stat1[addr] = stats.ProxyStat{}
	} else {
		s.stat1[addr] = s1
	}

	if !ok2 {
		s.stat2[addr] = stats.ProxyStat{}
	} else {
		s.stat2[addr] = s2
	}
}

func (s *ProxyManager) SlotStat(addr string, stat stats.ProxyStat) {
	if _, ok := s.Proxys[addr]; !ok {
		return
	}

	if s.stat1[addr].StatTime <= s.stat2[addr].StatTime {
		s.stat1[addr] = stat
	} else {
		s.stat2[addr] = stat
	}
}

func (s *ProxyManager) AggregationSlotByProxy(begin, end int) map[string]proxySlotsReqStat {
	proxystats := make(map[string]proxySlotsReqStat)
	for k, v := range s.Proxys {
		s1, _ := s.stat1[k]
		s2, _ := s.stat2[k]

		total1 := stats.RequestStat{}
		total2 := stats.RequestStat{}
		for id := begin; id <= end && id < redis.MaxSlotNum; id++ {
			total1 = total1.Add(s1.SlotStat[id])
			total2 = total2.Add(s2.SlotStat[id])
		}

		stat := proxySlotsReqStat{CfgVersion: v.CfgVersion, AliveTime: v.AliveTime}
		if s1.StatTime >= s2.StatTime {
			stat.Clients = s1.Clients
			stat.StatTime = s1.StatTime
			stat.Interval = s1.StatTime - s2.StatTime
			stat.SlotStat = total1.Sub(total2)
		} else {
			stat.Clients = s2.Clients
			stat.StatTime = s2.StatTime
			stat.Interval = s2.StatTime - s1.StatTime
			stat.SlotStat = total2.Sub(total1)
		}

		proxystats[k] = stat
	}

	return proxystats
}

func (s *ProxyManager) Delete(addr string) {
	delete(s.Proxys, addr)
	delete(s.stat1, addr)
	delete(s.stat2, addr)
}
