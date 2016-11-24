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
	"net"
	"strconv"
	"strings"
	"time"

	"redisproxy/helper"
	"redisproxy/logger"
	"redisproxy/redis"
	"redisproxy/stats"
	"redisproxy/yamlcfg"
)

const (
	CtlClearScreen     string        = "\033[2J"
	CtlCursorReset     string        = "\033[H"
	CtlNewEnter        string        = "\r\033[K"
	CtlNewline         string        = "\r\n\033[K"
	telnetCmdPrompt    string        = "\r\nCMD> "
	ExitString         string        = "exit"
	telnetCmdFreshTime time.Duration = 1
)

var telnetCmdHelpStr string = `
=======================================================================
exit                   --- exit telnet cmd mode or fresh screen
group                  --- view groups info: slotid keys;proxy(qps hand-time succ-rate); rediss...
proxy                  --- view proxys info: addr conns qps hand-time succ-rate
keyslot key            --- get slotid by key: slotid
migrate 000 555 master --- process
promote addr1 as addr2 --- process
-----------------------------------------------------------------------`

func (s *Server) enterTelnetMode(client net.Conn) error {
	client.Write([]byte(telnetCmdPrompt))
	for i := 0; ; i++ {
		inputCmd, inputArgv, err := netConnReadCmdArgv(client, 1000000)
		logger.Noticef("telnet input: %s: %v, err: %s", inputCmd, inputArgv, err)
		if err != nil {
			return err
		}

		client.Write([]byte(CtlNewline + time.Now().String()))
		if inputCmd == "group" {
			err = telnetGroupStatCmd(s, client, inputArgv)
		} else if inputCmd == "proxy" {
			err = telnetProxyStatCmd(s, client, inputArgv)
		} else if inputCmd == "keyslot" && len(inputArgv) >= 1 {
			err = telnetKeySlotCmd(s, client, inputArgv)
		} else if inputCmd == "migrate" && len(inputArgv) >= 3 {
			err = telnetMigrateSlotCmd(s, client, inputArgv)
		} else if inputCmd == "promote" && len(inputArgv) >= 3 {
			err = telnetPromoteCmd(s, client, inputArgv)
		} else if inputCmd == "exit" {
			err = fmt.Errorf("recv exit cmd, exit!")
		} else if inputCmd == "loglevel" && len(inputArgv) >= 1 {
			level, _ := strconv.ParseInt(string(inputArgv[0]), 10, 64)
			logger.SetLevel(int(level))
			client.Write([]byte(CtlNewline + "OK"))
		} else {
			client.Write([]byte(telnetCmdHelpStr))
		}

		client.Write([]byte(CtlNewline + time.Now().String()))

		if err == nil {
			client.Write([]byte(telnetCmdPrompt))
		} else {
			client.Write([]byte(fmt.Sprintf("cmd err: %s", err.Error())))
			return err
		}
	}

	return nil
}

func netConnReadCmdArgv(client net.Conn, timeout time.Duration) (string, []string, error) {
	client.SetReadDeadline(time.Now().Add(time.Second * timeout))

	data := make([]byte, 1024)
	n, err := client.Read(data)

	if e, ok := err.(*net.OpError); ok && e.Timeout() {
		return "", nil, nil
	}

	if err != nil || n < 0 || n >= 1024 {
		return "", nil, err
	}

	inputItems := make([]string, 0)
	strs := strings.Split(strings.Trim(string(data[0:n]), "\r\n"), " ")
	for _, str := range strs {
		if str != "" {
			inputItems = append(inputItems, str)
		}
	}

	if len(inputItems) == 0 {
		return "", nil, nil
	}

	return inputItems[0], inputItems[1:], nil
}

func telnetKeySlotCmd(s *Server, client net.Conn, argv []string) error {
	slotid := redis.HashSlot([]byte(argv[0]))
	client.Write([]byte(fmt.Sprintf("%s%s : slotid-%04d", CtlNewline, argv[0], slotid)))
	return nil
}

func telnetGroupStatCmd(s *Server, client net.Conn, argv []string) error {
	client.Write([]byte(CtlClearScreen))
	for {
		s.lock.Lock()
		output := CtlCursorReset + time.Now().String()

		for _, g := range s.conf.Groups {
			slavesInfo := "["
			slaves := parseSlaveInfoAndSort(g.Master, s.mastersStatus[g.Master])
			for _, s := range slaves {
				slavesInfo += s.Addr + "," + strconv.FormatInt(s.Offset, 10) + " "
			}
			slavesInfo += "]"

			masterReplOffset := ""
			if v, ok := s.mastersStatus[g.Master]; ok {
				masterReplOffset = v["master_repl_offset"]
			}
			masterInfo := fmt.Sprintf("%s,%s", g.Master, masterReplOffset)

			output += CtlNewline + slotsStatInfo(s, g.Slots[0], g.Slots[1]) + masterInfo + slavesInfo + g.Migrate
		}
		s.lock.Unlock()

		if _, err := client.Write([]byte(output)); err != nil {
			logger.Infof("write to telnet err: %s", err)
			break
		}

		if inputCmd, _, readErr := netConnReadCmdArgv(client, telnetCmdFreshTime); readErr != nil {
			return readErr
		} else if inputCmd == ExitString {
			return nil
		}
	}

	return nil
}

func telnetProxyStatCmd(s *Server, client net.Conn, argv []string) error {
	client.Write([]byte(CtlClearScreen))
	for {
		output := CtlCursorReset + time.Now().String()

		s.lock.Lock()
		proxystats := s.proxyMgr.AggregationSlotByProxy(0, redis.MaxSlotNum)
		for k, stat := range proxystats {
			proxyInfo := fmt.Sprintf("C:%d A:%d S:%d V:%s", stat.Clients,
				(helper.MicroSecond()-stat.AliveTime)/1000000,
				(helper.MicroSecond()-stat.StatTime)/1000000, stat.CfgVersion)
			output += CtlNewline + k + " " + proxyInfo + " " + stat.SlotStat.Str(stat.Interval)
		}
		s.lock.Unlock()

		if _, err := client.Write([]byte(output)); err != nil {
			logger.Infof("write to telnet err: %s", err)
			break
		}

		if inputCmd, _, readErr := netConnReadCmdArgv(client, telnetCmdFreshTime); readErr != nil {
			return readErr
		} else if inputCmd == ExitString {
			return nil
		}
	}

	return nil
}

func telnetMigrateSlotCmd(s *Server, client net.Conn, argv []string) error {
	if s.migrating {
		client.Write([]byte("A migrate job is in progress, please retry later."))
		return nil
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	begin, err1 := strconv.Atoi(argv[0])
	end, err2 := strconv.Atoi(argv[1])
	if err1 != nil || err2 != nil || begin < 0 || begin >= redis.MaxSlotNum || end < 0 || end >= redis.MaxSlotNum {
		client.Write([]byte(CtlNewline + "Cmd err: parse slot begin-end wrong"))
		return nil
	}

	groups, reshard := reshardSlotGroups(s.CopyConfigGroups(false), begin, end, string(argv[2]))
	if !reshard {
		client.Write([]byte(CtlNewline + "Please check the cmd not effect"))
		return nil
	}

	if err := broadcastGroupsToProxy(s, client, groups, helper.FuncArgc0RetErrNil); err != nil {
		logger.Errorf("%s", err)
		client.Write([]byte(CtlNewline + "Broadcast Setslot Failed!"))
		return nil
	}
	client.Write([]byte(CtlNewline + "Setslot Success! Migrate..."))

	s.migrating = true
	for i := 0; i < len(groups); i++ {
		if groups[i].Migrate == "" {
			continue
		}

		s.lock.Unlock()
		migrateGroupKeys(groups[i], client)
		s.lock.Lock()

		groups[i].Migrate = ""
	}
	s.migrating = false

	if err := broadcastGroupsToProxy(s, client, groups, helper.FuncArgc0RetErrNil); err != nil {
		logger.Errorf("%s", err)
		client.Write([]byte(CtlNewline + "Broadcast Migrate Over Failed!"))
		return nil
	}
	client.Write([]byte(CtlNewline + "Migrate Over, Success"))

	return nil
}

func telnetPromoteCmd(s *Server, client net.Conn, argv []string) error {
	if s.migrating {
		client.Write([]byte(CtlNewline + "A migrate job is in progress, please retry later."))
		return nil
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	newMaster, master := argv[0], argv[2]
	mastersStatus, err := getReplicationInfo(master)
	if err != nil {
		client.Write([]byte(CtlNewline + master + ": Info Replication err: " + err.Error()))
		return nil
	}

	foundNewMasterIsSlave := false
	groups := s.CopyConfigGroups(false)
	for i := 0; i < len(groups); i++ {
		if groups[i].Master != master {
			continue
		}

		slaves := parseSlaveInfoAndSort(master, mastersStatus)
		if info, exist := findAddrInSlaveInfoSlice(slaves, newMaster); exist {
			foundNewMasterIsSlave = true
			masterReplOffset, _ := strconv.ParseInt(mastersStatus["master_repl_offset"], 10, 64)
			if masterReplOffset-info.Offset < 0 || masterReplOffset-info.Offset > 1024*100 {
				client.Write([]byte(fmt.Sprintf(CtlNewline+"promote time was not right, offset:%d %d", masterReplOffset, info.Offset)))
				return nil
			}
		}

		groups[i].Master = newMaster
	}
	if !foundNewMasterIsSlave {
		client.Write([]byte(CtlNewline + master + "-" + newMaster + " is not master-slave pair"))
		return nil
	}

	voteSuccCb := func() error {
		beginTime := helper.MicroSecond()
		for {
			time.Sleep(time.Millisecond * 100)
			if helper.MicroSecond()-beginTime > 2*1000000 {
				break
			}

			mastersStatus, err := getReplicationInfo(master)
			if err != nil {
				client.Write([]byte(fmt.Sprintf(CtlNewline+"%s: Info Replication err: %s", master, err)))
				continue
			}

			slaves := parseSlaveInfoAndSort(master, mastersStatus)
			if info, exist := findAddrInSlaveInfoSlice(slaves, newMaster); exist {
				masterReplOffset, _ := strconv.ParseInt(mastersStatus["master_repl_offset"], 10, 64)
				if masterReplOffset == info.Offset {
					client.Write([]byte(fmt.Sprintf(CtlNewline+"%s->%s: wait for sync, success!", master, newMaster)))
					if err := redisSlaveof(newMaster, "NO", "ONE"); err == nil {
						client.Write([]byte(fmt.Sprintf(CtlNewline+"%s: slaveof no one, success!", newMaster)))
						return nil
					}
				}
			}
		}

		return fmt.Errorf("%s->%s: wait for sync timeout(2s)", master, newMaster)
	}

	if err := broadcastGroupsToProxy(s, client, groups, voteSuccCb); err != nil {
		logger.Errorf("%s", err)
		client.Write([]byte(CtlNewline + "Broadcast Setslot Failed!"))
		return nil
	}
	client.Write([]byte(CtlNewline + "Promote Success! "))

	return nil
}

func reshardSlotGroups(srcgroups []yamlcfg.GroupCfg, begin, end int, master string) ([]yamlcfg.GroupCfg, bool) {
	reshard := false
	groups := make([]yamlcfg.GroupCfg, 0)
	for _, g := range srcgroups {
		if g.Slots[0] == begin && g.Slots[1] > end {
			reshard = true
			group := yamlcfg.GroupCfg{
				Slots:   []int{begin, end},
				Master:  master,
				Migrate: g.Master,
			}
			groups = append(groups, group)

			g.Slots[0] = end + 1
			groups = append(groups, g)
		} else if g.Slots[0] == begin && g.Slots[1] == end {
			reshard = true
			group := yamlcfg.GroupCfg{
				Slots:   []int{begin, end},
				Master:  master,
				Migrate: g.Master,
			}
			groups = append(groups, group)
		} else if g.Slots[0] < begin && g.Slots[1] == end {
			reshard = true
			g.Slots[1] = begin - 1
			groups = append(groups, g)

			group := yamlcfg.GroupCfg{
				Slots:   []int{begin, end},
				Master:  master,
				Migrate: g.Master,
			}
			groups = append(groups, group)

		} else {
			groups = append(groups, g)
		}
	}

	return groups, reshard
}

func slotsStatInfo(s *Server, begin, end int) string {
	slotInfo := fmt.Sprintf("[%04d-%04d] ", begin, end)

	proxystats := s.proxyMgr.AggregationSlotByProxy(begin, end)

	totalStat := stats.RequestStat{}
	proxyInfo := ""
	for k, v := range proxystats {
		totalStat = totalStat.Add(v.SlotStat)
		proxyInfo += k + "(" + v.SlotStat.Str(v.Interval) + ");"
	}

	slotKeysSum := 0
	for id := begin; id <= end && id < redis.MaxSlotNum; id++ {
		slotKeysSum += s.slotKeys[id]
	}

	return slotInfo + "KEYS:" + strconv.Itoa(slotKeysSum) + ";" + proxyInfo
}
