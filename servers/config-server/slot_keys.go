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
	"io"
	"strconv"
	"time"

	"redisproxy/helper"
	"redisproxy/logger"
	"redisproxy/redis"
	"redisproxy/yamlcfg"
)

func (s *Server) timerUpdateSlotKeys() {
	var statDbSizeTime int64 = 0
	const statDbSizeInterval int64 = 10 * 1000000
	for i := 0; ; i++ {
		time.Sleep(time.Second)
		now := helper.MicroSecond()
		if s.role != "master" {
			continue
		}

		/****************************************************/
		if now-statDbSizeTime > statDbSizeInterval {
			statDbSizeTime = helper.MicroSecond()
			groups := s.CopyConfigGroups(true)
			for _, group := range groups {
				dbsizeMap := getSlotDbsize(group.Master, group.Slots[0], group.Slots[1])

				s.lock.Lock()
				for id := group.Slots[0]; id <= group.Slots[1] && id < redis.MaxSlotNum; id++ {
					s.slotKeys[id] = dbsizeMap[id]
				}
				s.lock.Unlock()
			}
		}
	}
}

func migrateGroupKeys(group yamlcfg.GroupCfg, client io.Writer) {
	for slot := group.Slots[0]; slot <= group.Slots[1] && slot < redis.MaxSlotNum; slot++ {
		client.Write([]byte(CtlNewline))
		slotKeys := 0
		for {
			if slotKeys = getSlotDbsize(group.Migrate, slot, slot)[slot]; slotKeys >= 0 {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}

		migrateSlotKeysOver := false
		for !migrateSlotKeysOver {
			migrateJobQuit := false
			migrateQuitChan := migrateSlotKeys(group.Migrate, group.Master, slot)
			for !migrateJobQuit {
				select {
				case <-migrateQuitChan:
					migrateJobQuit = true
				case <-time.After(time.Second * 1):
				}

				remainKey := getSlotDbsize(group.Migrate, slot, slot)[slot]
				bar := helper.ProgressBarStr(uint64(slotKeys-remainKey), uint64(slotKeys))
				client.Write([]byte(CtlNewEnter + fmt.Sprintf("migrate slot-%04d: %s", slot, bar)))
				if remainKey == 0 {
					migrateSlotKeysOver = true
				}
			}
		}
	}
}

func migrateSlotKeys(src, dst string, slot int) chan int {
	logger.Noticef("migrate slot-%04d: %s -> %s", slot, src, dst)
	migrateQuitChan := make(chan int, 1)

	go func() {
		defer close(migrateQuitChan)

		conn, err := redis.Connect(src, 2*time.Second)
		if err != nil {
			logger.Errorf("connect to redis err[%s]: %s", src, err)
			return
		}
		conn.ReaderTimeout = 1 * time.Second
		conn.WriterTimeout = 1 * time.Second
		defer conn.Close()

		selectCmd := redis.NewArray([]*redis.Proto{
			redis.NewBulk([]byte("SELECT")),
			redis.NewBulk(redis.Itob(int64(slot)))})
		reply, err := conn.Cmd(selectCmd)
		if err != nil || reply == nil || reply.Type != redis.TypeStatus || string(reply.Val) != "OK" {
			logger.Errorf("select db cmd err: %s", err)
			return
		}

		scanCount := int64(100)
		scanCursor := int64(0)
		for {
			scanCmd := redis.NewArray([]*redis.Proto{
				redis.NewBulk([]byte("SCAN")),
				redis.NewBulk(redis.Itob(scanCursor)),
				redis.NewBulk([]byte("COUNT")),
				redis.NewBulk(redis.Itob(scanCount))})
			reply, err = conn.Cmd(scanCmd)
			if err != nil || reply == nil || reply.Type != redis.TypeArray || len(reply.Array) != 2 ||
				reply.Array[1].Type != redis.TypeArray {
				logger.Errorf("scan cmd err: %s", err)
				return
			}

			scanCursor, err = strconv.ParseInt(string(reply.Array[0].Val), 10, 64)
			if err != nil {
				logger.Errorf("scan cmd parse cursor err: %s", err)
				return
			}

			for i := 0; i < len(reply.Array[1].Array); i++ {
				reply, err := conn.Cmd(redis.MigrateMsg(dst, reply.Array[1].Array[i].Val, slot))
				if err != nil || reply == nil || reply.Type != redis.TypeStatus || string(reply.Val) != "OK" {
					logger.Errorf("migrate cmd err %s", err)
					return
				}
			}

			if scanCursor == 0 {
				logger.Noticef("migrate slot-%04d: %s -> %s; scan cursor is 0 over!", slot, src, dst)
				return
			}
		}
	}()

	return migrateQuitChan
}

func getSlotDbsize(addr string, begin, end int) map[int]int {
	dbszieMap := make(map[int]int)
	for slotid := begin; slotid <= end && slotid < redis.MaxSlotNum; slotid++ {
		dbszieMap[slotid] = -1
	}

	conn, err := redis.Connect(addr, 2*time.Second)
	if err != nil {
		logger.Errorf("%s: get Slot Dbsize err: %s", addr, err)
		return dbszieMap
	}
	conn.ReaderTimeout = time.Second
	conn.WriterTimeout = time.Second
	defer conn.Close()

	for slotid := begin; slotid <= end && slotid < redis.MaxSlotNum; slotid++ {
		selectCmd := redis.NewArray([]*redis.Proto{
			redis.NewBulk([]byte("SELECT")),
			redis.NewBulk(redis.Itob(int64(slotid))),
		})

		reply, err := conn.Cmd(selectCmd)
		if err != nil || reply == nil || reply.Type != redis.TypeStatus || string(reply.Val) != "OK" {
			logger.Errorf("select db cmd err: %s", err)
			return dbszieMap
		}

		dbszieCmd := redis.NewArray([]*redis.Proto{
			redis.NewBulk([]byte("DBSIZE")),
		})

		reply, err = conn.Cmd(dbszieCmd)
		if err != nil || reply == nil || reply.Type != redis.TypeInt {
			logger.Errorf("dbszie cmd err: %s", err)
			return dbszieMap
		}

		count, _ := strconv.Atoi(string(reply.Val))
		dbszieMap[slotid] = count
	}

	return dbszieMap
}
