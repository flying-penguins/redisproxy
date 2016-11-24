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

package router

import (
	"fmt"
	"sync"

	"redisproxy/logger"
	"redisproxy/redis"
	"redisproxy/yamlcfg"
)

type Request struct {
	Cmd   string
	Sid   int
	Key   []byte
	Start int64

	Wait *sync.WaitGroup
	Req  *redis.Proto
	Rsp  *redis.Proto

	HandleErr   error
	ClosureFunc func() error
}

type Router struct {
	lock sync.Mutex

	slots [redis.MaxSlotNum]*Slot
}

func New() *Router {
	r := &Router{}
	for i := 0; i < redis.MaxSlotNum; i++ {
		r.slots[i] = &Slot{id: i}
	}
	return r
}

func (s *Router) Close() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	for i := 0; i < redis.MaxSlotNum; i++ {
		s.slots[i].Close()
	}

	return nil
}

func (s *Router) Dispatch(r *Request, slotId int) error {
	slot := s.slots[slotId]
	err := slot.Forward(r, r.Key)
	return err
}

func (s *Router) Update(groups []yamlcfg.GroupCfg) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if err := checkGroupsValidate(groups); err != nil {
		return err
	}

	for i := 0; i < redis.MaxSlotNum; i++ {
		group := findGroupBySlotId(groups, i)
		s.slots[i].Holdon(group.Master, group.Migrate)
	}

	return nil
}

func (s *Router) Commit(commit bool) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	for i := 0; i < redis.MaxSlotNum; i++ {
		s.slots[i].Release(commit)
	}
	logger.Noticef("commit groups config %t....", commit)

	return nil
}

func checkGroupsValidate(groups []yamlcfg.GroupCfg) error {
	for i := 0; i < redis.MaxSlotNum; i++ {
		if findGroupBySlotId(groups, i) == nil {
			return fmt.Errorf("slot: %04d can't find config", i)
		}
	}

	return nil
}

func findGroupBySlotId(groups []yamlcfg.GroupCfg, slotId int) *yamlcfg.GroupCfg {
	for i := 0; i < len(groups); i++ {
		if len(groups[i].Slots) != 2 {
			return nil
		}

		if slotId >= groups[i].Slots[0] && slotId <= groups[i].Slots[1] {
			return &groups[i]
		}
	}

	return nil
}
