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
	"sync/atomic"

	"redisproxy/helper"
	"redisproxy/logger"
	"redisproxy/redis"
)

type Slot struct {
	id int

	backend *BackendConn
	migrate *BackendConn

	changeToMaster  string
	changeToMigrate string

	lock    sync.RWMutex
	holding uint32
}

func (s *Slot) Close() {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.backend != nil {
		s.backend.Close()
	}

	if s.migrate != nil {
		s.migrate.Close()
	}
}
func (s *Slot) Holdon(master, migrate string) {
	if atomic.LoadUint32(&s.holding) == 1 {
		return
	}

	s.lock.Lock()

	needToHold := false
	if s.backend != nil && s.backend.Addr() != master {
		s.backend.Close()
		needToHold = true
	}

	if s.migrate != nil && s.migrate.Addr() != migrate {
		s.migrate.Close()
		needToHold = true
	}

	if s.backend == nil && master != "" {
		needToHold = true
	}

	if s.migrate == nil && migrate != "" {
		needToHold = true
	}

	s.changeToMaster = master
	s.changeToMigrate = migrate

	if needToHold {
		atomic.StoreUint32(&s.holding, 1)
	} else {
		s.lock.Unlock()
	}
}

func (s *Slot) Release(commit bool) {
	if atomic.LoadUint32(&s.holding) == 0 {
		return
	}

	if commit {
		if s.changeToMaster != "" {
			s.backend = NewBackendConn(s.changeToMaster, s.id)
		} else {
			s.backend = nil
		}

		if s.changeToMigrate != "" {
			s.migrate = NewBackendConn(s.changeToMigrate, s.id)
		} else {
			s.migrate = nil
		}
	} else {
		if s.backend != nil {
			s.backend = NewBackendConn(s.backend.Addr(), s.id)
		}

		if s.migrate != nil {
			s.migrate = NewBackendConn(s.migrate.Addr(), s.id)
		}
	}

	s.lock.Unlock()

	atomic.StoreUint32(&s.holding, 0)
}

func (s *Slot) Forward(r *Request, key []byte) error {
	s.lock.RLock()
	defer s.lock.RUnlock()

	if err := s.prepare(r, key); err != nil {
		return err
	}

	s.backend.PushBack(r)
	return nil
}

func (s *Slot) prepare(r *Request, key []byte) error {
	if s.backend == nil {
		return fmt.Errorf("slot-%04d is not ready: key = %s", s.id, string(key))
	}

	return s.migratekey(r, key)
}

func (s *Slot) migratekey(r *Request, key []byte) error {
	if len(key) == 0 || s.migrate == nil {
		return nil
	}

	move := &Request{
		Cmd:   "MIGRATE",
		Key:   key,
		Start: helper.MicroSecond(),
		Req:   redis.MigrateMsg(s.backend.Addr(), key, s.id),
		Wait:  &sync.WaitGroup{}}
	s.migrate.PushBack(move)
	move.Wait.Wait()
	if move.HandleErr == nil && move.Rsp != nil && move.Rsp.Type == redis.TypeStatus &&
		(string(move.Rsp.Val) == "OK" || string(move.Rsp.Val) == "NOKEY") {
		logger.Debugf("slot-%04d migrate from %s to %s: key = %s", s.id, s.migrate.Addr(), s.backend.Addr(), key)
		return nil
	}

	return fmt.Errorf("slot-%04d migrate from %s to %s: key = %s,err: %s",
		s.id, s.migrate.Addr(), s.backend.Addr(), key, move.HandleErr)
}
