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
	"errors"
	"fmt"
	"sync"
	"time"

	"redisproxy/logger"
	"redisproxy/redis"
)

type BackendConn struct {
	useDb int
	addr  string
	input chan *Request

	stop sync.Once
	wait sync.WaitGroup
}

func NewBackendConn(addr string, db int) *BackendConn {
	bc := &BackendConn{
		useDb: db,
		addr:  addr,
		input: make(chan *Request, 4096),
	}
	bc.wait.Add(1)

	go func() {
		defer bc.wait.Done()

		logger.Noticef("[%04d]backend conn [%p] to %s, start service", db, bc, bc.addr)
		for k := 0; ; k++ {
			err := bc.newLoop()
			if err == nil {
				break
			}

			logger.Noticef("[%d] backend conn [%p] to %s, restart,err: %s", k, bc, bc.addr, err)
			time.Sleep(time.Millisecond * 100)
		}
		logger.Noticef("[%04d]backend conn [%p] to %s, stop and exit", db, bc, bc.addr)
	}()

	return bc
}

func (bc *BackendConn) Addr() string {
	return bc.addr
}

func (bc *BackendConn) Close() {
	bc.stop.Do(func() {
		close(bc.input)
	})

	bc.wait.Wait()
}

func (bc *BackendConn) PushBack(r *Request) {
	if r.Wait != nil {
		r.Wait.Add(1)
	}
	bc.input <- r
}

func (bc *BackendConn) newLoop() error {
	conn, err := redis.Connect(bc.addr, 2*time.Second)
	if err != nil {
		return err
	}
	conn.ReaderTimeout = time.Second * 60
	conn.WriterTimeout = time.Second * 10

	selectCmd := redis.NewArray([]*redis.Proto{
		redis.NewBulk([]byte("SELECT")),
		redis.NewBulk(redis.Itob(int64(bc.useDb))),
	})

	reply, err := conn.Cmd(selectCmd)
	if err != nil || reply == nil || reply.Type != redis.TypeStatus || string(reply.Val) != "OK" {
		conn.Close()
		return fmt.Errorf("select db failed, err: %s", err)
	}

	tasks := make(chan *Request, 4096)
	defer close(tasks)

	go bc.newBackendReader(conn, tasks)

	for r := range bc.input {
		flush := len(bc.input) == 0
		if err := conn.Write(r.Req, flush); err != nil {
			bc.setResponse(r, nil, err)
			return fmt.Errorf("Write Err: %s", err)
		}

		tasks <- r
	}

	return nil
}

func (bc *BackendConn) newBackendReader(conn *redis.Conn, tasks chan *Request) {
	defer func() {
		conn.Close()
		for r := range tasks {
			bc.setResponse(r, nil, errors.New("read err found"))
		}
	}()

	for r := range tasks {
		rsp, err := conn.Read()
		bc.setResponse(r, rsp, err)
		if err != nil {
			logger.Warnf("recv from backend err: %s", err)
			break
		}
	}
}

func (bc *BackendConn) setResponse(r *Request, rsp *redis.Proto, err error) {
	r.Rsp, r.HandleErr = rsp, err
	if r.Wait != nil {
		r.Wait.Done()
	}
}
