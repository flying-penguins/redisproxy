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
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"redisproxy/helper"
	"redisproxy/logger"
	"redisproxy/redis"
	"redisproxy/stats"
	"redisproxy/yamlcfg"

	"redisproxy/servers/proxy-server/router"
)

var (
	globalConf *yamlcfg.Config = nil
	configFile                 = flag.String("c", "./etc_proxy.yaml", "config file, Usage<-c file>")
)

func init() {
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	var err error
	if globalConf, err = yamlcfg.ParseConfigFile(*configFile); err != nil {
		fmt.Printf("parse config file failed: %s %s", *configFile, err)
		return
	}

	logger.Config(globalConf.LogFile, globalConf.LogLevel)

	if server := NewServer(&globalConf.Cluster); server == nil {
		logger.Panicf("new server failed: %v", globalConf)
	} else {
		defer server.Close()
	}

	helper.StartHttpPProfRoutine()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM, os.Kill)
	sign := <-quit

	logger.Warnf("Exit signal found(%s), bye bye...:", sign)
}

type Server struct {
	clients      int32
	serverClosed bool
	router       *router.Router
	listener     net.Listener

	wait sync.WaitGroup
	lock sync.RWMutex
	conf *yamlcfg.ClusterCfg

	proxyStat stats.ProxyStat
}

func NewServer(conf *yamlcfg.ClusterCfg) *Server {
	listener, err := net.Listen("tcp", conf.Addr)
	if err != nil {
		logger.Panicf("open listener failed: %s", err)
		return nil
	}

	conf.GroupsVer = "0"
	s := &Server{conf: conf, router: router.New(), listener: listener}

	go s.configServerTask(conf.CfgServers)

	s.wait.Add(1)
	go func() {
		defer s.wait.Done()
		for {
			if s.serverClosed {
				logger.Noticef("server closed exit")
				break
			}

			client, err := s.listener.Accept()
			if err != nil {
				logger.Noticef("accept clients err: %s", err)
				continue
			}

			if atomic.LoadInt32(&s.clients) >= int32(s.conf.MaxClients) {
				logger.Noticef("clients count exceeded: %d(%d), addr: %s", s.conf.MaxClients, s.clients, client.RemoteAddr().String())
				client.Close()
				continue
			}

			logger.Infof("client addr: %s, clients: %d", client.RemoteAddr().String(), s.clients)
			go s.handleConn(client)
		}
	}()

	return s
}

func (s *Server) Close() {
	s.listener.Close()
	s.serverClosed = true
	time.Sleep(time.Millisecond * 100)
	if s.router != nil {
		s.router.Close()
	}

	s.wait.Wait()
}

func (s *Server) handleConn(client net.Conn) {
	atomic.AddInt32(&s.clients, 1)
	defer atomic.AddInt32(&s.clients, -1)

	conn := redis.Accept(client)
	conn.ReaderTimeout = time.Second * 60
	conn.WriterTimeout = time.Second * 10

	tasks := make(chan *router.Request, 1024)
	defer close(tasks)

	go s.handleResponse(conn, tasks)

	for {
		clientReq, err := conn.Read()
		if err != nil || s.serverClosed {
			logger.Infof("server-closed:%t client conn: %s, read err: %s", s.serverClosed, client.RemoteAddr().String(), err)
			return
		}

		req, err := s.handleRequest(clientReq)
		if err != nil {
			logger.Infof("client conn: %s, handleRequest err: %s", client.RemoteAddr().String(), err)
			return
		}

		tasks <- req
	}
}

func (s *Server) handleResponse(conn *redis.Conn, tasks chan *router.Request) {
	defer func() {
		conn.Close()
		for _ = range tasks {
		}
	}()

	for r := range tasks {
		r.Wait.Wait()
		handleTime := helper.MicroSecond() - r.Start
		s.statResponseInfo(r.Sid, r.Cmd, handleTime, r.HandleErr == nil)
		if handleTime < 20000 {
			logger.Debugf("R-R[%04d]: %s|%s, time: %d[%d]", r.Sid, r.Req.Overview(), r.Rsp.Overview(), r.Start, handleTime)
		} else if handleTime < 50000 {
			logger.Infof("R-R[%04d]: %s|%s, time: %d[%d]", r.Sid, r.Req.Overview(), r.Rsp.Overview(), r.Start, handleTime)
		} else if handleTime < 100000 {
			logger.Noticef("R-R[%04d]: %s|%s, time: %d[%d]", r.Sid, r.Req.Overview(), r.Rsp.Overview(), r.Start, handleTime)
		} else {
			logger.Warnf("R-R[%04d]: %s|%s, time: %d[%d]", r.Sid, r.Req.Overview(), r.Rsp.Overview(), r.Start, handleTime)
		}

		if r.ClosureFunc != nil {
			if err := r.ClosureFunc(); err != nil {
				logger.Warnf("ClosureFunc R-R[%04d]: %s|%s, time: %d[%d], err:%s", r.Sid, r.Req.Overview(), r.Rsp.Overview(), r.Start, handleTime, err)
				return
			}
		}

		if r.HandleErr != nil || r.Rsp == nil {
			logger.Warnf("handle R-R[%04d]: %s|%s, time: %d[%d], err:%s", r.Sid, r.Req.Overview(), r.Rsp.Overview(), r.Start, handleTime, r.HandleErr)
			return
		}

		flush := len(tasks) == 0
		if err := conn.Write(r.Rsp, flush); err != nil {
			logger.Warnf("write R-R[%04d]: %s|%s, time: %d[%d], err:%s", r.Sid, r.Req.Overview(), r.Rsp.Overview(), r.Start, handleTime, err)
			return
		}
	}
}

func (s *Server) handleRequest(req *redis.Proto) (*router.Request, error) {
	cmd, err := redis.GetCmdStr(req)
	if err != nil {
		return nil, err
	}
	if redis.IsNotAllowed(cmd) {
		return nil, fmt.Errorf("command <%s> is not allowed", cmd)
	}

	key := redis.GetKey(req, cmd)
	slotId := redis.HashSlot(key)
	s.statRequestInfo(slotId, cmd)
	r := &router.Request{
		Cmd:   cmd,
		Sid:   slotId,
		Key:   key,
		Start: helper.MicroSecond(),
		Req:   req,
		Wait:  &sync.WaitGroup{}}

	switch cmd {
	case "LOGLEVEL":
		if len(req.Array) >= 2 {
			level, _ := strconv.ParseInt(string(req.Array[1].Val), 10, 64)
			logger.SetLevel(int(level))
		}
		r.Rsp, r.HandleErr = redis.NewStatus([]byte("OK")), nil
	case "SELECT", "QUIT", "AUTH":
		r.Rsp, r.HandleErr = redis.NewStatus([]byte("OK")), nil
	case "PING":
		r.Rsp, r.HandleErr = redis.NewStatus([]byte("PONG")), nil
	case "MGET":
		return r, s.handleRequestMGet(r)
	case "MSET":
		return r, s.handleRequestMSet(r)
	case "DEL":
		return r, s.handleRequestMDel(r)
	default:
		return r, s.router.Dispatch(r, slotId)
	}

	return r, nil
}

func (s *Server) handleRequestMGet(r *router.Request) error {
	nkeys := len(r.Req.Array) - 1
	if nkeys <= 1 {
		return s.router.Dispatch(r, r.Sid)
	}

	var sub = make([]*router.Request, nkeys)
	for i := 0; i < len(sub); i++ {
		key := r.Req.Array[i+1].Val
		slotId := redis.HashSlot(key)

		sub[i] = &router.Request{
			Cmd:   r.Cmd,
			Sid:   slotId,
			Key:   key,
			Start: helper.MicroSecond(),
			Req: redis.NewArray([]*redis.Proto{
				redis.NewBulk([]byte(r.Cmd)),
				redis.NewBulk(key)}),
			Wait: r.Wait}

		if err := s.router.Dispatch(sub[i], slotId); err != nil {
			return err
		}
	}

	r.ClosureFunc = func() error {
		var array = make([]*redis.Proto, len(sub))
		for i, x := range sub {
			if x.HandleErr != nil || x.Rsp == nil {
				return fmt.Errorf("redis rsp err: %s, rsp: %p", x.HandleErr.Error(), x.Rsp)
			}

			if x.Rsp.Type != redis.TypeArray || len(x.Rsp.Array) != 1 {
				return fmt.Errorf("bad mget rsp: %s array.len = %d", x.Rsp.Type, len(x.Rsp.Array))
			}
			array[i] = x.Rsp.Array[0]
		}
		r.Rsp, r.HandleErr = redis.NewArray(array), nil
		return nil
	}

	return nil
}

func (s *Server) handleRequestMSet(r *router.Request) error {
	nkeys := len(r.Req.Array) - 1
	if nkeys <= 2 {
		return s.router.Dispatch(r, r.Sid)
	}

	if nkeys%2 != 0 {
		r.Rsp, r.HandleErr = redis.NewError([]byte("ERR wrong number of arguments for 'MSET' command")), nil
		return nil
	}

	var sub = make([]*router.Request, nkeys/2)
	for i := 0; i < len(sub); i++ {
		key := r.Req.Array[2*i+1].Val
		slotId := redis.HashSlot(key)

		sub[i] = &router.Request{
			Cmd:   r.Cmd,
			Sid:   slotId,
			Key:   key,
			Start: helper.MicroSecond(),
			Req: redis.NewArray([]*redis.Proto{
				redis.NewBulk([]byte(r.Cmd)),
				redis.NewBulk(key),
				redis.NewBulk(r.Req.Array[2*i+2].Val)}),
			Wait: r.Wait}

		if err := s.router.Dispatch(sub[i], slotId); err != nil {
			return err
		}
	}

	r.ClosureFunc = func() error {
		for _, x := range sub {
			if x.HandleErr != nil || x.Rsp == nil {
				return fmt.Errorf("redis rsp err: %s, rsp: %p", x.HandleErr.Error(), x.Rsp)
			}

			if x.Rsp.Type != redis.TypeStatus || string(x.Rsp.Val) != "OK" {
				return fmt.Errorf("bad mset rsp: %s val = %s", x.Rsp.Type, string(x.Rsp.Val))
			}
		}

		r.Rsp, r.HandleErr = redis.NewStatus([]byte("OK")), nil

		return nil
	}

	return nil
}

func (s *Server) handleRequestMDel(r *router.Request) error {
	nkeys := len(r.Req.Array) - 1
	if nkeys <= 1 {
		return s.router.Dispatch(r, r.Sid)
	}

	var sub = make([]*router.Request, nkeys)
	for i := 0; i < len(sub); i++ {
		key := r.Req.Array[i+1].Val
		slotId := redis.HashSlot(key)

		sub[i] = &router.Request{
			Cmd:   r.Cmd,
			Sid:   slotId,
			Key:   key,
			Start: helper.MicroSecond(),
			Req: redis.NewArray([]*redis.Proto{
				redis.NewBulk([]byte(r.Cmd)),
				redis.NewBulk(key)}),
			Wait: r.Wait}

		if err := s.router.Dispatch(sub[i], slotId); err != nil {
			return err
		}
	}

	r.ClosureFunc = func() error {
		count := 0
		for _, x := range sub {
			if x.HandleErr != nil || x.Rsp == nil {
				return fmt.Errorf("redis rsp err: %s, rsp: %p", x.HandleErr.Error(), x.Rsp)
			}

			if x.Rsp.Type != redis.TypeInt || len(x.Rsp.Val) != 1 {
				return fmt.Errorf("bad del rsp: %s array.len = %d", x.Rsp.Type, len(x.Rsp.Val))
			}

			if x.Rsp.Val[0] != '0' {
				count++
			}
		}
		r.Rsp, r.HandleErr = redis.NewInt(redis.Itob(int64(count))), nil
		return nil
	}

	return nil
}

func (s *Server) statRequestInfo(sid int, cmd string) {
	atomic.AddInt64(&s.proxyStat.SlotStat[sid].Total, 1)
}

func (s *Server) statResponseInfo(sid int, cmd string, us int64, ok bool) {
	atomic.AddInt64(&s.proxyStat.SlotStat[sid].Hand, 1)
	atomic.AddInt64(&s.proxyStat.SlotStat[sid].Time, us)
	if ok {
		atomic.AddInt64(&s.proxyStat.SlotStat[sid].Succ, 1)
	}
}
