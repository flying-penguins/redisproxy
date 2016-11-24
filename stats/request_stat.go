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

package stats

import (
	"fmt"

	"redisproxy/redis"
)

type ProxyStat struct {
	Clients  int
	StatTime int64
	SlotStat [redis.MaxSlotNum]RequestStat
}

type RequestStat struct {
	Total int64 // recv total request
	Hand  int64 // handle request
	Time  int64 // handle time
	Succ  int64 // handle success count
}

func (me RequestStat) Sub(r RequestStat) RequestStat {
	stat := RequestStat{}
	stat.Total = me.Total - r.Total
	stat.Hand = me.Hand - r.Hand
	stat.Time = me.Time - r.Time
	stat.Succ = me.Succ - r.Succ

	return stat
}

func (me RequestStat) Add(r RequestStat) RequestStat {
	stat := RequestStat{}
	stat.Total = me.Total + r.Total
	stat.Hand = me.Hand + r.Hand
	stat.Time = me.Time + r.Time
	stat.Succ = me.Succ + r.Succ

	return stat
}

func (me RequestStat) Str(interval int64) string {
	if me.Total < 0 || me.Hand < 0 || me.Time < 0 || me.Succ < 0 {
		me.Total = 0
		me.Hand = 0
		me.Time = 0
		me.Succ = 0
	}

	return fmt.Sprintf("Q:%d T:%dus R:%2.2f",
		1000000*me.Total/(interval+1),
		me.Time/(me.Hand+1),
		100.0*(float32(me.Succ)+1.0)/(float32(me.Total)+1.0))
}
