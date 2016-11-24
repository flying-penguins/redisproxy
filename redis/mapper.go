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

package redis

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"strings"

	"redisproxy/helper"
)

const MaxSlotNum = 1024

var blacklist = make(map[string]bool)

func init() {
	for _, s := range []string{"KEYS", "MOVE", "OBJECT", "RENAME", "RENAMENX", "SCAN", "BITOP", "MSETNX", "MIGRATE",
		"RESTORE", "BLPOP", "BRPOP", "BRPOPLPUSH", "PSUBSCRIBE", "PUBLISH", "PUNSUBSCRIBE", "SUBSCRIBE", "RANDOMKEY",
		"UNSUBSCRIBE", "DISCARD", "EXEC", "MULTI", "UNWATCH", "WATCH", "SCRIPT", "BGREWRITEAOF", "BGSAVE", "CLIENT",
		"CONFIG", "DBSIZE", "DEBUG", "FLUSHALL", "FLUSHDB", "LASTSAVE", "MONITOR", "SAVE", "SHUTDOWN", "SLAVEOF",
		"SLOWLOG", "SYNC", "TIME"} {
		blacklist[s] = true
	}
}

func IsNotAllowed(opstr string) bool {
	return blacklist[opstr]
}

func GetCmdStr(resp *Proto) (string, error) {
	if resp.Type != TypeArray || len(resp.Array) == 0 {
		return "", fmt.Errorf("bad request type for command")
	}

	for _, r := range resp.Array {
		if r.Type != TypeBulk {
			return "", fmt.Errorf("bad request type for command(item)")
		}
	}

	var op = resp.Array[0].Val
	if len(op) == 0 || len(op) > 64 {
		return "", fmt.Errorf("bad command length, too short or too long")
	}

	return strings.ToUpper(helper.String(op)), nil
}

func GetKey(resp *Proto, opstr string) []byte {
	var index = 1
	switch opstr {
	case "ZINTERSTORE", "ZUNIONSTORE", "EVAL", "EVALSHA":
		index = 3
	}
	if index < len(resp.Array) {
		return resp.Array[index].Val
	}
	return []byte("")
}

func HashSlot(key []byte) int {
	if beg := bytes.IndexByte(key, '{'); beg >= 0 {
		if end := bytes.IndexByte(key[beg+1:], '}'); end >= 0 {
			key = key[beg+1 : beg+1+end]
		}
	}
	return int(crc32.ChecksumIEEE(key) % MaxSlotNum)
}
