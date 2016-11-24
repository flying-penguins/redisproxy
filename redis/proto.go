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
	"fmt"
	"strconv"
	"strings"
)

const (
	TypeInt    byte = ':'
	TypeBulk   byte = '$'
	TypeError  byte = '-'
	TypeStatus byte = '+'
	TypeArray  byte = '*'
)

type Proto struct {
	Type  byte
	Val   []byte
	Array []*Proto
}

func NewInt(value []byte) *Proto {
	return &Proto{
		Type: TypeInt,
		Val:  value,
	}
}

func NewBulk(value []byte) *Proto {
	return &Proto{
		Type: TypeBulk,
		Val:  value,
	}
}

func NewError(value []byte) *Proto {
	return &Proto{
		Type: TypeError,
		Val:  value,
	}
}

func NewStatus(value []byte) *Proto {
	return &Proto{
		Type: TypeStatus,
		Val:  value,
	}
}

func NewArray(array []*Proto) *Proto {
	return &Proto{
		Type:  TypeArray,
		Array: array,
	}
}

func (p *Proto) Dump() {
	if p == nil {
		fmt.Printf("reply nil\n")
		return
	}

	if p.Type == TypeBulk || p.Type == TypeError || p.Type == TypeStatus || p.Type == TypeInt {
		fmt.Printf("value str: %c%s\n", p.Type, string(p.Val))
	} else if p.Type == TypeArray {
		fmt.Printf("value array=====\n")
		for i := 0; i < len(p.Array); i++ {
			p.Array[i].Dump()
		}
	}
}

func (p *Proto) Overview() string {
	if p == nil {
		return "nil"
	}

	if p.Type == TypeBulk || p.Type == TypeError || p.Type == TypeStatus || p.Type == TypeInt {
		valLen := len(p.Val)
		if valLen > 48 {
			valLen = 48
		}
		return fmt.Sprintf("%c%d:%s", p.Type, len(p.Val), string(p.Val[0:valLen]))
	}

	if p.Type == TypeArray {
		arrLen := len(p.Array)
		strs := "*" + Itoa(int64(arrLen))
		for i := 0; i < arrLen && i < 4; i++ {
			strs += " " + p.Array[i].Overview()
		}
		return strs
	}

	return ""
}

func MigrateMsg(dstAddr string, key []byte, db int) *Proto {
	strs := strings.Split(dstAddr, ":")
	if len(strs) < 2 {
		strs = append(strs, "", "")
	}

	return NewArray([]*Proto{
		NewBulk([]byte("MIGRATE")),
		NewBulk([]byte(strs[0])),
		NewBulk([]byte(strs[1])),
		NewBulk(key),
		NewBulk(Itob(int64(db))),
		NewBulk(Itob(int64(2000))),
		NewBulk([]byte("REPLACE"))})
}

const IntStringMapSize = 1024 * 128

var (
	itoamap []string
	itobmap [][]byte
)

func init() {
	itoamap = make([]string, IntStringMapSize)
	itobmap = make([][]byte, IntStringMapSize)
	for i := 0; i < len(itoamap); i++ {
		itoamap[i] = strconv.Itoa(i)
		itobmap[i] = []byte(strconv.Itoa(i))
	}
}

func Itoa(i int64) string {
	if i >= 0 && i < IntStringMapSize {
		return itoamap[i]
	}
	return strconv.FormatInt(i, 10)
}

func Itob(i int64) []byte {
	if i >= 0 && i < IntStringMapSize {
		return itobmap[i]
	}
	return []byte(strconv.FormatInt(i, 10))
}
