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
	"sort"
	"testing"
)

func TestGetReplicationInfo(t *testing.T) {
	m, _ := getReplicationInfo("192.168.200.129:9001")
	fmt.Printf("%+v", m)
}

func TestSort(t *testing.T) {
	slaves := []SlaveInfo{{"a123", 123}, {"a96", 96}, {"a456", 456}, {"a96", 96}, {"a0", 0}}
	sort.Sort(SlaveInfoSlice(slaves))

	fmt.Printf("%+v", slaves)
}
