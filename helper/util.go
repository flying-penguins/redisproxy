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

package helper

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"time"
)

var DevNullFile *os.File = nil
var FuncArgc0RetErrNil func() error = nil

func init() {
	DevNullFile, _ = os.OpenFile("/dev/null", os.O_RDWR, 0666)

	FuncArgc0RetErrNil = func() error {
		return nil
	}
}

func MicroSecond() int64 {
	return time.Now().UnixNano() / int64(time.Microsecond)
}

func MD5(data []byte) string {
	h := md5.New()
	h.Write(data)
	cipherStr := h.Sum(nil)

	return hex.EncodeToString(cipherStr)
}

func ProgressBarStr(count, total uint64) string {
	arr := make([]byte, 0)
	for i := 0; i < 100; i++ {
		if i < int(100*(count+1)/(total+1)) {
			arr = append(arr, '#')
		} else {
			arr = append(arr, ' ')
		}
	}

	return fmt.Sprintf("[%s] %d/%d = %2.2f", string(arr), count, total,
		100.0*(float64(count)+1.0)/(float64(total)+1.0))
}

func SegmentToMap(str, itemSep, kvSep string) map[string]string {
	m := make(map[string]string)
	items := strings.Split(str, itemSep)
	for i := 0; i < len(items); i++ {
		kv := strings.Split(items[i], kvSep)
		if len(kv) != 2 {
			continue
		}

		m[kv[0]] = kv[1]
	}

	return m
}

func Hash31(b []byte) uint64 {
	hash := uint64(0)
	for i := 0; i < len(b); i++ {
		//hash = ((hash << 5) + hash) + uint64(b[i])
		hash += uint64(b[i]) * 31
	}

	return hash
}
