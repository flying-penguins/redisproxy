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
	"fmt"
	"net/http"
	"time"

	_ "net/http/pprof"
)

func StartHttpPProfRoutine() {
	go func() {
		for i := 5000; i < 6000; i++ {
			debugaddr := fmt.Sprintf("0.0.0.0:%d", i)
			fmt.Printf("http pprof debug server: %s\n", http.ListenAndServe(debugaddr, nil))
			time.Sleep(time.Second)
		}
	}()
}
