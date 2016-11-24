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

package logger

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"
)

const (
	PanicLevel int = iota
	ErrorLevel
	WarnLevel
	NoticeLevel
	InfoLevel
	DebugLevel
)

type LogFile struct {
	level    int
	logTime  int64
	fileName string
	fileFd   *os.File
}

var logFile LogFile

func Config(logFolder string, level int) {
	logFile.fileName = logFolder
	logFile.level = level

	log.SetOutput(logFile)
	log.SetFlags(log.Lmicroseconds | log.Lshortfile)
}

func SetLevel(level int) {
	logFile.level = level
}

func Debugf(format string, args ...interface{}) {
	if logFile.level >= DebugLevel {
		log.SetPrefix("debug ")
		log.Output(2, fmt.Sprintf(format, args...))
	}
}

func Infof(format string, args ...interface{}) {
	if logFile.level >= InfoLevel {
		log.SetPrefix("info ")
		log.Output(2, fmt.Sprintf(format, args...))
	}
}

func Noticef(format string, args ...interface{}) {
	if logFile.level >= NoticeLevel {
		log.SetPrefix("notice ")
		log.Output(2, fmt.Sprintf(format, args...))
	}
}

func Warnf(format string, args ...interface{}) {
	if logFile.level >= WarnLevel {
		log.SetPrefix("warn ")
		log.Output(2, fmt.Sprintf(format, args...))
	}
}

func Errorf(format string, args ...interface{}) {
	if logFile.level >= ErrorLevel {
		log.SetPrefix("error ")
		log.Output(2, fmt.Sprintf(format, args...))
	}
}

func Panicf(format string, args ...interface{}) {
	if logFile.level >= PanicLevel {
		log.SetPrefix("panic ")
		log.Output(2, fmt.Sprintf(format, args...))
		log.Panicf(format, args...)
	}
}

func (me LogFile) Write(buf []byte) (n int, err error) {
	if me.fileName == "" {
		fmt.Printf("consol: %s", buf)
		return len(buf), nil
	}

	if logFile.logTime+3600 < time.Now().Unix() {
		logFile.createLogFile()
		logFile.logTime = time.Now().Unix()
	}

	if logFile.fileFd == nil {
		return len(buf), nil
	}

	return logFile.fileFd.Write(buf)
}

func (me *LogFile) createLogFile() {
	logdir := "./"
	if index := strings.LastIndex(me.fileName, "/"); index != -1 {
		logdir = me.fileName[0:index] + "/"
		os.MkdirAll(me.fileName[0:index], os.ModePerm)
	}

	now := time.Now()
	filename := fmt.Sprintf("%s_%04d%02d%02d_%02d%02d", me.fileName, now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute())
	if err := os.Rename(me.fileName, filename); err == nil {
		go func() {
			tarCmd := exec.Command("tar", "-zcf", filename+".tar.gz", filename, "--remove-files")
			tarCmd.Run()

			rmCmd := exec.Command("/bin/sh", "-c", "find "+logdir+` -type f -mtime +2 -exec rm {} \;`)
			rmCmd.Run()
		}()
	}

	for index := 0; index < 10; index++ {
		if fd, err := os.OpenFile(me.fileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModeExclusive); nil == err {
			me.fileFd.Sync()
			me.fileFd.Close()
			me.fileFd = fd
			break
		}

		me.fileFd = nil
	}
}
