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
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"redisproxy/helper"
)

type Conn struct {
	sock   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer

	ReaderTimeout time.Duration
	WriterTimeout time.Duration

	readerDeadlineTime int64
	writerDeadlineTime int64
}

func Connect(addr string, timeout time.Duration) (*Conn, error) {
	client, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}

	if tc, ok := client.(*net.TCPConn); ok {
		tc.SetNoDelay(false)
	}

	return &Conn{
		sock:   client,
		reader: bufio.NewReaderSize(client, 1024*64),
		writer: bufio.NewWriterSize(client, 1024*64),
	}, nil
}

func Accept(client net.Conn) *Conn {
	if tc, ok := client.(*net.TCPConn); ok {
		tc.SetNoDelay(false)
	}

	return &Conn{
		sock:   client,
		reader: bufio.NewReaderSize(client, 1024*64),
		writer: bufio.NewWriterSize(client, 1024*64),
	}
}

func (c *Conn) Close() {
	c.sock.Close()
}

func (c *Conn) Read() (*Proto, error) {
	if err := c.setReadTimeout(); err != nil {
		return nil, err
	}

	protoType, err := c.reader.ReadByte()
	if err != nil {
		return nil, err
	}

	switch protoType {
	case TypeInt, TypeStatus, TypeError:
		b, err := c.readLine()
		if err != nil {
			return nil, fmt.Errorf("readLine err: %c, %s", protoType, err)
		}

		return &Proto{Type: protoType, Val: b}, nil

	case TypeBulk:
		n, err := c.readInt64()
		if err != nil || n < -1 {
			return nil, fmt.Errorf("ReadReplyInt64 err: %s", err)
		}

		if n == -1 {
			return &Proto{Type: TypeBulk, Val: nil}, nil
		}

		b, err := c.readBytes(n)
		if err != nil {
			return nil, err
		}

		return &Proto{Type: TypeBulk, Val: b}, nil

	case TypeArray:
		n, err := c.readInt64()
		if err != nil || n < 0 {
			return &Proto{Type: TypeArray, Array: nil}, nil
		}

		reply := &Proto{Type: TypeArray, Array: make([]*Proto, n)}
		for i := 0; i < len(reply.Array); i++ {
			reply.Array[i], err = c.Read()
			if err != nil {
				return nil, fmt.Errorf("Parse Array err: %s, %d %d", err, n, i)
			}
		}

		return reply, nil

	default:
		return nil, fmt.Errorf("Data Type err: %d", protoType)
	}

	return nil, nil
}

func (c *Conn) Write(p *Proto, flush bool) error {
	if err := c.setWriteTimeout(); err != nil {
		return err
	}
	if err := c.bufWrite(p); err != nil {
		return err
	}

	if !flush {
		return nil
	}

	if err := c.writer.Flush(); err != nil {
		return err
	}

	return nil
}

func (c *Conn) bufWrite(p *Proto) error {
	if p == nil {
		return errors.New("buf Write pointer is nil")
	}

	if err := c.writer.WriteByte(byte(p.Type)); err != nil {
		return errors.New("WriteByte Type Failed")
	}

	switch p.Type {
	case TypeInt, TypeStatus, TypeError:
		return c.writeBytesLine(p.Val)
	case TypeBulk:
		return c.writeBulkLine(p.Val)
	case TypeArray:
		length := -1
		if p.Array != nil {
			length = len(p.Array)
		}
		if err := c.writeIntLine(int64(length)); err != nil {
			return err
		}

		for i := 0; i < length; i++ {
			if err := c.bufWrite(p.Array[i]); err != nil {
				return err
			}
		}
		return nil

	default:
		return fmt.Errorf("bad resp type %s", p.Type)
	}
}

func (c *Conn) Cmd(p *Proto) (*Proto, error) {
	if err := c.Write(p, true); err != nil {
		return nil, err
	}

	reply, err := c.Read()
	if err != nil {
		return nil, err
	}

	return reply, err
}

func (c *Conn) writeBulkLine(b []byte) error {
	length := int64(-1)
	if b != nil {
		length = int64(len(b))
	}

	if err := c.writeIntLine(length); err != nil {
		return err
	}

	return c.writeBytesLine(b)
}

func (c *Conn) writeBytesLine(b []byte) error {
	if b == nil {
		return nil
	}

	if _, err := c.writer.Write(b); err != nil {
		return err
	}

	if _, err := c.writer.WriteString("\r\n"); err != nil {
		return err
	}

	return nil
}
func (c *Conn) writeIntLine(i int64) error {
	str := strconv.FormatInt(i, 10) + "\r\n"
	if _, err := c.writer.WriteString(str); err != nil {
		return err
	}

	return nil
}

func (c *Conn) readLine() ([]byte, error) {
	b, err := c.reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}

	n := len(b) - 2
	if n < 0 || b[n] != '\r' {
		return nil, fmt.Errorf("readLine proto format err %d %d", n)
	}

	return b[:n], nil
}

func (c *Conn) readBytes(n int64) ([]byte, error) {
	b := make([]byte, n+2)
	if _, err := io.ReadFull(c.reader, b); err != nil {
		return nil, fmt.Errorf("readBytes ReadFull err %s", err)
	}

	if b[n] != '\r' || b[n+1] != '\n' {
		return nil, fmt.Errorf("readBytes bad CRLF end")
	}

	return b[:n], nil
}

func (c *Conn) readInt64() (int64, error) {
	b, err := c.readLine()
	if err != nil {
		return 0, err
	}

	n, err := strconv.ParseInt(helper.String(b), 10, 64)
	if err != nil {
		return 0, errors.New("ParseInt err: " + string(b))
	}

	return n, nil
}

func (c *Conn) setReadTimeout() error {
	timeout := c.ReaderTimeout
	nowTime := helper.MicroSecond() * 1000
	if timeout == 0 || c.readerDeadlineTime-nowTime > int64(timeout)*9/10 {
		return nil
	}

	c.readerDeadlineTime = nowTime + int64(timeout)
	return c.sock.SetReadDeadline(time.Now().Add(timeout))

}

func (c *Conn) setWriteTimeout() error {
	timeout := c.WriterTimeout
	nowTime := helper.MicroSecond() * 1000
	if timeout == 0 || c.writerDeadlineTime-nowTime > int64(timeout)*9/10 {
		return nil
	}

	c.writerDeadlineTime = nowTime + int64(timeout)
	return c.sock.SetWriteDeadline(time.Now().Add(timeout))
}
