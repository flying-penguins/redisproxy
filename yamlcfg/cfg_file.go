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

package yamlcfg

import (
	"fmt"
	"io/ioutil"

	"github.com/go-yaml/yaml"
)

type Config struct {
	LogLevel int    `yaml:"log_level"`
	LogFile  string `yaml:"log_file"`

	Cluster ClusterCfg `yaml:"cluster"`
}

type ClusterCfg struct {
	Addr          string   `yaml:"addr"`
	CfgServers    []string `yaml:"cfg_servers,flow"`
	KeepaliveTime int      `yaml:"keepalive_time"`
	MaxClients    int      `yaml:"max_clients"`
	RedisDownTime int64    `yaml:"redis_down_time"`
	GroupsVer     string   `yaml:"groups_ver"`

	Groups []GroupCfg `yaml:"groups"`
}

//GroupCfg.Slaves is not used for proxy
type GroupCfg struct {
	Slots   []int  `yaml:"slots,flow"`
	Master  string `yaml:"master"`
	Migrate string `yaml:"migrate"`
}

func ParseConfigFile(fileName string) (*Config, error) {
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, err
	}

	conf, err := ParseConfigData(data)
	if err != nil {
		return nil, err
	}

	if len(conf.Cluster.CfgServers) <= 0 {
		conf.Cluster.CfgServers = append(conf.Cluster.CfgServers, "")
	}

	for _, g := range conf.Cluster.Groups {
		if len(g.Slots) != 2 {
			return nil, fmt.Errorf("slots array size is not 2: %v", g.Slots)
		}
	}

	return conf, nil
}

func SaveConfigToFile(cfg *Config, filename string) error {
	data, err := yaml.Marshal(&cfg)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(filename, data, 0755)
}

func ParseConfigData(data []byte) (*Config, error) {
	var cfg Config
	if err := yaml.Unmarshal([]byte(data), &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func MarshalGroups(groups []GroupCfg) ([]byte, error) {
	return yaml.Marshal(&groups)
}

func UnmarshalGroups(data []byte) ([]GroupCfg, error) {
	var groups []GroupCfg
	if err := yaml.Unmarshal([]byte(data), &groups); err != nil {
		return nil, err
	}
	return groups, nil
}

func CopyGroupCfg(g GroupCfg) GroupCfg {
	group := GroupCfg{
		Slots:   make([]int, len(g.Slots)),
		Master:  g.Master,
		Migrate: g.Migrate,
	}

	copy(group.Slots, g.Slots)

	return group
}

func CopyGroupCfgSlice(gs []GroupCfg) []GroupCfg {
	groups := make([]GroupCfg, 0)
	for _, g := range gs {
		groups = append(groups, CopyGroupCfg(g))
	}

	return groups
}
