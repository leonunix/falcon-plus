// Copyright 2017 Xiaomi, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sender

import (
	"errors"
	"log"
	"strings"

	backend "github.com/open-falcon/falcon-plus/common/backend_pool"
	"github.com/open-falcon/falcon-plus/modules/transfer/g"
	nset "github.com/toolkits/container/set"
)

var (
	errInvalidDSNUnescaped = errors.New("Invalid DSN: Did you forget to escape a param value?")
	errInvalidDSNAddr      = errors.New("Invalid DSN: Network Address not terminated (missing closing brace)")
	errInvalidDSNNoSlash   = errors.New("Invalid DSN: Missing the slash separating the database name")
)

func parseDSN(dsn string) (cfg *backend.InfluxdbConnection, err error) {
	// New config
	cfg = &backend.InfluxdbConnection{}

	// [username[:password]@][protocol[(address)]]/dbname
	// Find the last '/' (since the password or the net addr might contain a '/')
	foundSlash := false
	for i := len(dsn) - 1; i >= 0; i-- {
		if dsn[i] == '/' {
			foundSlash = true
			var j, k int

			// left part is empty if i <= 0
			if i > 0 {
				// [username[:password]@][protocol[(address)]]
				// Find the last '@' in dsn[:i]
				for j = i; j >= 0; j-- {
					if dsn[j] == '@' {
						// username[:password]
						// Find the first ':' in dsn[:j]
						for k = 0; k < j; k++ {
							if dsn[k] == ':' {
								cfg.Password = dsn[k+1 : j]
								break
							}
						}
						cfg.Username = dsn[:k]

						break
					}
				}

				// [protocol[(address)]]
				// Find the first '(' in dsn[j+1:i]
				for k = j + 1; k < i; k++ {
					if dsn[k] == '(' {
						// dsn[i-1] must be == ')' if an address is specified
						if dsn[i-1] != ')' {
							if strings.ContainsRune(dsn[k+1:i], ')') {
								return nil, errInvalidDSNUnescaped
							}
							return nil, errInvalidDSNAddr
						}
						cfg.Address = dsn[k+1 : i-1]
						break
					}
				}
				cfg.Protocol = dsn[j+1 : k]
			}

			// /dbname
			cfg.DBName = dsn[i+1 : len(dsn)]

			break
		}
	}

	if !foundSlash && len(dsn) > 0 {
		return nil, errInvalidDSNNoSlash
	}

	// Set default network if empty
	if cfg.Protocol == "" {
		cfg.Protocol = "tcp"
	}

	// Set default address if empty
	if cfg.Address == "" {
		return nil, errors.New("Empty DSN address")
	}

	return
}

func initConnPools() {
	cfg := g.Config()

	// judge
	judgeInstances := nset.NewStringSet()
	for _, instance := range cfg.Judge.Cluster {
		judgeInstances.Add(instance)
	}
	JudgeConnPools = backend.CreateSafeRpcConnPools(cfg.Judge.MaxConns, cfg.Judge.MaxIdle,
		cfg.Judge.ConnTimeout, cfg.Judge.CallTimeout, judgeInstances.ToSlice())

	// tsdb
	if cfg.Tsdb.Enabled {
		TsdbConnPoolHelper = backend.NewTsdbConnPoolHelper(cfg.Tsdb.Address, cfg.Tsdb.MaxConns, cfg.Tsdb.MaxIdle, cfg.Tsdb.ConnTimeout, cfg.Tsdb.CallTimeout)
	}

	// influxdb
	if cfg.Influxdb.Enabled {
		influxdbInstances := make([]backend.InfluxdbConnection, 1)
		dsn, err := parseDSN(cfg.Influxdb.Address)
		if err != nil {
			log.Print("syntax of influxdb address is wrong")
		} else {
			influxdbInstances[0] = *dsn
			InfluxdbConnPools = backend.CreateInfluxdbConnPools(cfg.Influxdb.MaxConns, cfg.Influxdb.MaxIdle,
				cfg.Influxdb.ConnTimeout, cfg.Influxdb.CallTimeout, influxdbInstances)
		}
	}

	// graph
	graphInstances := nset.NewSafeSet()
	for _, nitem := range cfg.Graph.ClusterList {
		for _, addr := range nitem.Addrs {
			graphInstances.Add(addr)
		}
	}
	GraphConnPools = backend.CreateSafeRpcConnPools(cfg.Graph.MaxConns, cfg.Graph.MaxIdle,
		cfg.Graph.ConnTimeout, cfg.Graph.CallTimeout, graphInstances.ToSlice())

}

func DestroyConnPools() {
	JudgeConnPools.Destroy()
	GraphConnPools.Destroy()
	TsdbConnPoolHelper.Destroy()
	InfluxdbConnPools.Destroy()
}
