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

package g

import (
	"log"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/open-falcon/falcon-plus/common/sentinel"
)

var RedisConnPool *redis.Pool

func InitRedisConnPool() {
	if !Config().Alarm.Enabled {
		return
	}

	dsn := Config().Alarm.Redis.Dsn
	maxIdle := Config().Alarm.Redis.MaxIdle
	idleTimeout := 240 * time.Second

	connTimeout := time.Duration(Config().Alarm.Redis.ConnTimeout) * time.Millisecond
	readTimeout := time.Duration(Config().Alarm.Redis.ReadTimeout) * time.Millisecond
	writeTimeout := time.Duration(Config().Alarm.Redis.WriteTimeout) * time.Millisecond

	if len(strings.Split(dsn, ",")) > 1 {
		sntnl := &sentinel.Sentinel{
			Addrs:      strings.Split(dsn, ","),
			MasterName: "redismaster",
			Dial: func(addr string) (redis.Conn, error) {
				c, err := redis.DialTimeout("tcp", addr, connTimeout, readTimeout, writeTimeout)
				if err != nil {
					return nil, err
				}
				return c, nil
			},
		}
		RedisConnPool = &redis.Pool{
			MaxIdle:     maxIdle,
			IdleTimeout: idleTimeout,
			Dial: func() (redis.Conn, error) {
				masterAddr, err := sntnl.MasterAddr()
				if err != nil {
					return nil, err
				}
				c, err := redis.Dial("tcp", masterAddr)
				if err != nil {
					return nil, err
				}
				return c, err
			},
			TestOnBorrow: PingRedis,
		}
	}else{
		RedisConnPool = &redis.Pool{
			MaxIdle:     maxIdle,
			IdleTimeout: idleTimeout,
			Dial: func() (redis.Conn, error) {
				masterAddr, err := sntnl.MasterAddr()
				if err != nil {
					return nil, err
				}
				c, err := redis.DialTimeout("tcp", dsn, connTimeout, readTimeout, writeTimeout)
				if err != nil {
					return nil, err
				}
				return c, err
			},
			TestOnBorrow: PingRedis,
	}
}

func PingRedis(c redis.Conn, t time.Time) error {
	_, err := c.Do("ping")
	if err != nil {
		log.Println("[ERROR] ping redis fail", err)
	}
	if !sentinel.TestRole(c, "master") {
		return errors.New("Role check failed")
	} else {
		return nil
	}
	return err
}
