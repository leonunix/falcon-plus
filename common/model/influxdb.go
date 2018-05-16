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

package model

import (
	"fmt"

	"github.com/open-falcon/falcon-plus/common/utils"
)

type InfluxdbItem struct {
	Endpoint  string            `json:"endpoint"`
	Metric    string            `json:"metric"`
	Value     float64           `json:"value"`
	Timestamp int64             `json:"timestamp"`
	Tags      map[string]string `json:"tags"`
}

func (this *InfluxdbItem) String() string {
	return fmt.Sprintf("<Endpoint:%s, Metric:%s, Value:%f, Timestamp:%d, Tags:%v>",
		this.Endpoint,
		this.Metric,
		this.Value,
		this.Timestamp,
		this.Tags)
}

func (this *InfluxdbItem) PrimaryKey() string {
	return utils.Md5(utils.PK(this.Endpoint, this.Metric, this.Tags))
}

type HistoryData struct {
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
}
