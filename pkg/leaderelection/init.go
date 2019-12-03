// Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
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

package leaderelection

import (
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/wrappers"
	flag "github.com/spf13/pflag"
)

// NewLeaderEletionConfig returns the leader election config.
func NewLeaderEletionConfig() *Config {
	return &Config{
		RetryPeriod: wrappers.Duration{Duration: 5 * time.Second},
	}
}

// AddFlags adds the flags to flagset.
func (c *Config) AddFlags(fs *flag.FlagSet) {
	fs.DurationVar(&c.RetryPeriod.Duration, "leader-election-retry-period", c.RetryPeriod.Duration, "the duration the LeaderElector clients should waitbetween tries of actions.")
}

// Validate validates the config.
func (c *Config) Validate() error {
	return nil
}
