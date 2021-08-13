// Copyright (c) 2021 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
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
	"fmt"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/wrappers"
	flag "github.com/spf13/pflag"
)

// NewLeaderElectionConfig returns the Config.
func NewLeaderElectionConfig() *Config {
	return &Config{
		ReelectionPeriod:      wrappers.Duration{Duration: DefaultReelectionPeriod},
		EtcdConnectionTimeout: wrappers.Duration{Duration: DefaultEtcdConnectionTimeout},
	}
}

// AddFlags adds the flags to flagset.
func (c *Config) AddFlags(fs *flag.FlagSet) {
	fs.DurationVar(&c.EtcdConnectionTimeout.Duration, "etcd-connection-timeout-leader-election", c.EtcdConnectionTimeout.Duration, "timeout duration of etcd client connection during leader election.")
	fs.DurationVar(&c.ReelectionPeriod.Duration, "reelection-period", c.ReelectionPeriod.Duration, "Period after which election will be re-triggered to check the leadership status.")
}

// Validate validates the Config.
func (c *Config) Validate() error {
	if c.ReelectionPeriod.Duration <= time.Duration(1*time.Second) {
		return fmt.Errorf("ReelectionPeriod should be greater than 1 second")
	}

	if c.EtcdConnectionTimeout.Duration <= time.Duration(1*time.Second) {
		return fmt.Errorf("etcd connection timeout in leaderElection should be greater than 1 second")
	}

	return nil
}
