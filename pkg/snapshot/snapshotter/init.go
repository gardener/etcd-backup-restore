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

package snapshotter

import (
	"fmt"

	"github.com/gardener/etcd-backup-restore/pkg/wrappers"
	cron "github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
	flag "github.com/spf13/pflag"
)

// NewSnapshotterConfig returns the snapshotter config.
func NewSnapshotterConfig() *Config {
	return &Config{
		FullSnapshotSchedule:     defaultFullSnapshotSchedule,
		DeltaSnapshotPeriod:      wrappers.Duration{Duration: DefaultDeltaSnapshotInterval},
		DeltaSnapshotMemoryLimit: DefaultDeltaSnapMemoryLimit,
		GarbageCollectionPeriod:  wrappers.Duration{Duration: defaultGarbageCollectionPeriod},
		GarbageCollectionPolicy:  GarbageCollectionPolicyExponential,
		MaxBackups:               DefaultMaxBackups,
	}
}

// AddFlags adds the flags to flagset.
func (c *Config) AddFlags(fs *flag.FlagSet) {
	fs.StringVarP(&c.FullSnapshotSchedule, "schedule", "s", c.FullSnapshotSchedule, "schedule for snapshots")
	fs.DurationVar(&c.DeltaSnapshotPeriod.Duration, "delta-snapshot-period", c.DeltaSnapshotPeriod.Duration, "Period after which delta snapshot will be persisted. If this value is set to be lesser than 1, delta snapshotting will be disabled.")
	fs.UintVar(&c.DeltaSnapshotMemoryLimit, "delta-snapshot-memory-limit", c.DeltaSnapshotMemoryLimit, "memory limit after which delta snapshots will be taken")
	fs.DurationVar(&c.GarbageCollectionPeriod.Duration, "garbage-collection-period", c.GarbageCollectionPeriod.Duration, "Period for garbage collecting old backups")
	fs.StringVar(&c.GarbageCollectionPolicy, "garbage-collection-policy", c.GarbageCollectionPolicy, "Policy for garbage collecting old backups")
	fs.UintVarP(&c.MaxBackups, "max-backups", "m", c.MaxBackups, "maximum number of previous backups to keep")
}

// Validate validates the config.
func (c *Config) Validate() error {
	if _, err := cron.ParseStandard(c.FullSnapshotSchedule); err != nil {
		return err
	}
	if c.GarbageCollectionPolicy != GarbageCollectionPolicyLimitBased && c.GarbageCollectionPolicy != GarbageCollectionPolicyExponential {
		return fmt.Errorf("invalid garbage collection policy: %s", c.GarbageCollectionPolicy)
	}
	if c.GarbageCollectionPolicy == GarbageCollectionPolicyLimitBased && c.MaxBackups <= 0 {
		return fmt.Errorf("max backups should be greather than zero for garbage collection policy set to limit based")
	}

	if c.DeltaSnapshotPeriod.Duration < deltaSnapshotIntervalThreshold {
		logrus.Infof("Found delta snapshot interval %s less than 1 second. Disabling delta snapshotting. ", c.DeltaSnapshotPeriod)
	}

	if c.DeltaSnapshotMemoryLimit < 1 {
		logrus.Infof("Found delta snapshot memory limit %d bytes less than 1 byte. Setting it to default: %d ", c.DeltaSnapshotMemoryLimit, DefaultDeltaSnapMemoryLimit)
		c.DeltaSnapshotMemoryLimit = DefaultDeltaSnapMemoryLimit
	}
	return nil
}
