// Copyright (c) 2018 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
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

package types

import (
	"fmt"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/wrappers"
	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
	flag "github.com/spf13/pflag"
)

const (
	// GarbageCollectionPolicyExponential defines the exponential policy for garbage collecting old backups
	GarbageCollectionPolicyExponential = "Exponential"
	// GarbageCollectionPolicyLimitBased defines the limit based policy for garbage collecting old backups
	GarbageCollectionPolicyLimitBased = "LimitBased"
	// DefaultMaxBackups is default number of maximum backups for limit based garbage collection policy.
	DefaultMaxBackups = 7

	// SnapshotterInactive is set when the snapshotter has not started taking snapshots.
	SnapshotterInactive SnapshotterState = 0
	// SnapshotterActive is set when the snapshotter has started taking snapshots.
	SnapshotterActive SnapshotterState = 1

	// DefaultDeltaSnapMemoryLimit is default memory limit for delta snapshots.
	DefaultDeltaSnapMemoryLimit = 10 * 1024 * 1024 //10Mib
	// DefaultDeltaSnapshotInterval is the default interval for delta snapshots.
	DefaultDeltaSnapshotInterval = 20 * time.Second

	// DefaultFullSnapshotSchedule is the default schedule
	DefaultFullSnapshotSchedule = "0 */1 * * *"
	// DefaultGarbageCollectionPeriod is the default interval for garbage collection
	DefaultGarbageCollectionPeriod = time.Minute

	// DeltaSnapshotIntervalThreshold is interval between delta snapshot
	DeltaSnapshotIntervalThreshold = time.Second
)

// SnapshotterState denotes the state the snapshotter would be in.
type SnapshotterState int

// SnapshotterConfig holds the snapshotter config.
type SnapshotterConfig struct {
	FullSnapshotSchedule     string            `json:"schedule,omitempty"`
	DeltaSnapshotPeriod      wrappers.Duration `json:"deltaSnapshotPeriod,omitempty"`
	DeltaSnapshotMemoryLimit uint              `json:"deltaSnapshotMemoryLimit,omitempty"`
	GarbageCollectionPeriod  wrappers.Duration `json:"garbageCollectionPeriod,omitempty"`
	GarbageCollectionPolicy  string            `json:"garbageCollectionPolicy,omitempty"`
	MaxBackups               uint              `json:"maxBackups,omitempty"`
}

// AddFlags adds the flags to flagset.
func (c *SnapshotterConfig) AddFlags(fs *flag.FlagSet) {
	fs.StringVarP(&c.FullSnapshotSchedule, "schedule", "s", c.FullSnapshotSchedule, "schedule for snapshots")
	fs.DurationVar(&c.DeltaSnapshotPeriod.Duration, "delta-snapshot-period", c.DeltaSnapshotPeriod.Duration, "Period after which delta snapshot will be persisted. If this value is set to be lesser than 1, delta snapshotting will be disabled.")
	fs.UintVar(&c.DeltaSnapshotMemoryLimit, "delta-snapshot-memory-limit", c.DeltaSnapshotMemoryLimit, "memory limit after which delta snapshots will be taken")
	fs.DurationVar(&c.GarbageCollectionPeriod.Duration, "garbage-collection-period", c.GarbageCollectionPeriod.Duration, "Period for garbage collecting old backups")
	fs.StringVar(&c.GarbageCollectionPolicy, "garbage-collection-policy", c.GarbageCollectionPolicy, "Policy for garbage collecting old backups")
	fs.UintVarP(&c.MaxBackups, "max-backups", "m", c.MaxBackups, "maximum number of previous backups to keep")
}

// Validate validates the config.
func (c *SnapshotterConfig) Validate() error {
	if _, err := cron.ParseStandard(c.FullSnapshotSchedule); err != nil {
		return err
	}
	if c.GarbageCollectionPolicy != GarbageCollectionPolicyLimitBased && c.GarbageCollectionPolicy != GarbageCollectionPolicyExponential {
		return fmt.Errorf("invalid garbage collection policy: %s", c.GarbageCollectionPolicy)
	}
	if c.GarbageCollectionPolicy == GarbageCollectionPolicyLimitBased && c.MaxBackups <= 0 {
		return fmt.Errorf("max backups should be greather than zero for garbage collection policy set to limit based")
	}

	if c.DeltaSnapshotPeriod.Duration < DeltaSnapshotIntervalThreshold {
		logrus.Infof("Found delta snapshot interval %s less than 1 second. Disabling delta snapshotting. ", c.DeltaSnapshotPeriod)
	}

	if c.DeltaSnapshotMemoryLimit < 1 {
		logrus.Infof("Found delta snapshot memory limit %d bytes less than 1 byte. Setting it to default: %d ", c.DeltaSnapshotMemoryLimit, DefaultDeltaSnapMemoryLimit)
		c.DeltaSnapshotMemoryLimit = DefaultDeltaSnapMemoryLimit
	}
	return nil
}
