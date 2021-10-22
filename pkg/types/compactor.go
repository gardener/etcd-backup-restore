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

package types

import (
	"fmt"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/wrappers"
	flag "github.com/spf13/pflag"
)

const (
	// defaultDefragTimeout defines default timeout duration for ETCD defrag call during compaction of snapshots.
	defaultDefragTimeout time.Duration = 8 * time.Minute
	// defaultSnapshotTimeout defines default timeout duration for taking compacted FullSnapshot.
	defaultSnapshotTimeout time.Duration = 8 * time.Minute
)

// CompactOptions holds all configurable options of compact.
type CompactOptions struct {
	*RestoreOptions
	*CompactorConfig
}

// CompactorConfig holds all configuration options related to `compact` subcommand.
type CompactorConfig struct {
	NeedDefragmentation    bool              `json:"needDefrag,omitempty"`
	SnapshotTimeout        wrappers.Duration `json:"snapshotTimeout,omitempty"`
	DefragTimeout          wrappers.Duration `json:"defragTimeout,omitempty"`
	FullSnapshotLeaseName  string            `json:"fullSnapshotLeaseName,omitempty"`
	DeltaSnapshotLeaseName string            `json:"deltaSnapshotLeaseName,omitempty"`
	EnabledLeaseRenewal    bool              `json:"enabledLeaseRenewal"`
}

// NewCompactorConfig returns the CompactorConfig.
func NewCompactorConfig() *CompactorConfig {
	return &CompactorConfig{
		NeedDefragmentation:    true,
		SnapshotTimeout:        wrappers.Duration{Duration: defaultSnapshotTimeout},
		DefragTimeout:          wrappers.Duration{Duration: defaultDefragTimeout},
		FullSnapshotLeaseName:  DefaultFullSnapshotLeaseName,
		DeltaSnapshotLeaseName: DefaultDeltaSnapshotLeaseName,
		EnabledLeaseRenewal:    DefaultSnapshotLeaseRenewalEnabled,
	}
}

// AddFlags adds the flags to flagset.
func (c *CompactorConfig) AddFlags(fs *flag.FlagSet) {
	fs.BoolVar(&c.NeedDefragmentation, "defragment", c.NeedDefragmentation, "defragment after compaction")
	fs.DurationVar(&c.SnapshotTimeout.Duration, "etcd-snapshot-timeout", c.SnapshotTimeout.Duration, "timeout duration for taking compacted full snapshots")
	fs.DurationVar(&c.DefragTimeout.Duration, "etcd-defrag-timeout", c.DefragTimeout.Duration, "timeout duration for etcd defrag call during compaction.")
	fs.StringVar(&c.FullSnapshotLeaseName, "full-snapshot-lease-name", c.FullSnapshotLeaseName, "full snapshot lease name")
	fs.StringVar(&c.DeltaSnapshotLeaseName, "delta-snapshot-lease-name", c.DeltaSnapshotLeaseName, "delta snapshot lease name")
	fs.BoolVar(&c.EnabledLeaseRenewal, "enable-snapshot-lease-renewal", c.EnabledLeaseRenewal, "Allows compactor to renew the full snapshot lease when successfully compacted snapshot is uploaded")
}

// Validate validates the config.
func (c *CompactorConfig) Validate() error {
	if c.SnapshotTimeout.Duration <= 0 {
		return fmt.Errorf("snapshot timeout should be greater than zero")

	}
	if c.DefragTimeout.Duration <= 0 {
		return fmt.Errorf("etcd defrag timeout should be greater than zero")
	}
	if c.EnabledLeaseRenewal {
		if len(c.FullSnapshotLeaseName) == 0 {
			return fmt.Errorf("FullSnapshotLeaseName can not be an empty string when enable-snapshot-lease-renewal is true")
		}
		if len(c.DeltaSnapshotLeaseName) == 0 {
			return fmt.Errorf("DeltaSnapshotLeaseName can not be an empty string when enable-snapshot-lease-renewal is true")
		}
	}
	return nil
}
