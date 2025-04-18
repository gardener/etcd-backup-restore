// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

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
	defaultSnapshotTimeout time.Duration = 30 * time.Minute
	//defaultMetricsScrapeWaitDuration defines default duration to wait for after compaction is completed, to allow Prometheus metrics to be scraped
	defaultMetricsScrapeWaitDuration time.Duration = 0 * time.Second
)

// CompactOptions holds all configurable options of compact.
type CompactOptions struct {
	*RestoreOptions
	*CompactorConfig
	TempDir string
}

// CompactorConfig holds all configuration options related to `compact` subcommand.
type CompactorConfig struct {
	FullSnapshotLeaseName     string            `json:"fullSnapshotLeaseName,omitempty"`
	DeltaSnapshotLeaseName    string            `json:"deltaSnapshotLeaseName,omitempty"`
	SnapshotTimeout           wrappers.Duration `json:"snapshotTimeout,omitempty"`
	DefragTimeout             wrappers.Duration `json:"defragTimeout,omitempty"`
	MetricsScrapeWaitDuration wrappers.Duration `json:"metricsScrapeWaitDuration,omitempty"`
	NeedDefragmentation       bool              `json:"needDefrag,omitempty"`
	EnabledLeaseRenewal       bool              `json:"enabledLeaseRenewal"`
	// see https://github.com/gardener/etcd-druid/issues/648
}

// NewCompactorConfig returns the CompactorConfig.
func NewCompactorConfig() *CompactorConfig {
	return &CompactorConfig{
		NeedDefragmentation:       true,
		SnapshotTimeout:           wrappers.Duration{Duration: defaultSnapshotTimeout},
		DefragTimeout:             wrappers.Duration{Duration: defaultDefragTimeout},
		FullSnapshotLeaseName:     DefaultFullSnapshotLeaseName,
		DeltaSnapshotLeaseName:    DefaultDeltaSnapshotLeaseName,
		EnabledLeaseRenewal:       DefaultSnapshotLeaseRenewalEnabled,
		MetricsScrapeWaitDuration: wrappers.Duration{Duration: defaultMetricsScrapeWaitDuration},
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
	fs.DurationVar(&c.MetricsScrapeWaitDuration.Duration, "metrics-scrape-wait-duration", c.MetricsScrapeWaitDuration.Duration, "The duration to wait for after compaction is completed, to allow Prometheus metrics to be scraped")
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
