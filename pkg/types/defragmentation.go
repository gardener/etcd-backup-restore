// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package types

import (
	"fmt"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/wrappers"
	"github.com/robfig/cron/v3"

	flag "github.com/spf13/pflag"
)

const (
	// DefaultDefragSchedule is the default cron schedule for defragmentation (midnight every 3 days).
	DefaultDefragSchedule = "0 0 */3 * *"
	// DefaultDefragConnectionTimeout defines default timeout duration for ETCD defrag call.
	DefaultDefragConnectionTimeout time.Duration = 8 * time.Minute
	// DefragRetryPeriod is used as the duration after which a defragmentation is retried.
	DefragRetryPeriod time.Duration = 1 * time.Minute
	// DefaultFreeSpaceThreshold is the minimum default free space above which schedule defragmentation is triggered.
	DefaultFreeSpaceThreshold = 1 * 1024 * 1024 * 1024 // 1GB
)

// DefragConfig holds the configuration for etcd defragmentation.
type DefragConfig struct {
	// DefragmentationSchedule is the cron schedule on which defragmentation of etcd data directory is triggered.
	DefragmentationSchedule string `json:"defragmentationSchedule"`
	// DefragTimeout is the timeout for a single etcd's defragmentation API call.
	DefragTimeout wrappers.Duration `json:"defragTimeout,omitempty"`
	// FreespaceThreshold is the minimum free space above which schedule defragmentation is triggered.
	FreespaceThreshold int64 `json:"freespaceThreshold,omitempty"`
}

// NewDefragConfig returns a DefragConfig with default values.
func NewDefragConfig() *DefragConfig {
	return &DefragConfig{
		DefragmentationSchedule: DefaultDefragSchedule,
		FreespaceThreshold:      DefaultFreeSpaceThreshold,
		DefragTimeout:           wrappers.Duration{Duration: DefaultDefragConnectionTimeout},
	}
}

// AddFlags adds defragmentation-related flags to the given flag set.
func (c *DefragConfig) AddFlags(fs *flag.FlagSet) {
	fs.Int64Var(&c.FreespaceThreshold, "freespace-threshold", c.FreespaceThreshold, "minimum free-space in database only above which schedule defragmentation is triggered.")
	fs.StringVar(&c.DefragmentationSchedule, "defragmentation-schedule", c.DefragmentationSchedule, "cron schedule to defragment etcd data directory")
	fs.DurationVar(&c.DefragTimeout.Duration, "etcd-defrag-timeout", c.DefragTimeout.Duration, "timeout duration for etcd defrag call")
}

// Validate validates the DefragConfig fields.
func (c *DefragConfig) Validate() error {
	if c.DefragTimeout.Duration <= 0 {
		return fmt.Errorf("etcd defrag timeout should be greater than zero")
	}
	if _, err := cron.ParseStandard(c.DefragmentationSchedule); err != nil {
		return err
	}
	if c.FreespaceThreshold <= 0 {
		return fmt.Errorf("freespace-threshold for schedule defrag must be greater than 0")
	}
	return nil
}
