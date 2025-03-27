// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package types

import (
	"fmt"
	"math"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/wrappers"

	flag "github.com/spf13/pflag"
)

const (
	// defaultMultiplier defines default value of multiplier for Exponential-Backoff mechanism.
	defaultMultiplier = 2
	// defaultAttemptLimit defines default no. of attempts to retry before a ThresholdTime is achieved.
	defaultAttemptLimit = 6
	// defaultThresholdTime defines default threshold Backoff time used when threshold is achieved.
	defaultThresholdTime time.Duration = 128 * time.Second
)

// ExponentialBackoffConfig holds the configuration of the Exponential-Backoff mechanism.
type ExponentialBackoffConfig struct {
	// Multiplier defines multiplicative factor for Exponential-Backoff mechanism.
	Multiplier uint `json:"multiplier,omitempty"`
	// AttemptLimit defines the threshold no. of attempts to retry before a ThresholdTime is achieved.
	AttemptLimit uint `json:"attemptLimit,omitempty"`
	// ThresholdTime defines the upper bound time of Exponential-Backoff mechanism for retry operation.
	ThresholdTime wrappers.Duration `json:"thresholdTime,omitempty"`
}

// NewExponentialBackOffConfig returns new ExponentialBackoff.
func NewExponentialBackOffConfig() *ExponentialBackoffConfig {
	return &ExponentialBackoffConfig{
		Multiplier:    defaultMultiplier,
		AttemptLimit:  defaultAttemptLimit,
		ThresholdTime: wrappers.Duration{Duration: defaultThresholdTime},
	}
}

// AddFlags adds the flags to flagset.
func (e *ExponentialBackoffConfig) AddFlags(fs *flag.FlagSet) {
	fs.UintVar(&e.Multiplier, "backoff-multiplier", e.Multiplier, "multiplicative factor for backoff mechanism")
	fs.UintVar(&e.AttemptLimit, "backoff-attempt-limit", e.AttemptLimit, "threshold no. of attempt limit")
	fs.DurationVar(&e.ThresholdTime.Duration, "backoff-threshold-time", e.ThresholdTime.Duration, "upper bound backoff time")
}

// Validate validates the ExponentialBackoffConfig.
func (e *ExponentialBackoffConfig) Validate() error {
	if e.Multiplier <= 0 {
		return fmt.Errorf("multiplicative factor for backoff mechanism can't be less than zero")
	} else if e.Multiplier > math.MaxInt64 {
		return fmt.Errorf("multiplicative factor for backoff mechanism can't be more than %d", math.MaxInt64)
	}
	if e.AttemptLimit <= 0 {
		return fmt.Errorf("backoff attempt-limit can't be less than zero")
	}
	if e.ThresholdTime.Duration <= 0 {
		return fmt.Errorf("backoff threshold time should be than zero")
	}

	return nil
}
