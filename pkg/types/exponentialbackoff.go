// Copyright (c) 2022 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
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
	}
	if e.AttemptLimit <= 0 {
		return fmt.Errorf("backoff attempt-limit can't be less than zero")
	}
	if e.ThresholdTime.Duration <= 0 {
		return fmt.Errorf("backoff threshold time should be than zero")
	}

	return nil
}
