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
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/wrappers"
)

const (
	// defaultBackoffMultiplier defines default multiplier for BackOff-Exponential mechanism.
	defaultBackoffMultiplier = 2
	// defaultThresholdAttempt defines default no. of attempts before a ThresholdTime is achieved.
	defaultThresholdAttempt = 6
	// defaultThresholdTime defines default Threshold Backoff time used when threshold is achieved.
	defaultThresholdTime time.Duration = 128 * time.Second
	// DefaultMinimunBackoff defines default minimum Backoff time.
	DefaultMinimunBackoff time.Duration = 1 * time.Second
)

// ExponentialBackoffConfig holds the configuration for the ExponentialBackoff mechanism.
type ExponentialBackoffConfig struct {
	Start              bool              `json:"start,omitempty"`
	Multiplier         int               `json:"multiplier,omitempty"`
	Attempt            uint64            `json:"attempt,omitempty"`
	ThresholdAttempt   uint64            `json:"attemptLimit,omitempty"`
	ThresholdTime      wrappers.Duration `json:"thresholdTime,omitempty"`
	CurrentBackoffTime wrappers.Duration `json:"currentBackoff,omitempty"`
}

// NewExponentialBackOffConfig creates and returns new ExponentialBackoff.
func NewExponentialBackOffConfig() *ExponentialBackoffConfig {
	return &ExponentialBackoffConfig{
		Start:              false,
		Multiplier:         defaultBackoffMultiplier,
		ThresholdAttempt:   defaultThresholdAttempt,
		ThresholdTime:      wrappers.Duration{Duration: defaultThresholdTime},
		CurrentBackoffTime: wrappers.Duration{Duration: DefaultMinimunBackoff},
	}
}
