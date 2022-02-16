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

package backoff

import (
	"time"
)

//  Exponential-Backoff mechanism example with default values:
//  -----------------------------------------------
//  # currentAttempt   CurrentBackoffTime(seconds)
//      1                    2s                     => CurrentBackoffTime = CurrentBackoffTime * multiplier when [currentAttempt < attemptLimit]
//      2                    4s
//      3                    8s
//      4                   16s
//      5                   32s
//      6                   64s
//      7                  128s                     => CurrentBackoffTime = thresholdTime when [currentAttempt > attemptLimit]
//      8                  128s
//      9                  128s
//  -----------------------------------------------

const (
	// defaultMinimumBackoff defines default minimum Backoff time.
	defaultMinimumBackoff time.Duration = 1 * time.Second
)

// BackOff interface defines Exponential-Backoff mechanism.
type BackOff interface {
	// GetNextBackoffTime returns the duration to wait before retrying the operation.
	GetNextBackoffTime()

	// Reset to ExponentialBackoff initial state.
	ResetExponentialBackoff()
}

// ExponentialBackoff holds the configuration for the ExponentialBackoff mechanism.
type ExponentialBackoff struct {
	Start              bool
	multiplier         uint
	attemptLimit       uint
	currentAttempt     uint
	thresholdTime      time.Duration
	currentBackoffTime time.Duration
}

// NewExponentialBackOffConfig returns new ExponentialBackoff.
func NewExponentialBackOffConfig(attempt uint, multiplier uint, thresholdTime time.Duration) *ExponentialBackoff {
	return &ExponentialBackoff{
		Start:              false,
		multiplier:         multiplier,
		attemptLimit:       attempt,
		thresholdTime:      thresholdTime,
		currentBackoffTime: defaultMinimumBackoff,
	}
}

// GetNextBackoffTime returns the duration to wait before retrying the operation.
func (e *ExponentialBackoff) GetNextBackoffTime() time.Duration {
	if e.currentAttempt > e.attemptLimit {
		return e.thresholdTime
	}

	e.currentAttempt++
	e.currentBackoffTime *= time.Duration(e.multiplier)
	return e.currentBackoffTime
}

// ResetExponentialBackoff resets the ExponentialBackoff to initial state.
func (e *ExponentialBackoff) ResetExponentialBackoff() {
	// set backoff start to false
	e.Start = false

	// reset the backoff time time with default.
	e.currentBackoffTime = defaultMinimumBackoff

	// reset the no. of attempt to 0.
	e.currentAttempt = 0
}
