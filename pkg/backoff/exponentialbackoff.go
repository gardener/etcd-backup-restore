// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

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
