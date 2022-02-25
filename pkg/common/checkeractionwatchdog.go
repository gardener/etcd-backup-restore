// Copyright (c) 2021 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"context"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/backoff"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/clock"
)

// Action is a context-aware action.
type Action interface {
	// Do performs an action.
	Do(ctx context.Context)
}

// ActionFunc is a function that implements Action.
type ActionFunc func(ctx context.Context)

// Do performs an action.
func (f ActionFunc) Do(ctx context.Context) {
	f(ctx)
}

// Watchdog manages a goroutine that can be started and stopped.
type Watchdog interface {
	// Start starts a goroutine using the given context.
	Start(context.Context, uint, *backoff.ExponentialBackoff)
	// Stop stops the goroutine started by Start.
	Stop()
	// Confirm gives the confirmation about checkerActionWatchdog check.
	Confirm(context.Context, uint, *backoff.ExponentialBackoff) bool
}

// NewCheckerActionWatchdog creates a new Watchdog that regularly checks if the condition checked by the given checker
// is true every given interval, and if the check fails or returns false, performs the given action and returns.
func NewCheckerActionWatchdog(checker Checker, action Action, interval time.Duration, clock clock.Clock, logger *logrus.Entry) Watchdog {
	return &checkerActionWatchdog{
		checker:  checker,
		action:   action,
		interval: interval,
		clock:    clock,
		logger:   logger,
	}
}

type checkerActionWatchdog struct {
	checker    Checker
	action     Action
	interval   time.Duration
	clock      clock.Clock
	logger     *logrus.Entry
	cancelFunc context.CancelFunc
}

// Start starts a goroutine that checks if the condition checked by the watchdog checker is true every watchdog interval.
// If the check fails or returns false, it performs the watchdog action and returns.
func (w *checkerActionWatchdog) Start(ctx context.Context, failureThreshold uint, backoff *backoff.ExponentialBackoff) {
	w.logger.Info("Starting watchdog")
	ctx, w.cancelFunc = context.WithCancel(ctx)

	go func() {
		w.logger.Debug("Starting watchdog goroutine")
		defer w.logger.Debug("Stopping watchdog goroutine")

		for {
			result, err := w.checker.Check(ctx)
			if err != nil || !result {
				isFail := w.Confirm(ctx, failureThreshold, backoff)
				if isFail {
					w.logger.Debug("Performing watchdog action")
					w.action.Do(ctx)
					return
				}
				backoff.ResetExponentialBackoff()
			}

			select {
			case <-ctx.Done():
				return
			case <-w.clock.After(w.interval):
			}
		}
	}()
}

// Confirm gives the confirmation that condition checked by the watchdog has failed or not.
// It returns the boolean.
func (w *checkerActionWatchdog) Confirm(ctx context.Context, failureThreshold uint, backoff *backoff.ExponentialBackoff) bool {
	w.logger.Info("Starting watchdog confirm")
	defer w.logger.Info("Stopping watchdog confirm")

	// turn off the Confirm
	if failureThreshold == 0 {
		w.logger.Info("watchdog check fails: CONFIRM")
		return true
	}

	var watchdogChecksFailCount uint = 0

	for {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(backoff.GetNextBackoffTime()):
			result, err := w.checker.Check(ctx)
			if err != nil || !result {
				watchdogChecksFailCount++
				w.logger.Debugf("watchdog ChecksFailCount: %v", watchdogChecksFailCount)
				if watchdogChecksFailCount >= failureThreshold {
					w.logger.Info("watchdog check fails: CONFIRM")
					return true
				}
			} else {
				w.logger.Info("watchdog check fails: NOT CONFIRM")
				return false
			}
		}
	}
}

// Stop stops the goroutine started by Start.
func (w *checkerActionWatchdog) Stop() {
	w.logger.Info("Stopping watchdog")
	w.cancelFunc()
}
