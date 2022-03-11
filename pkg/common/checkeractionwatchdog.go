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
	Start(ctx context.Context)
	// Stop stops the goroutine started by Start.
	Stop()
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
func (w *checkerActionWatchdog) Start(ctx context.Context) {
	w.logger.Info("Starting watchdog")
	ctx, w.cancelFunc = context.WithCancel(ctx)

	go func() {
		w.logger.Debug("Starting watchdog goroutine")
		defer w.logger.Debug("Stopping watchdog goroutine")

		for {
			result, err := w.checker.Check(ctx)
			if err != nil {
				w.logger.Errorf("watchdog check fails: %v", err)
			} else if !result {
				w.logger.Info("Performing watchdog action")
				w.action.Do(ctx)
				return
			}

			select {
			case <-ctx.Done():
				return
			case <-w.clock.After(w.interval):
			}
		}
	}()
}

// Stop stops the goroutine started by Start.
func (w *checkerActionWatchdog) Stop() {
	w.logger.Info("Stopping watchdog")
	w.cancelFunc()
}
