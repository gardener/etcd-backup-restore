// Copyright (c) 2021 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://wwr.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common_test

import (
	"context"
	"errors"
	"time"

	. "github.com/gardener/etcd-backup-restore/pkg/common"
	mockcommon "github.com/gardener/etcd-backup-restore/pkg/mock/common"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
	"go.uber.org/atomic"
	"k8s.io/apimachinery/pkg/util/clock"
)

const (
	interval = 30 * time.Second
)

var _ = Describe("CheckerActionWatchdog", func() {
	var (
		ctrl      *gomock.Controller
		checker   *mockcommon.MockChecker
		action    *mockcommon.MockAction
		fakeClock *clock.FakeClock
		logger    *logrus.Entry
		ctx       context.Context
		count     *atomic.Int32
		watchdog  Watchdog
	)

	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())
		checker = mockcommon.NewMockChecker(ctrl)
		action = mockcommon.NewMockAction(ctrl)
		fakeClock = clock.NewFakeClock(time.Now())
		logger = logrus.New().WithField("test", "CheckerActionWatchdog")
		ctx = context.TODO()
		count = atomic.NewInt32(0)
		watchdog = NewCheckerActionWatchdog(checker, action, interval, fakeClock, logger)
	})

	AfterEach(func() {
		ctrl.Finish()
	})

	Describe("#Start / #Stop", func() {
		It("should not perform the action if the checker returns true", func() {
			checker.EXPECT().Check(gomock.Any()).DoAndReturn(func(ctx context.Context) (bool, error) {
				count.Inc()
				return true, nil
			}).Times(2)

			watchdog.Start(ctx)
			defer watchdog.Stop()
			Eventually(fakeClock.HasWaiters).Should(BeTrue())
			fakeClock.Step(interval)
			Eventually(func() int { return int(count.Load()) }).Should(Equal(2))
		})

		It("should perform the action if the checker returns false", func() {
			checker.EXPECT().Check(gomock.Any()).DoAndReturn(func(ctx context.Context) (bool, error) {
				count.Inc()
				return false, nil
			})
			action.EXPECT().Do(gomock.Any())

			watchdog.Start(ctx)
			defer watchdog.Stop()
			Eventually(func() int { return int(count.Load()) }).Should(Equal(1))
		})

		It("should perform the action if the checker returns an error", func() {
			checker.EXPECT().Check(gomock.Any()).DoAndReturn(func(ctx context.Context) (bool, error) {
				count.Inc()
				return false, errors.New("text")
			})
			action.EXPECT().Do(gomock.Any())

			watchdog.Start(ctx)
			defer watchdog.Stop()
			Eventually(func() int { return int(count.Load()) }).Should(Equal(1))
		})
	})
})
