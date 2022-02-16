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

package backoff_test

import (
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/backoff"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("#Backoff", func() {
	var (
		exponentialBackoff *backoff.ExponentialBackoff
		attemptLimit       uint = 3
		multiplier         uint = 2
		thresholdTime           = 6 * time.Second
	)

	BeforeEach(func() {
		exponentialBackoff = backoff.NewExponentialBackOffConfig(attemptLimit, multiplier, thresholdTime)
	})

	Context("when currentAttempt < attemptLimit", func() {
		It("should return current NextBackoffTime", func() {
			retryTime := 2 * time.Second
			backoffTime := exponentialBackoff.GetNextBackoffTime()
			Expect(backoffTime).Should(Equal(retryTime))

			backoffTime = exponentialBackoff.GetNextBackoffTime()
			Expect(backoffTime).Should(Equal(2 * retryTime))

		})
	})

	Context("when currentAttempt > attemptLimit", func() {
		It("should return threshold time ", func() {
			attempt := 0
			for attempt < 4 {
				exponentialBackoff.GetNextBackoffTime()
				attempt++
			}

			// when currentAttempt > attemptLimit
			backoffTime := exponentialBackoff.GetNextBackoffTime()
			Expect(backoffTime).Should(Equal(thresholdTime))
		})
	})
})
