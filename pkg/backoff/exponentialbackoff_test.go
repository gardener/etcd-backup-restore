// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package backoff_test

import (
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/backoff"

	. "github.com/onsi/ginkgo/v2"
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
