// Copyright (c) 2018 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
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

package defragmentor_test

import (
	"context"
	"fmt"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"

	. "github.com/gardener/etcd-backup-restore/pkg/defragmentor"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	cron "github.com/robfig/cron/v3"
)

var _ = Describe("Defrag", func() {
	var (
		etcdConnectionConfig *etcdutil.EtcdConnectionConfig
		keyPrefix            = "/defrag/key-"
		valuePrefix          = "val"
	)

	BeforeEach(func() {
		etcdConnectionConfig = etcdutil.NewEtcdConnectionConfig()
		etcdConnectionConfig.Endpoints = endpoints
		etcdConnectionConfig.ConnectionTimeout.Duration = 30 * time.Second
	})

	Context("Defragmentation", func() {
		BeforeEach(func() {
			now := time.Now().Unix()
			client, err := etcdutil.GetTLSClientForEtcd(etcdConnectionConfig)
			defer client.Close()
			logger.Infof("etcdConnectionConfig %v, Endpoint %v", etcdConnectionConfig, endpoints)
			Expect(err).ShouldNot(HaveOccurred())
			for index := 0; index <= 1000; index++ {
				ctx, cancel := context.WithTimeout(testCtx, etcdConnectionConfig.ConnectionTimeout.Duration)
				client.Put(ctx, fmt.Sprintf("%s%d%d", keyPrefix, now, index), valuePrefix)
				cancel()
			}
			for index := 0; index <= 500; index++ {
				ctx, cancel := context.WithTimeout(testCtx, etcdConnectionConfig.ConnectionTimeout.Duration)
				client.Delete(ctx, fmt.Sprintf("%s%d%d", keyPrefix, now, index))
				cancel()
			}
		})

		It("should defragment and reduce size of DB within time", func() {
			client, err := etcdutil.GetTLSClientForEtcd(etcdConnectionConfig)
			Expect(err).ShouldNot(HaveOccurred())
			defer client.Close()
			ctx, cancel := context.WithTimeout(testCtx, etcdDialTimeout)
			oldStatus, err := client.Status(ctx, endpoints[0])
			cancel()
			Expect(err).ShouldNot(HaveOccurred())
			oldDBSize := oldStatus.DbSize
			oldRevision := oldStatus.Header.GetRevision()

			defragmentorJob := NewDefragmentorJob(testCtx, etcdConnectionConfig, logger, nil)
			defragmentorJob.Run()

			ctx, cancel = context.WithTimeout(testCtx, etcdDialTimeout)
			newStatus, err := client.Status(ctx, endpoints[0])
			cancel()
			Expect(err).ShouldNot(HaveOccurred())

			Expect(newStatus.DbSize).Should(BeNumerically("<", oldDBSize))
			Expect(newStatus.Header.GetRevision()).Should(BeNumerically("==", oldRevision))
		})

		It("should keep size of DB same in case of timeout", func() {
			etcdConnectionConfig.ConnectionTimeout.Duration = time.Microsecond
			client, err := etcdutil.GetTLSClientForEtcd(etcdConnectionConfig)
			Expect(err).ShouldNot(HaveOccurred())
			defer client.Close()
			ctx, cancel := context.WithTimeout(testCtx, etcdDialTimeout)
			oldStatus, err := client.Status(ctx, endpoints[0])
			cancel()
			Expect(err).ShouldNot(HaveOccurred())
			oldDBSize := oldStatus.DbSize
			oldRevision := oldStatus.Header.GetRevision()

			defragmentorJob := NewDefragmentorJob(testCtx, etcdConnectionConfig, logger, nil)
			defragmentorJob.Run()

			ctx, cancel = context.WithTimeout(testCtx, etcdDialTimeout)
			newStatus, err := client.Status(ctx, endpoints[0])
			cancel()
			Expect(err).ShouldNot(HaveOccurred())

			Expect(newStatus.Header.GetRevision()).Should(BeNumerically("==", oldRevision))
			Expect(newStatus.DbSize).Should(Equal(oldDBSize))
		})

		It("should defrag periodically with callback", func() {
			defragCount := 0
			minimumExpectedDefragCount := 2
			defragSchedule, _ := cron.ParseStandard("*/1 * * * *")

			client, err := etcdutil.GetTLSClientForEtcd(etcdConnectionConfig)
			Expect(err).ShouldNot(HaveOccurred())
			defer client.Close()
			statusReqCtx, cancelStatusReq := context.WithTimeout(testCtx, etcdDialTimeout)
			oldStatus, err := client.Status(statusReqCtx, endpoints[0])
			cancelStatusReq()
			Expect(err).ShouldNot(HaveOccurred())
			oldDBSize := oldStatus.DbSize
			oldRevision := oldStatus.Header.GetRevision()

			defragThreadCtx, cancelDefragThread := context.WithTimeout(testCtx, time.Second*time.Duration(135))
			defer cancelDefragThread()
			DefragDataPeriodically(defragThreadCtx, etcdConnectionConfig, defragSchedule, func(_ context.Context) (*snapstore.Snapshot, error) {
				defragCount++
				return nil, nil
			}, logger)

			statusReqCtx, cancelStatusReq = context.WithTimeout(testCtx, etcdDialTimeout)
			newStatus, err := client.Status(statusReqCtx, endpoints[0])
			cancelStatusReq()
			Expect(err).ShouldNot(HaveOccurred())

			Expect(defragCount).Should(Or(Equal(minimumExpectedDefragCount), Equal(minimumExpectedDefragCount+1)))
			Expect(newStatus.DbSize).Should(BeNumerically("<=", oldDBSize))
			Expect(newStatus.Header.GetRevision()).Should(BeNumerically("==", oldRevision))
		})
	})
})
