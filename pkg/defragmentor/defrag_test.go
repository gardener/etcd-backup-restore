// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package defragmentor_test

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/gardener/etcd-backup-restore/test/utils"

	cron "github.com/robfig/cron/v3"
	clientv3 "go.etcd.io/etcd/client/v3"

	. "github.com/gardener/etcd-backup-restore/pkg/defragmentor"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Defrag", func() {
	var (
		etcdConnectionConfig *brtypes.EtcdConnectionConfig
		keyPrefix            = "/defrag/key-"
		valuePrefix          = "val"
	)

	BeforeEach(func() {
		etcdConnectionConfig = brtypes.NewEtcdConnectionConfig()
		etcdConnectionConfig.Endpoints = endpoints
		etcdConnectionConfig.ConnectionTimeout.Duration = 30 * time.Second
		etcdConnectionConfig.SnapshotTimeout.Duration = 30 * time.Second
		etcdConnectionConfig.DefragTimeout.Duration = 30 * time.Second

	})

	Context("Defragmentation", func() {
		BeforeEach(func() {
			now := time.Now().Unix()
			clientFactory := etcdutil.NewFactory(*etcdConnectionConfig)
			clientKV, err := clientFactory.NewKV()
			Expect(err).ShouldNot(HaveOccurred())
			defer clientKV.Close()
			logger.Infof("etcdConnectionConfig %v, Endpoint %v", etcdConnectionConfig, endpoints)
			for index := 0; index <= 1000; index++ {
				ctx, cancel := context.WithTimeout(testCtx, etcdConnectionConfig.ConnectionTimeout.Duration)
				_, err = clientKV.Put(ctx, fmt.Sprintf("%s%d%d", keyPrefix, now, index), valuePrefix)
				Expect(err).ShouldNot(HaveOccurred())
				cancel()
			}
			for index := 0; index <= 500; index++ {
				ctx, cancel := context.WithTimeout(testCtx, etcdConnectionConfig.ConnectionTimeout.Duration)
				_, err = clientKV.Delete(ctx, fmt.Sprintf("%s%d%d", keyPrefix, now, index))
				Expect(err).ShouldNot(HaveOccurred())
				cancel()
			}
		})

		It("should defragment and reduce size of DB within time", func() {
			clientFactory := etcdutil.NewFactory(*etcdConnectionConfig)

			clientMaintenance, err := clientFactory.NewMaintenance()
			Expect(err).ShouldNot(HaveOccurred())
			defer clientMaintenance.Close()

			clientKV, err := clientFactory.NewKV()
			Expect(err).ShouldNot(HaveOccurred())
			defer clientKV.Close()

			ctx, cancel := context.WithTimeout(testCtx, etcdDialTimeout)
			oldStatus, err := clientMaintenance.Status(ctx, endpoints[0])
			cancel()
			Expect(err).ShouldNot(HaveOccurred())
			oldDBSize := oldStatus.DbSize
			oldRevision := oldStatus.Header.GetRevision()

			// compact the ETCD DB to let the defragmentor have full effect
			_, err = clientKV.Compact(testCtx, oldRevision, clientv3.WithCompactPhysical())
			Expect(err).ShouldNot(HaveOccurred())
			defragmentorJob := NewDefragmentorJob(testCtx, etcdConnectionConfig, logger, nil)
			defragmentorJob.Run()

			ctx, cancel = context.WithTimeout(testCtx, etcdDialTimeout)
			newStatus, err := clientMaintenance.Status(ctx, endpoints[0])
			cancel()
			Expect(err).ShouldNot(HaveOccurred())

			Expect(newStatus.DbSize).Should(BeNumerically("<", oldDBSize))
			Expect(newStatus.Header.GetRevision()).Should(BeNumerically("==", oldRevision))
		})

		It("should keep size of DB same in case of timeout", func() {
			etcdConnectionConfig.DefragTimeout.Duration = time.Microsecond
			clientFactory := etcdutil.NewFactory(*etcdConnectionConfig)

			clientMaintenance, err := clientFactory.NewMaintenance()
			Expect(err).ShouldNot(HaveOccurred())
			defer clientMaintenance.Close()

			ctx, cancel := context.WithTimeout(testCtx, etcdDialTimeout)
			oldStatus, err := clientMaintenance.Status(ctx, endpoints[0])

			Expect(err).ShouldNot(HaveOccurred())
			oldDBSize := oldStatus.DbSize
			oldRevision := oldStatus.Header.GetRevision()

			defragmentorJob := NewDefragmentorJob(ctx, etcdConnectionConfig, logger, nil)
			defragmentorJob.Run()
			cancel()

			ctx, cancel = context.WithTimeout(testCtx, etcdDialTimeout)
			newStatus, err := clientMaintenance.Status(ctx, endpoints[0])
			cancel()
			Expect(err).ShouldNot(HaveOccurred())

			Expect(newStatus.Header.GetRevision()).Should(BeNumerically("==", oldRevision))
			Expect(newStatus.DbSize).Should(Equal(oldDBSize))
		})

		It("should defrag periodically with callback", func() {
			defragCount := 0
			minimumExpectedDefragCount := 2
			defragSchedule, _ := cron.ParseStandard("*/1 * * * *")

			// Then populate the etcd with some more data to add the subsequent delta snapshots
			populatorCtx, cancelPopulator := context.WithTimeout(testCtx, 5*time.Second)
			defer cancelPopulator()
			resp := &utils.EtcdDataPopulationResponse{}
			wg := &sync.WaitGroup{}
			wg.Add(1)
			go utils.PopulateEtcdWithWaitGroup(populatorCtx, wg, logger, endpoints, "", "", resp)
			Expect(resp.Err).ShouldNot(HaveOccurred())

			// Wait unitil the populator finishes with populating ETCD
			wg.Wait()

			clientFactory := etcdutil.NewFactory(*etcdConnectionConfig)
			clientMaintenance, err := clientFactory.NewMaintenance()
			Expect(err).ShouldNot(HaveOccurred())
			defer clientMaintenance.Close()

			statusReqCtx, cancelStatusReq := context.WithTimeout(testCtx, etcdDialTimeout)
			oldStatus, err := clientMaintenance.Status(statusReqCtx, endpoints[0])
			cancelStatusReq()
			Expect(err).ShouldNot(HaveOccurred())
			oldDBSize := oldStatus.DbSize
			oldRevision := oldStatus.Header.GetRevision()

			defragThreadCtx, cancelDefragThread := context.WithTimeout(testCtx, time.Second*time.Duration(235))
			defer cancelDefragThread()
			DefragDataPeriodically(defragThreadCtx, etcdConnectionConfig, defragSchedule, func(_ context.Context, _ bool) (*brtypes.Snapshot, error) {
				defragCount++
				return nil, nil
			}, logger)

			statusReqCtx, cancelStatusReq = context.WithTimeout(testCtx, etcdDialTimeout)
			newStatus, err := clientMaintenance.Status(statusReqCtx, endpoints[0])
			cancelStatusReq()
			Expect(err).ShouldNot(HaveOccurred())

			Expect(defragCount).Should(Or(Equal(minimumExpectedDefragCount), Equal(minimumExpectedDefragCount+1)))
			Expect(newStatus.DbSize).Should(BeNumerically("<", oldDBSize))
			Expect(newStatus.Header.GetRevision()).Should(BeNumerically("==", oldRevision))
		})
	})
})
