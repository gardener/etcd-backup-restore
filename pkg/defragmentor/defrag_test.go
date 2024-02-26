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
	mockfactory "github.com/gardener/etcd-backup-restore/pkg/mock/etcdutil/client"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/gardener/etcd-backup-restore/test/utils"
	"github.com/golang/mock/gomock"
	cron "github.com/robfig/cron/v3"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/etcdserver/etcdserverpb"

	. "github.com/gardener/etcd-backup-restore/pkg/defragmentor"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Defrag", func() {
	var (
		etcdConnectionConfig *brtypes.EtcdConnectionConfig
		keyPrefix            = "/defrag/key-"
		valuePrefix          = "val"

		ctrl    *gomock.Controller
		factory *mockfactory.MockFactory
		cm      *mockfactory.MockMaintenanceCloser
		cl      *mockfactory.MockClusterCloser
	)

	BeforeEach(func() {
		etcdConnectionConfig = brtypes.NewEtcdConnectionConfig()
		etcdConnectionConfig.Endpoints = endpoints
		etcdConnectionConfig.ConnectionTimeout.Duration = 30 * time.Second
		etcdConnectionConfig.SnapshotTimeout.Duration = 30 * time.Second
		etcdConnectionConfig.DefragTimeout.Duration = 30 * time.Second

		ctrl = gomock.NewController(GinkgoT())
		factory = mockfactory.NewMockFactory(ctrl)
		cm = mockfactory.NewMockMaintenanceCloser(ctrl)
		cl = mockfactory.NewMockClusterCloser(ctrl)
	})

	Describe("With Etcd cluster", func() {
		var (
			dummyID              = uint64(1111)
			dummyClientEndpoints = []string{"http://127.0.0.1:2379", "http://127.0.0.1:9090"}
		)
		BeforeEach(func() {
			factory.EXPECT().NewMaintenance().Return(cm, nil)
			factory.EXPECT().NewCluster().Return(cl, nil)
		})

		Context("MemberList API call fails", func() {
			It("should return error", func() {
				clientMaintenance, err := factory.NewMaintenance()
				Expect(err).ShouldNot(HaveOccurred())

				client, err := factory.NewCluster()
				Expect(err).ShouldNot(HaveOccurred())

				cl.EXPECT().MemberList(gomock.Any()).Return(nil, fmt.Errorf("failed to connect with the dummy etcd")).AnyTimes()

				leaderEtcdEndpoints, followerEtcdEndpoints, err := etcdutil.GetEtcdEndPointsSorted(testCtx, clientMaintenance, client, dummyClientEndpoints, logger)
				Expect(err).Should(HaveOccurred())
				Expect(leaderEtcdEndpoints).Should(BeNil())
				Expect(followerEtcdEndpoints).Should(BeNil())

				err = etcdutil.DefragmentData(testCtx, clientMaintenance, client, dummyClientEndpoints, mockTimeout, logger)
				Expect(err).Should(HaveOccurred())
			})
		})

		Context("MemberList API call succeeds and Status API call fails", func() {
			It("should return error", func() {
				clientMaintenance, err := factory.NewMaintenance()
				Expect(err).ShouldNot(HaveOccurred())

				client, err := factory.NewCluster()
				Expect(err).ShouldNot(HaveOccurred())

				cl.EXPECT().MemberList(gomock.Any()).DoAndReturn(func(_ context.Context) (*clientv3.MemberListResponse, error) {
					response := new(clientv3.MemberListResponse)
					dummyMember1 := &etcdserverpb.Member{
						ID:         dummyID,
						ClientURLs: []string{dummyClientEndpoints[0]},
					}
					dummyMember2 := &etcdserverpb.Member{
						ID:         dummyID + 1,
						ClientURLs: []string{dummyClientEndpoints[1]},
					}
					response.Members = []*etcdserverpb.Member{dummyMember1, dummyMember2}
					return response, nil
				}).AnyTimes()

				cm.EXPECT().Status(gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("failed to connect to the dummy etcd")).AnyTimes()

				leaderEtcdEndpoints, followerEtcdEndpoints, err := etcdutil.GetEtcdEndPointsSorted(testCtx, clientMaintenance, client, dummyClientEndpoints, logger)
				Expect(err).Should(HaveOccurred())
				Expect(leaderEtcdEndpoints).Should(BeNil())
				Expect(followerEtcdEndpoints).Should(BeNil())

				err = etcdutil.DefragmentData(testCtx, clientMaintenance, client, dummyClientEndpoints, mockTimeout, logger)
				Expect(err).Should(HaveOccurred())
			})
		})

		Context("Only Defragment API call fails", func() {
			It("should return error and fail to perform defragmentation", func() {
				clientMaintenance, err := factory.NewMaintenance()
				Expect(err).ShouldNot(HaveOccurred())

				client, err := factory.NewCluster()
				Expect(err).ShouldNot(HaveOccurred())

				cl.EXPECT().MemberList(gomock.Any()).DoAndReturn(func(_ context.Context) (*clientv3.MemberListResponse, error) {
					response := new(clientv3.MemberListResponse)
					// dummy etcd cluster leader
					dummyMember1 := &etcdserverpb.Member{
						ID:         dummyID,
						ClientURLs: []string{dummyClientEndpoints[0]},
					}
					// dummy etcd cluster follower
					dummyMember2 := &etcdserverpb.Member{
						ID:         dummyID + 1,
						ClientURLs: []string{dummyClientEndpoints[1]},
					}
					response.Members = []*etcdserverpb.Member{dummyMember1, dummyMember2}
					return response, nil
				}).AnyTimes()

				cm.EXPECT().Status(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ string) (*clientv3.StatusResponse, error) {
					response := new(clientv3.StatusResponse)
					response.Leader = dummyID
					return response, nil
				}).Times(2)

				cm.EXPECT().Status(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ string) (*clientv3.StatusResponse, error) {
					response := new(clientv3.StatusResponse)
					response.DbSize = 1
					return response, nil
				}).Times(1)

				cm.EXPECT().Defragment(gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("failed to defrag the etcd")).AnyTimes()

				leaderEtcdEndpoints, followerEtcdEndpoints, err := etcdutil.GetEtcdEndPointsSorted(testCtx, clientMaintenance, client, dummyClientEndpoints, logger)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(leaderEtcdEndpoints).Should(Equal([]string{dummyClientEndpoints[0]}))
				Expect(followerEtcdEndpoints).Should(Equal([]string{dummyClientEndpoints[1]}))

				err = etcdutil.DefragmentData(testCtx, clientMaintenance, client, dummyClientEndpoints, mockTimeout, logger)
				Expect(err).Should(HaveOccurred())
			})
		})

		Context("All etcd client API call succeeds", func() {
			It("should able to perform the defragmentation on etcd follower as well as on etcd leader", func() {
				clientMaintenance, err := factory.NewMaintenance()
				Expect(err).ShouldNot(HaveOccurred())

				client, err := factory.NewCluster()
				Expect(err).ShouldNot(HaveOccurred())

				cl.EXPECT().MemberList(gomock.Any()).DoAndReturn(func(_ context.Context) (*clientv3.MemberListResponse, error) {
					response := new(clientv3.MemberListResponse)
					// etcd cluster leader
					dummyMember1 := &etcdserverpb.Member{
						ID:         dummyID,
						ClientURLs: []string{dummyClientEndpoints[0]},
					}
					// etcd cluster follower
					dummyMember2 := &etcdserverpb.Member{
						ID:         dummyID + 1,
						ClientURLs: []string{dummyClientEndpoints[1]},
					}
					response.Members = []*etcdserverpb.Member{dummyMember1, dummyMember2}
					return response, nil
				}).AnyTimes()

				cm.EXPECT().Status(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ string) (*clientv3.StatusResponse, error) {
					response := new(clientv3.StatusResponse)
					response.Leader = dummyID
					response.DbSize = 10
					return response, nil
				}).AnyTimes()

				cm.EXPECT().Defragment(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ string) (*clientv3.DefragmentResponse, error) {
					response := new(clientv3.DefragmentResponse)
					return response, nil
				}).AnyTimes()

				err = etcdutil.DefragmentData(testCtx, clientMaintenance, client, dummyClientEndpoints, mockTimeout, logger)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})
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
				clientKV.Put(ctx, fmt.Sprintf("%s%d%d", keyPrefix, now, index), valuePrefix)
				cancel()
			}
			for index := 0; index <= 500; index++ {
				ctx, cancel := context.WithTimeout(testCtx, etcdConnectionConfig.ConnectionTimeout.Duration)
				clientKV.Delete(ctx, fmt.Sprintf("%s%d%d", keyPrefix, now, index))
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
			go utils.PopulateEtcdWithWaitGroup(populatorCtx, wg, logger, endpoints, resp)
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
