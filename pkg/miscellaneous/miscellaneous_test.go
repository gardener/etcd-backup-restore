// Copyright (c) 2021 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
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

package miscellaneous

import (
	"context"
	"fmt"
	"io"
	"os"
	"reflect"
	"time"

	mockfactory "github.com/gardener/etcd-backup-restore/pkg/mock/etcdutil/client"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/etcdserver/etcdserverpb"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	snapList brtypes.SnapList
	ds       DummyStore
)

const (
	generatedSnaps = 20
)

var _ = Describe("Miscellaneous Tests", func() {
	var (
		etcdConnectionConfig *brtypes.EtcdConnectionConfig
		ctrl                 *gomock.Controller
		factory              *mockfactory.MockFactory
		cm                   *mockfactory.MockMaintenanceCloser
		cl                   *mockfactory.MockClusterCloser
	)

	BeforeEach(func() {
		etcdConnectionConfig = brtypes.NewEtcdConnectionConfig()
		etcdConnectionConfig.Endpoints = []string{"http://127.0.0.1:2379"}
		etcdConnectionConfig.ConnectionTimeout.Duration = 30 * time.Second
		etcdConnectionConfig.SnapshotTimeout.Duration = 30 * time.Second
		etcdConnectionConfig.DefragTimeout.Duration = 30 * time.Second

		ctrl = gomock.NewController(GinkgoT())
		factory = mockfactory.NewMockFactory(ctrl)
		cm = mockfactory.NewMockMaintenanceCloser(ctrl)
		cl = mockfactory.NewMockClusterCloser(ctrl)
	})

	Describe("Filtering snapshots", func() {
		BeforeEach(func() {
			snapList = generateSnapshotList(generatedSnaps)
			ds = NewDummyStore(snapList)
		})
		It("should return the whole snaplist", func() {
			snaps, err := GetFilteredBackups(&ds, -1, nil)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(snaps)).To(Equal(len(snapList)))
		})
		It("should get the last 3 snapshots and its deltas", func() {
			n := 3
			snaps, err := GetFilteredBackups(&ds, n, nil)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(snaps)).To(Equal(n * 2))
			expectedSnapID := 0
			for i := 0; i < n; i++ {
				if reflect.DeepEqual(snaps[i].Kind, brtypes.SnapshotKindFull) {
					Expect(snaps[i].SnapName).To(Equal(fmt.Sprintf("%s-%d", brtypes.SnapshotKindFull, expectedSnapID)))
					Expect(snaps[i+1].SnapName).To(Equal(fmt.Sprintf("%s-%d", brtypes.SnapshotKindDelta, expectedSnapID)))
					expectedSnapID++
				}
			}
		})
		It("should get the last backups created in the past 5 days", func() {
			n := 5
			backups, err := GetFilteredBackups(&ds, n, func(snap brtypes.Snapshot) bool {
				return snap.CreatedOn.After(time.Now().UTC().AddDate(0, 0, -n))
			})
			Expect(err).ToNot(HaveOccurred())
			Expect(len(backups)).To(Equal(n * 2))
			for i := 0; i < n; i++ {
				if reflect.DeepEqual(backups[i].Kind, brtypes.SnapshotKindFull) {
					backups[i].CreatedOn.After(time.Now().UTC().AddDate(0, 0, -n))
				}
			}
		})
	})

	Describe("Etcd Cluster", func() {
		var (
			dummyID              = uint64(1111)
			dummyClientEndpoints = []string{"http://127.0.0.1:2379", "http://127.0.0.1:9090"}
		)
		BeforeEach(func() {
			factory.EXPECT().NewMaintenance().Return(cm, nil)
			factory.EXPECT().NewCluster().Return(cl, nil)
		})

		Context("MemberList API call succeeds", func() {
			It("should return the endpoints of all etcd cluster members", func() {
				client, err := factory.NewCluster()
				Expect(err).ShouldNot(HaveOccurred())

				cl.EXPECT().MemberList(gomock.Any()).DoAndReturn(func(_ context.Context) (*clientv3.MemberListResponse, error) {
					response := new(clientv3.MemberListResponse)
					// two dummy etcd cluster members:
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
				})

				etcdEndpoints, err := GetAllEtcdEndpoints(testCtx, client, etcdConnectionConfig, logger)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(etcdEndpoints).To(Equal(dummyClientEndpoints))
			})
		})

		Context("MemberList API call fails", func() {
			It("should return error", func() {
				client, err := factory.NewCluster()
				Expect(err).ShouldNot(HaveOccurred())

				cl.EXPECT().MemberList(gomock.Any()).Return(nil, fmt.Errorf("unable to connect to the dummy etcd"))

				etcdEndpoints, err := GetAllEtcdEndpoints(testCtx, client, etcdConnectionConfig, logger)
				Expect(err).Should(HaveOccurred())
				Expect(etcdEndpoints).Should(BeNil())
			})
		})

		Context("Status API call succeeds", func() {
			It("should return boolean indicating etcd cluster is healthy", func() {
				clientMaintenance, err := factory.NewMaintenance()
				Expect(err).ShouldNot(HaveOccurred())

				cm.EXPECT().Status(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ string) (*clientv3.StatusResponse, error) {
					response := new(clientv3.StatusResponse)
					return response, nil
				}).AnyTimes()

				isHealthy, err := IsEtcdClusterHealthy(testCtx, clientMaintenance, etcdConnectionConfig, dummyClientEndpoints, logger)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(isHealthy).To(BeTrue())
			})
		})

		Context("Status API call fails", func() {
			It("should return error", func() {
				clientMaintenance, err := factory.NewMaintenance()
				Expect(err).ShouldNot(HaveOccurred())

				client, err := factory.NewCluster()
				Expect(err).ShouldNot(HaveOccurred())

				cm.EXPECT().Status(gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("unable to connect to the dummy etcd")).AnyTimes()

				isHealthy, err := IsEtcdClusterHealthy(testCtx, clientMaintenance, etcdConnectionConfig, dummyClientEndpoints, logger)
				Expect(err).Should(HaveOccurred())
				Expect(isHealthy).To(BeFalse())

				leaderID, endpoint, err := GetLeader(testCtx, clientMaintenance, client, dummyClientEndpoints[0])
				Expect(err).Should(HaveOccurred())
				Expect(leaderID).Should(Equal(NoLeaderState))
				Expect(endpoint).Should(BeNil())
			})
		})

		Context("Have No Quorum or No etcd Leader present", func() {
			It("should return error", func() {
				clientMaintenance, err := factory.NewMaintenance()
				Expect(err).ShouldNot(HaveOccurred())

				client, err := factory.NewCluster()
				Expect(err).ShouldNot(HaveOccurred())

				cm.EXPECT().Status(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ string) (*clientv3.StatusResponse, error) {
					response := new(clientv3.StatusResponse)
					response.Leader = NoLeaderState
					return response, nil
				})

				leaderID, endpoint, err := GetLeader(testCtx, clientMaintenance, client, dummyClientEndpoints[0])
				Expect(err).Should(HaveOccurred())
				Expect(leaderID).Should(Equal(NoLeaderState))
				Expect(endpoint).Should(BeNil())
			})
		})

		Context("Both Status and MemberList API call suceeds", func() {
			It("should return etcd cluster's LeaderID and its endpoints", func() {
				clientMaintenance, err := factory.NewMaintenance()
				Expect(err).ShouldNot(HaveOccurred())

				client, err := factory.NewCluster()
				Expect(err).ShouldNot(HaveOccurred())

				cm.EXPECT().Status(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ string) (*clientv3.StatusResponse, error) {
					response := new(clientv3.StatusResponse)
					// setting the etcd leaderID
					response.Leader = dummyID
					return response, nil
				})

				cl.EXPECT().MemberList(gomock.Any()).DoAndReturn(func(_ context.Context) (*clientv3.MemberListResponse, error) {
					response := new(clientv3.MemberListResponse)
					// etcd Leader
					dummyMember1 := &etcdserverpb.Member{
						ID:         dummyID,
						ClientURLs: []string{dummyClientEndpoints[0]},
					}
					// etcd Follower
					dummyMember2 := &etcdserverpb.Member{
						ID:         dummyID + 1,
						ClientURLs: []string{dummyClientEndpoints[1]},
					}
					response.Members = []*etcdserverpb.Member{dummyMember1, dummyMember2}
					return response, nil
				})

				leaderID, endpoint, err := GetLeader(testCtx, clientMaintenance, client, dummyClientEndpoints[0])
				Expect(err).ShouldNot(HaveOccurred())
				Expect(leaderID).Should(Equal(dummyID))
				Expect(endpoint).Should(Equal([]string{dummyClientEndpoints[0]}))
			})
		})

		Context("Status API call succeeds and MemberList API call fails", func() {
			It("should return error and etcd cluster's LeaderID", func() {
				clientMaintenance, err := factory.NewMaintenance()
				Expect(err).ShouldNot(HaveOccurred())

				client, err := factory.NewCluster()
				Expect(err).ShouldNot(HaveOccurred())

				cm.EXPECT().Status(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ string) (*clientv3.StatusResponse, error) {
					response := new(clientv3.StatusResponse)
					// setting the etcd leaderID
					response.Leader = dummyID
					return response, nil
				})

				cl.EXPECT().MemberList(gomock.Any()).Return(nil, fmt.Errorf("unable to connect to the dummy etcd"))

				leaderID, endpoint, err := GetLeader(testCtx, clientMaintenance, client, dummyClientEndpoints[0])
				Expect(err).Should(HaveOccurred())
				Expect(leaderID).Should(Equal(dummyID))
				Expect(endpoint).Should(BeNil())
			})
		})

	})

	Describe("BackupLeaderEndpoint", func() {
		var (
			portNo    = uint(8080)
			endpoints = []string{"http://127.0.0.1:2379"}
		)

		Context("Empty Etcd endpoint passed", func() {
			It("should return error", func() {
				_, err := GetBackupLeaderEndPoint([]string{}, portNo)
				Expect(err).Should(HaveOccurred())
			})
		})

		Context("Correct Etcd endpoint passed", func() {
			It("should return backupLeaderEndPoint", func() {
				backupLeaderEndPoint, err := GetBackupLeaderEndPoint(endpoints, portNo)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(backupLeaderEndPoint).To(Equal("http://127.0.0.1:8080"))
			})
		})
	})

	Describe("Check Emptiness of backup-bucket", func() {
		var snapStoreConfig *brtypes.SnapstoreConfig
		BeforeEach(func() {
			snapStoreConfig = snapstore.NewSnapstoreConfig()
			snapStoreConfig.Provider = "Local"
		})
		Context("#Empty backup-bucket", func() {
			It("should return true", func() {
				isBackupBucketEmpty := IsBackupBucketEmpty(snapStoreConfig, logger.Logger)
				Expect(isBackupBucketEmpty).Should(BeTrue())
			})
		})
		Context("#Storage provider is not specified", func() {
			It("should return true", func() {
				snapStoreConfig.Provider = ""
				isBackupBucketEmpty := IsBackupBucketEmpty(snapStoreConfig, logger.Logger)
				Expect(isBackupBucketEmpty).Should(BeTrue())
			})
		})
	})

	Describe("Get the Initial ClusterState", func() {
		var (
			sts *appsv1.StatefulSet
		)
		BeforeEach(func() {
			os.Setenv("STS_NAME", "etcd-test")
			os.Setenv("POD_NAME", "etcd-test-0")
			os.Setenv("NAMESPACE", "test_namespace")
		})
		AfterEach(func() {
			os.Unsetenv("STS_NAME")
			os.Unsetenv("POD_NAME")
			os.Unsetenv("NAMESPACE")
		})
		Context("In single-node etcd", func() {
			It("Should return the cluster state as `new` ", func() {
				sts = &appsv1.StatefulSet{
					TypeMeta: metav1.TypeMeta{
						Kind:       "StatefulSet",
						APIVersion: "apps/v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      os.Getenv("STS_NAME"),
						Namespace: os.Getenv("NAMESPACE"),
					},
					Spec: appsv1.StatefulSetSpec{
						Replicas: getInt32Pointer(1),
					},
					Status: appsv1.StatefulSetStatus{
						UpdatedReplicas: 1,
					},
				}
				clientSet := GetFakeKubernetesClientSet()

				err := clientSet.Create(testCtx, sts)
				Expect(err).ShouldNot(HaveOccurred())

				clusterState := GetInitialClusterState(testCtx, *logger, clientSet, os.Getenv("POD_NAME"), os.Getenv("NAMESPACE"))
				Expect(clusterState).Should(Equal(ClusterStateNew))
			})
		})
		Context("In multi-node etcd bootstrap", func() {
			It("Should return the cluster state as `new` ", func() {
				sts = &appsv1.StatefulSet{
					TypeMeta: metav1.TypeMeta{
						Kind:       "StatefulSet",
						APIVersion: "apps/v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      os.Getenv("STS_NAME"),
						Namespace: os.Getenv("NAMESPACE"),
					},
					Spec: appsv1.StatefulSetSpec{
						Replicas: getInt32Pointer(3),
					},
					Status: appsv1.StatefulSetStatus{
						UpdatedReplicas: 3,
					},
				}
				clientSet := GetFakeKubernetesClientSet()

				err := clientSet.Create(testCtx, sts)
				Expect(err).ShouldNot(HaveOccurred())

				clusterState := GetInitialClusterState(testCtx, *logger, clientSet, os.Getenv("POD_NAME"), os.Getenv("NAMESPACE"))
				Expect(clusterState).Should(Equal(ClusterStateNew))
			})
		})
		Context("In case of Scaling up from single node to multi-node etcd", func() {
			It("Should return clusterState as `existing` ", func() {
				sts = &appsv1.StatefulSet{
					TypeMeta: metav1.TypeMeta{
						Kind:       "StatefulSet",
						APIVersion: "apps/v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      os.Getenv("STS_NAME"),
						Namespace: os.Getenv("NAMESPACE"),
					},
					Spec: appsv1.StatefulSetSpec{
						Replicas: getInt32Pointer(3),
					},
					Status: appsv1.StatefulSetStatus{
						UpdatedReplicas: 1,
					},
				}
				clientSet := GetFakeKubernetesClientSet()

				err := clientSet.Create(testCtx, sts)
				Expect(err).ShouldNot(HaveOccurred())

				clusterState := GetInitialClusterState(testCtx, *logger, clientSet, os.Getenv("POD_NAME"), os.Getenv("NAMESPACE"))
				Expect(clusterState).Should(Equal(ClusterStateExisting))
			})
		})
	})

})

func generateSnapshotList(n int) brtypes.SnapList {
	snapList := brtypes.SnapList{}
	for i := 0; i < n; i++ {
		fullSnap := &brtypes.Snapshot{
			SnapName:  fmt.Sprintf("%s-%d", brtypes.SnapshotKindFull, i),
			Kind:      brtypes.SnapshotKindFull,
			CreatedOn: time.Now().UTC().AddDate(0, 0, -i),
		}
		deltaSnap := &brtypes.Snapshot{
			SnapName:  fmt.Sprintf("%s-%d", brtypes.SnapshotKindDelta, i),
			Kind:      brtypes.SnapshotKindDelta,
			CreatedOn: time.Now().UTC().AddDate(0, 0, -i),
		}
		snapList = append(snapList, fullSnap, deltaSnap)
	}
	return snapList
}

type DummyStore struct {
	SnapList brtypes.SnapList
}

func NewDummyStore(snapList brtypes.SnapList) DummyStore {
	return DummyStore{SnapList: snapList}
}

func (ds *DummyStore) List() (brtypes.SnapList, error) {
	return ds.SnapList, nil
}

func (ds *DummyStore) Delete(s brtypes.Snapshot) error {
	return nil
}

func (ds *DummyStore) Save(snap brtypes.Snapshot, rc io.ReadCloser) error {
	return nil
}

func (ds *DummyStore) Fetch(snap brtypes.Snapshot) (io.ReadCloser, error) {
	return nil, nil
}

func getInt32Pointer(val int) *int32 {
	value := int32(val)
	return &value
}
