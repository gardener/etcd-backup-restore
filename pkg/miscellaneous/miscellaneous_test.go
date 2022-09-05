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
	"path/filepath"
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
	"k8s.io/utils/pointer"
)

var (
	snapList brtypes.SnapList
	ds       DummyStore
)

const (
	generatedSnaps   = 20
	generatedNoSnaps = 0
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

		Context("MemberPromote API call succeeds", func() {
			It("should not return error", func() {
				etcdMember := &etcdserverpb.Member{
					ID: dummyID,
				}

				clientCluster, err := factory.NewCluster()
				Expect(err).ShouldNot(HaveOccurred())

				cl.EXPECT().MemberPromote(gomock.Any(), gomock.Any()).Return(nil, nil)

				err = DoPromoteMember(testCtx, etcdMember, clientCluster, logger)
				Expect(err).ShouldNot(HaveOccurred())

			})
		})

		Context("MemberPromote API call fails", func() {
			It("should return error", func() {
				etcdMember := &etcdserverpb.Member{
					ID: dummyID,
				}

				clientCluster, err := factory.NewCluster()
				Expect(err).ShouldNot(HaveOccurred())

				cl.EXPECT().MemberPromote(gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("unable to connect to the dummy etcd"))

				err = DoPromoteMember(testCtx, etcdMember, clientCluster, logger)
				Expect(err).Should(HaveOccurred())
			})
		})

		Context("Learner is present in a cluster", func() {
			It("should return true", func() {

				clientCluster, err := factory.NewCluster()
				Expect(err).ShouldNot(HaveOccurred())

				cl.EXPECT().MemberList(gomock.Any()).DoAndReturn(func(_ context.Context) (*clientv3.MemberListResponse, error) {
					etcdMember1 := &etcdserverpb.Member{
						ID:        dummyID,
						IsLearner: true,
					}

					etcdMember2 := &etcdserverpb.Member{
						ID:        dummyID + 1,
						IsLearner: false,
					}

					response := new(clientv3.MemberListResponse)

					response.Members = append(response.Members, etcdMember1, etcdMember2)
					response.Members = []*etcdserverpb.Member{etcdMember1, etcdMember2}
					return response, nil
				})

				isPresent, err := CheckIfLearnerPresent(testCtx, clientCluster)
				Expect(isPresent).Should(BeTrue())
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

		Context("Learner is not present in a cluster", func() {
			It("should return false", func() {

				clientCluster, err := factory.NewCluster()
				Expect(err).ShouldNot(HaveOccurred())

				cl.EXPECT().MemberList(gomock.Any()).DoAndReturn(func(_ context.Context) (*clientv3.MemberListResponse, error) {
					etcdMember1 := &etcdserverpb.Member{
						ID:        dummyID,
						IsLearner: false,
					}

					etcdMember2 := &etcdserverpb.Member{
						ID:        dummyID + 1,
						IsLearner: false,
					}

					response := new(clientv3.MemberListResponse)

					response.Members = append(response.Members, etcdMember1, etcdMember2)
					response.Members = []*etcdserverpb.Member{etcdMember1, etcdMember2}
					return response, nil
				})

				isPresent, err := CheckIfLearnerPresent(testCtx, clientCluster)
				Expect(isPresent).Should(BeFalse())
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

		Context("Remove member from etcd cluster", func() {
			It("should not return error", func() {

				clientCluster, err := factory.NewCluster()
				Expect(err).ShouldNot(HaveOccurred())

				cl.EXPECT().MemberRemove(gomock.Any(), gomock.Any()).Return(nil, nil)

				err = RemoveMemberFromCluster(testCtx, clientCluster, dummyID, logger)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

		Context("Unable to remove member from etcd cluster", func() {
			It("should return error", func() {

				clientCluster, err := factory.NewCluster()
				Expect(err).ShouldNot(HaveOccurred())

				cl.EXPECT().MemberRemove(gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("unable to connect dummy etcd"))

				err = RemoveMemberFromCluster(testCtx, clientCluster, dummyID, logger)
				Expect(err).Should(HaveOccurred())
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

		Context("#Contains backup", func() {
			It("should return true", func() {
				snapList = generateSnapshotList(generatedSnaps)
				ds = NewDummyStore(snapList)
				containsBackup := ContainsBackup(&ds, logger.Logger)
				Expect(containsBackup).Should(BeTrue())
			})
		})

		Context("#Contains no backup", func() {
			It("should return false", func() {
				snapList = generateSnapshotList(generatedNoSnaps)
				ds = NewDummyStore(snapList)
				containsBackup := ContainsBackup(&ds, logger.Logger)
				Expect(containsBackup).Should(BeFalse())
			})
		})
	})

	Describe("Get the Initial ClusterState for scale-up feature", func() {
		var (
			sts             *appsv1.StatefulSet
			statefulSetName = "etcd-test"
			podName         = "etcd-test-0"
			namespace       = "test_namespace"
			emptyString     = ""
		)
		Context("In single node etcd: no scale-up", func() {
			It("Should return the cluster state as empty string ", func() {
				sts = &appsv1.StatefulSet{
					TypeMeta: metav1.TypeMeta{
						Kind:       "StatefulSet",
						APIVersion: "apps/v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      statefulSetName,
						Namespace: namespace,
					},
					Spec: appsv1.StatefulSetSpec{
						Replicas: pointer.Int32Ptr(1),
					},
					Status: appsv1.StatefulSetStatus{
						UpdatedReplicas: 1,
					},
				}
				clientSet := GetFakeKubernetesClientSet()

				err := clientSet.Create(testCtx, sts)
				Expect(err).ShouldNot(HaveOccurred())

				clusterState := GetInitialClusterStateIfScaleup(testCtx, *logger, clientSet, podName, namespace)
				Expect(clusterState).Should(Equal(emptyString))
			})
		})
		Context("In multi-node etcd bootstrap: no scale-up", func() {
			It("Should return the cluster state as empty string ", func() {
				sts = &appsv1.StatefulSet{
					TypeMeta: metav1.TypeMeta{
						Kind:       "StatefulSet",
						APIVersion: "apps/v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      statefulSetName,
						Namespace: namespace,
					},
					Spec: appsv1.StatefulSetSpec{
						Replicas: pointer.Int32Ptr(3),
					},
					Status: appsv1.StatefulSetStatus{
						UpdatedReplicas: 3,
					},
				}
				clientSet := GetFakeKubernetesClientSet()

				err := clientSet.Create(testCtx, sts)
				Expect(err).ShouldNot(HaveOccurred())

				clusterState := GetInitialClusterStateIfScaleup(testCtx, *logger, clientSet, podName, namespace)
				Expect(clusterState).Should(Equal(emptyString))
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
						Name:      statefulSetName,
						Namespace: namespace,
					},
					Spec: appsv1.StatefulSetSpec{
						Replicas: pointer.Int32Ptr(3),
					},
					Status: appsv1.StatefulSetStatus{
						UpdatedReplicas: 1,
					},
				}
				clientSet := GetFakeKubernetesClientSet()

				err := clientSet.Create(testCtx, sts)
				Expect(err).ShouldNot(HaveOccurred())

				clusterState := GetInitialClusterStateIfScaleup(testCtx, *logger, clientSet, podName, namespace)
				Expect(clusterState).Should(Equal(ClusterStateExisting))
			})
		})

		Context("Unable to fetch statefulset", func() {
			It("Should return clusterState as `new` ", func() {
				sts = &appsv1.StatefulSet{
					TypeMeta: metav1.TypeMeta{
						Kind:       "StatefulSet",
						APIVersion: "apps/v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      statefulSetName,
						Namespace: namespace,
					},
					Spec: appsv1.StatefulSetSpec{
						Replicas: pointer.Int32Ptr(3),
					},
					Status: appsv1.StatefulSetStatus{
						UpdatedReplicas: 1,
					},
				}

				wrongNamespace := "wrongNamespace"
				clientSet := GetFakeKubernetesClientSet()

				err := clientSet.Create(testCtx, sts)
				Expect(err).ShouldNot(HaveOccurred())

				clusterState := GetInitialClusterStateIfScaleup(testCtx, *logger, clientSet, podName, wrongNamespace)
				Expect(clusterState).Should(Equal(ClusterStateNew))
			})
		})
	})
	Describe("parse peer urls config", func() {
		var (
			initialAdPeerURL string
			podName          string
		)
		BeforeEach(func() {
			podName = "etcd-test-pod-0"
		})
		Context("parse peer url", func() {
			It("parsing well-defined initial-advertise-peer-urls", func() {
				initialAdPeerURL = "https@etcd-events-peer@shoot--dev--test@2380"
				peerURL, err := ParsePeerURL(initialAdPeerURL, podName)
				Expect(err).To(BeNil())
				Expect(peerURL).To(Equal("https://etcd-test-pod-0.etcd-events-peer.shoot--dev--test.svc:2380"))
			})
			It("parsing malformed initial-advertise-peer-urls", func() {
				initialAdPeerURL = "https@etcd-events-peer@shoot--dev--test"
				_, err := ParsePeerURL(initialAdPeerURL, podName)
				Expect(err).ToNot(BeNil())
			})
		})
	})
	Describe("read config file into a map", func() {
		const testdataPath = "testdata"
		var (
			configPath string
		)
		Context("valid config file", func() {
			BeforeEach(func() {
				configPath = filepath.Join(testdataPath, "valid_config.yaml")
			})
			It("test read and parse yaml", func() {
				configAsMap, err := ReadConfigFileAsMap(configPath)
				Expect(err).To(BeNil())
				Expect(configAsMap).ToNot(BeNil())
				Expect(configAsMap["name"]).To(Equal("etcd-57c38d")) //just testing one property
			})
		})
		Context("invalid file path", func() {
			It("test read and parse for a non-existent path", func() {
				configPath = "file-does-not-exist.yaml"
				_, err := ReadConfigFileAsMap(configPath)
				Expect(err).ToNot(BeNil())
			})
		})
		Context("invalid yaml file", func() {
			BeforeEach(func() {
				configPath = filepath.Join(testdataPath, "invalid_config.yaml")
			})
			It("test read and parse an invalid config yaml", func() {
				_, err := ReadConfigFileAsMap(configPath)
				Expect(err).ToNot(BeNil())
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
