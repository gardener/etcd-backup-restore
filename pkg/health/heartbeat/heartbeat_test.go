package heartbeat_test

import (
	"context"
	"os"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/health/heartbeat"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/gardener/etcd-backup-restore/pkg/wrappers"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"

	v1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

var _ = Describe("Heartbeat", func() {
	var (
		etcdConnectionConfig *brtypes.EtcdConnectionConfig
		metadata             map[string]string
	)

	BeforeEach(func() {
		etcdConnectionConfig = brtypes.NewEtcdConnectionConfig()
		etcdConnectionConfig.Endpoints = []string{etcd.Clients[0].Addr().String()}
		etcdConnectionConfig.ConnectionTimeout.Duration = 5 * time.Second
		metadata = map[string]string{}
	})

	Describe("creating Heartbeat", func() {
		BeforeEach(func() {
			Expect(os.Setenv("POD_NAME", "test_pod")).To(Succeed())
			Expect(os.Setenv("POD_NAMESPACE", "test_namespace")).To(Succeed())
		})
		AfterEach(func() {
			Expect(os.Unsetenv("POD_NAME")).To(Succeed())
			Expect(os.Unsetenv("POD_NAMESPACE")).To(Succeed())
		})
		Context("With valid config", func() {
			It("should not return error", func() {
				_, err := heartbeat.NewHeartbeat(logger, etcdConnectionConfig, miscellaneous.GetFakeKubernetesClientSet(), metadata)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})
		Context("With invalid etcdconnection config passed", func() {
			It("should return error", func() {
				_, err := heartbeat.NewHeartbeat(logger, nil, miscellaneous.GetFakeKubernetesClientSet(), metadata)
				Expect(err).Should(HaveOccurred())
			})
		})
		Context("With invalid clientset passed", func() {
			It("should return error", func() {
				_, err := heartbeat.NewHeartbeat(logger, etcdConnectionConfig, nil, metadata)
				Expect(err).Should(HaveOccurred())
			})
		})
		Context("With valid config and metadata", func() {
			BeforeEach(func() {
				metadata[heartbeat.PeerUrlTLSEnabledKey] = "true"
			})
			It("should not return error", func() {
				_, err := heartbeat.NewHeartbeat(logger, etcdConnectionConfig, miscellaneous.GetFakeKubernetesClientSet(), metadata)
				Expect(err).ToNot(HaveOccurred())
			})
		})
	})

	Describe("Renewing snapshot lease", func() {
		var (
			lease        *v1.Lease
			k8sClientset client.Client
		)
		Context("With valid full snapshot lease present", func() {
			BeforeEach(func() {
				Expect(os.Setenv("POD_NAME", "test_pod")).To(Succeed())
				Expect(os.Setenv("POD_NAMESPACE", "test_namespace")).To(Succeed())
				k8sClientset = fake.NewClientBuilder().Build()
				lease = &v1.Lease{
					TypeMeta: metav1.TypeMeta{
						Kind:       "Lease",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      brtypes.DefaultFullSnapshotLeaseName,
						Namespace: os.Getenv("POD_NAMESPACE"),
					},
				}
			})
			AfterEach(func() {
				Expect(os.Unsetenv("POD_NAME")).To(Succeed())
				Expect(os.Unsetenv("POD_NAMESPACE")).To(Succeed())
			})
			It("Should Correctly update holder identity of full snapshot lease", func() {
				Expect(os.Getenv("POD_NAME")).Should(Equal("test_pod"))
				Expect(os.Getenv("POD_NAMESPACE")).Should(Equal("test_namespace"))

				snap := &brtypes.Snapshot{
					Kind:          brtypes.SnapshotKindFull,
					CreatedOn:     time.Now(),
					StartRevision: 0,
					LastRevision:  989,
				}
				snap.GenerateSnapshotName()
				err := k8sClientset.Create(context.TODO(), lease)
				Expect(err).ShouldNot(HaveOccurred())

				err = heartbeat.UpdateFullSnapshotLease(context.TODO(), logger, snap, k8sClientset, brtypes.DefaultFullSnapshotLeaseName)
				Expect(err).ShouldNot(HaveOccurred())

				l := &v1.Lease{}
				Expect(k8sClientset.Get(context.TODO(), client.ObjectKey{
					Namespace: lease.Namespace,
					Name:      lease.Name,
				}, l)).To(Succeed())

				Expect(l.Spec.HolderIdentity).To(PointTo(Equal("989")))
				Expect(err).ShouldNot(HaveOccurred())

				err = k8sClientset.Delete(context.TODO(), l)
				Expect(err).ShouldNot(HaveOccurred())
			})
			It("Should return error if no snapshot is passed", func() {
				Expect(os.Getenv("POD_NAME")).Should(Equal("test_pod"))
				Expect(os.Getenv("POD_NAMESPACE")).Should(Equal("test_namespace"))

				Expect(k8sClientset.Create(context.TODO(), lease)).To(Succeed())

				err = heartbeat.UpdateFullSnapshotLease(context.TODO(), logger, nil, k8sClientset, brtypes.DefaultFullSnapshotLeaseName)
				Expect(err).Should(HaveOccurred())

				err = k8sClientset.Delete(context.TODO(), lease)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})
		Context("With valid delta snapshot lease present", func() {
			BeforeEach(func() {
				Expect(os.Setenv("POD_NAME", "test_pod")).To(Succeed())
				Expect(os.Setenv("POD_NAMESPACE", "test_namespace")).To(Succeed())
				k8sClientset = fake.NewClientBuilder().Build()
				lease = &v1.Lease{
					TypeMeta: metav1.TypeMeta{
						Kind:       "Lease",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      brtypes.DefaultDeltaSnapshotLeaseName,
						Namespace: os.Getenv("POD_NAMESPACE"),
					},
				}
			})
			AfterEach(func() {
				os.Unsetenv("POD_NAME")
				os.Unsetenv("POD_NAMESPACE")
			})
			It("Should renew and correctly update holder identity of delta snapshot lease when delta snapshot list is passed", func() {
				Expect(os.Getenv("POD_NAME")).Should(Equal("test_pod"))
				Expect(os.Getenv("POD_NAMESPACE")).Should(Equal("test_namespace"))

				var snapList brtypes.SnapList
				deltasnap1 := &brtypes.Snapshot{
					Kind:          brtypes.SnapshotKindDelta,
					CreatedOn:     time.Now(),
					StartRevision: 0,
					LastRevision:  989,
				}
				deltasnap2 := &brtypes.Snapshot{
					Kind:          brtypes.SnapshotKindDelta,
					CreatedOn:     time.Now(),
					StartRevision: 999,
					LastRevision:  1900,
				}
				deltasnap3 := &brtypes.Snapshot{
					Kind:          brtypes.SnapshotKindDelta,
					CreatedOn:     time.Now(),
					StartRevision: 1901,
					LastRevision:  2500,
				}
				snapList = append(snapList, deltasnap1)
				snapList = append(snapList, deltasnap2)
				snapList = append(snapList, deltasnap3)
				err = k8sClientset.Create(context.TODO(), lease)
				Expect(err).ShouldNot(HaveOccurred())

				err = heartbeat.UpdateDeltaSnapshotLease(context.TODO(), logger, snapList, k8sClientset, brtypes.DefaultDeltaSnapshotLeaseName)
				Expect(err).ShouldNot(HaveOccurred())

				l := &v1.Lease{}
				err = k8sClientset.Get(context.TODO(), client.ObjectKey{
					Namespace: os.Getenv("POD_NAMESPACE"),
					Name:      brtypes.DefaultDeltaSnapshotLeaseName,
				}, l)

				Expect(l.Spec.HolderIdentity).To(PointTo(Equal("2500")))
				Expect(l.Spec.RenewTime).ToNot(BeNil())
				Expect(err).ShouldNot(HaveOccurred())

				err = k8sClientset.Delete(context.TODO(), l)
				Expect(err).ShouldNot(HaveOccurred())
			})
			It("Should renew delta snapshot and set holderid to 0 if no delta snapshot list is passed", func() {
				Expect(os.Getenv("POD_NAME")).Should(Equal("test_pod"))
				Expect(os.Getenv("POD_NAMESPACE")).Should(Equal("test_namespace"))

				err = k8sClientset.Create(context.TODO(), lease)

				err = heartbeat.UpdateDeltaSnapshotLease(context.TODO(), logger, nil, k8sClientset, brtypes.DefaultDeltaSnapshotLeaseName)
				Expect(err).ShouldNot(HaveOccurred())

				l := &v1.Lease{}
				err = k8sClientset.Get(context.TODO(), client.ObjectKey{
					Namespace: os.Getenv("POD_NAMESPACE"),
					Name:      brtypes.DefaultDeltaSnapshotLeaseName,
				}, l)
				Expect(err).ShouldNot(HaveOccurred())

				Expect(l.Spec.RenewTime).ToNot(BeNil())
				//Expect(l.Spec.HolderIdentity).To(BeNil())
				Expect(l.Spec.HolderIdentity).To(PointTo(Equal("0")))

				err = k8sClientset.Delete(context.TODO(), l)
				Expect(err).ShouldNot(HaveOccurred())
			})
			It("Should only renew delta snapshot if no delta snapshot list is passed when holderidentity is already set", func() {
				Expect(os.Getenv("POD_NAME")).Should(Equal("test_pod"))
				Expect(os.Getenv("POD_NAMESPACE")).Should(Equal("test_namespace"))

				actor := "123"
				newLease := lease
				newLease.Spec.HolderIdentity = &actor

				err = k8sClientset.Create(context.TODO(), newLease)

				err = heartbeat.UpdateDeltaSnapshotLease(context.TODO(), logger, nil, k8sClientset, brtypes.DefaultDeltaSnapshotLeaseName)
				Expect(err).ShouldNot(HaveOccurred())

				l := &v1.Lease{}
				err = k8sClientset.Get(context.TODO(), client.ObjectKey{
					Namespace: os.Getenv("POD_NAMESPACE"),
					Name:      brtypes.DefaultDeltaSnapshotLeaseName,
				}, l)
				Expect(err).ShouldNot(HaveOccurred())

				Expect(l.Spec.RenewTime).ToNot(BeNil())
				Expect(l.Spec.HolderIdentity).To(PointTo(Equal("123")))

				err = k8sClientset.Delete(context.TODO(), l)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})
	})
	Describe("Renewing member lease", func() {
		var (
			lease *v1.Lease
			pod   *corev1.Pod
		)
		Context("With corresponding member lease present", func() {
			BeforeEach(func() {
				os.Setenv("POD_NAME", "test_pod")
				os.Setenv("POD_NAMESPACE", "test_namespace")
				lease = &v1.Lease{
					TypeMeta: metav1.TypeMeta{
						Kind:       "Lease",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      os.Getenv("POD_NAME"),
						Namespace: os.Getenv("POD_NAMESPACE"),
					},
				}
				pod = &corev1.Pod{
					TypeMeta: metav1.TypeMeta{
						Kind:       "Pod",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      os.Getenv("POD_NAME"),
						Namespace: os.Getenv("POD_NAMESPACE"),
					},
				}
				metadata = map[string]string{}
			})
			AfterEach(func() {
				os.Unsetenv("POD_NAME")
				os.Unsetenv("POD_NAMESPACE")
			})
			It("Should correctly update the member lease", func() {
				clientSet := miscellaneous.GetFakeKubernetesClientSet()
				heartbeat, err := heartbeat.NewHeartbeat(logger, etcdConnectionConfig, clientSet, metadata)
				Expect(err).ShouldNot(HaveOccurred())

				err = clientSet.Create(context.TODO(), lease)
				Expect(err).ShouldNot(HaveOccurred())
				err = clientSet.Create(context.TODO(), pod)
				Expect(err).ShouldNot(HaveOccurred())

				err = heartbeat.RenewMemberLease(context.TODO())
				Expect(err).ShouldNot(HaveOccurred())

				l := &v1.Lease{}
				err = clientSet.Get(context.TODO(), client.ObjectKey{
					Namespace: os.Getenv("POD_NAMESPACE"),
					Name:      os.Getenv("POD_NAME"),
				}, l)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(l.Spec.HolderIdentity).ToNot(BeNil())

				err = clientSet.Delete(context.TODO(), l)
				Expect(err).ShouldNot(HaveOccurred())
				err = clientSet.Delete(context.TODO(), pod)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})
		Context("With corresponding member lease not present", func() {
			BeforeEach(func() {
				os.Setenv("POD_NAME", "test_pod")
				os.Setenv("POD_NAMESPACE", "test_namespace")
			})
			AfterEach(func() {
				os.Unsetenv("POD_NAME")
				os.Unsetenv("POD_NAMESPACE")
			})
			It("Should return an error", func() {
				heartbeat, err := heartbeat.NewHeartbeat(logger, etcdConnectionConfig, miscellaneous.GetFakeKubernetesClientSet(), metadata)
				Expect(err).ShouldNot(HaveOccurred())

				err = heartbeat.RenewMemberLease(context.TODO())
				Expect(err).Should(HaveOccurred())
			})
		})
	})

	Describe("Renewing member lease periodically", func() {
		var (
			mmStopCh          chan struct{}
			hConfig           = &brtypes.HealthConfig{}
			leaseUpdatePeriod = 2 * time.Second
		)
		BeforeEach(func() {
			mmStopCh = make(chan struct{})
			hConfig = &brtypes.HealthConfig{
				MemberLeaseRenewalEnabled: true,
				HeartbeatDuration:         wrappers.Duration{Duration: leaseUpdatePeriod},
			}
		})
		Context("With fail to create clientset", func() {
			It("Should return an error", func() {
				err := heartbeat.RenewMemberLeasePeriodically(testCtx, mmStopCh, hConfig, logger, etcdConnectionConfig, "https://etcd-main-0:2379")
				Expect(err).Should(HaveOccurred())
			})
		})
	})

})
