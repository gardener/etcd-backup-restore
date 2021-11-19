package heartbeat_test

import (
	"context"
	"k8s.io/utils/pointer"
	"os"
	"time"

	heartbeat "github.com/gardener/etcd-backup-restore/pkg/health/heartbeat"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"

	v1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/controller-runtime/pkg/client"
	fake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

var _ = Describe("Heartbeat", func() {
	var (
		etcdConnectionConfig *brtypes.EtcdConnectionConfig
	)

	BeforeEach(func() {
		etcdConnectionConfig = brtypes.NewEtcdConnectionConfig()
		etcdConnectionConfig.Endpoints = []string{etcd.Clients[0].Addr().String()}
		etcdConnectionConfig.ConnectionTimeout.Duration = 5 * time.Second
	})

	Describe("creating Heartbeat", func() {
		BeforeEach(func() {
			os.Setenv("POD_NAME", "test_pod")
			os.Setenv("POD_NAMESPACE", "test_namespace")
		})
		AfterEach(func() {
			os.Unsetenv("POD_NAME")
			os.Unsetenv("POD_NAMESPACE")
		})
		Context("With valid config", func() {
			It("should not return error", func() {
				_, err := heartbeat.NewHeartbeat(logger, etcdConnectionConfig, miscellaneous.GetFakeKubernetesClientSet())
				Expect(err).ShouldNot(HaveOccurred())
			})
		})
		Context("With invalid etcdconnection config passed", func() {
			It("should return error", func() {
				_, err := heartbeat.NewHeartbeat(logger, nil, miscellaneous.GetFakeKubernetesClientSet())
				Expect(err).Should(HaveOccurred())
			})
		})
		Context("With invalid clientset passed", func() {
			It("should return error", func() {
				_, err := heartbeat.NewHeartbeat(logger, etcdConnectionConfig, nil)
				Expect(err).Should(HaveOccurred())
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
				os.Setenv("POD_NAME", "test_pod")
				os.Setenv("POD_NAMESPACE", "test_namespace")
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
				os.Unsetenv("POD_NAME")
				os.Unsetenv("POD_NAMESPACE")
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
				k8sClientset.Get(context.TODO(), client.ObjectKey{
					Namespace: lease.Namespace,
					Name:      lease.Name,
				}, l)

				Expect(l.Spec.HolderIdentity).To(PointTo(Equal("989")))
				Expect(err).ShouldNot(HaveOccurred())

				err = k8sClientset.Delete(context.TODO(), l)
				Expect(err).ShouldNot(HaveOccurred())
			})
			It("Should return error if no snapshot is passed", func() {
				Expect(os.Getenv("POD_NAME")).Should(Equal("test_pod"))
				Expect(os.Getenv("POD_NAMESPACE")).Should(Equal("test_namespace"))

				k8sClientset.Create(context.TODO(), lease)

				err = heartbeat.UpdateFullSnapshotLease(context.TODO(), logger, nil, k8sClientset, brtypes.DefaultFullSnapshotLeaseName)
				Expect(err).Should(HaveOccurred())

				err = k8sClientset.Delete(context.TODO(), lease)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})
		Context("With valid delta snapshot lease present", func() {
			BeforeEach(func() {
				os.Setenv("POD_NAME", "test_pod")
				os.Setenv("POD_NAMESPACE", "test_namespace")
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
			It("Should correctly update holder identity of delta snapshot lease when delta snapshot list is passed", func() {
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
				Expect(err).ShouldNot(HaveOccurred())

				err = k8sClientset.Delete(context.TODO(), l)
				Expect(err).ShouldNot(HaveOccurred())
			})
			FIt("Should not update delta snapshot with holderIdentity of full snapshot if no delta snapshot list is passed", func() {
				Expect(os.Getenv("POD_NAME")).Should(Equal("test_pod"))
				Expect(os.Getenv("POD_NAMESPACE")).Should(Equal("test_namespace"))

				lease.Spec.HolderIdentity = pointer.StringPtr("foo")
				err = k8sClientset.Create(context.TODO(), lease)

				err = heartbeat.UpdateDeltaSnapshotLease(context.TODO(), logger, nil, k8sClientset, brtypes.DefaultDeltaSnapshotLeaseName)
				Expect(err).ShouldNot(HaveOccurred())

				l := &v1.Lease{}
				err = k8sClientset.Get(context.TODO(), client.ObjectKey{
					Namespace: os.Getenv("POD_NAMESPACE"),
					Name:      brtypes.DefaultDeltaSnapshotLeaseName,
				}, l)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(l.Spec).To(Equal(lease.Spec))

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
			})
			AfterEach(func() {
				os.Unsetenv("POD_NAME")
				os.Unsetenv("POD_NAMESPACE")
			})
			It("Should correctly update the member lease", func() {
				clientSet := miscellaneous.GetFakeKubernetesClientSet()
				heartbeat, err := heartbeat.NewHeartbeat(logger, etcdConnectionConfig, clientSet)
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
				heartbeat, err := heartbeat.NewHeartbeat(logger, etcdConnectionConfig, miscellaneous.GetFakeKubernetesClientSet())
				Expect(err).ShouldNot(HaveOccurred())

				err = heartbeat.RenewMemberLease(context.TODO())
				Expect(err).Should(HaveOccurred())
			})
		})
	})
})
