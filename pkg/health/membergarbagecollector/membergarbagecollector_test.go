// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package membergarbagecollector_test

import (
	"context"
	"os"
	"strconv"
	"time"

	mgc "github.com/gardener/etcd-backup-restore/pkg/health/membergarbagecollector"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	mocketcdutil "github.com/gardener/etcd-backup-restore/pkg/mock/etcdutil/client"
	"github.com/gardener/etcd-backup-restore/pkg/wrappers"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/etcdserver/etcdserverpb"
	"sigs.k8s.io/controller-runtime/pkg/client"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"

	appsv1 "k8s.io/api/apps/v1"
	coordv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

var _ = Describe("Membergarbagecollector", func() {
	var (
		ctrl                  *gomock.Controller
		etcdConnectionTimeout = wrappers.Duration{Duration: 30 * time.Second}
	)

	Describe("Creating MemberGarbageCollector", func() {
		BeforeEach(func() {
			os.Setenv("POD_NAME", "test-pod")
			os.Setenv("POD_NAMESPACE", "test-namespace")
		})
		AfterEach(func() {
			os.Unsetenv("POD_NAME")
			os.Unsetenv("POD_NAMESPACE")
		})

		It("should not return error with valid configuration", func() {
			_, err := mgc.NewMemberGarbageCollector(logger, miscellaneous.GetFakeKubernetesClientSet(), etcdConnectionTimeout)
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("should return error when invalid clientset is passed", func() {
			_, err := mgc.NewMemberGarbageCollector(logger, nil, etcdConnectionTimeout)
			Expect(err).Should(HaveOccurred())
		})
	})

	Describe("Calling Remove Superfluous Members assuming a 3 member cluster", func() {
		var (
			k8sClientset client.Client
		)
		BeforeEach(func() {
			ctrl = gomock.NewController(GinkgoT())
			k8sClientset = miscellaneous.GetFakeKubernetesClientSet()
			os.Setenv("POD_NAME", "test-etcd-pod")
			os.Setenv("POD_NAMESPACE", "test-namespace")
		})
		AfterEach(func() {
			ctrl.Finish()
			os.Unsetenv("POD_NAME")
			os.Unsetenv("POD_NAMESPACE")
		})

		Context("With three member pods present", func() {
			BeforeEach(func() {
				//Create three pods
				k8sClientset.Create(context.TODO(), getPodWithName("test-etcd-0"))
				k8sClientset.Create(context.TODO(), getPodWithName("test-etcd-1"))
				k8sClientset.Create(context.TODO(), getPodWithName("test-etcd-2"))
			})
			AfterEach(func() {
				k8sClientset.Delete(context.TODO(), getPodWithName("test-etcd-0"))
				k8sClientset.Delete(context.TODO(), getPodWithName("test-etcd-1"))
				k8sClientset.Delete(context.TODO(), getPodWithName("test-etcd-2"))
			})

			It("Should not remove any members if equal members in statefulset and etcd cluster", func() {
				//Create sts object
				k8sClientset.Create(context.TODO(), getStsWithName("test-etcd", 3))

				//Create mocks
				etcdutilchecker := mocketcdutil.NewMockClusterCloser(ctrl)
				etcdutilchecker.EXPECT().MemberList(gomock.Any()).Return(getMemberListResponse(3), nil).Times(1)
				etcdutilchecker.EXPECT().MemberRemove(gomock.Any(), gomock.Any()).Return(nil, nil).Times(0)

				membergc, err := mgc.NewMemberGarbageCollector(logger, k8sClientset, etcdConnectionTimeout)
				Expect(err).To(BeNil())
				err = membergc.RemoveSuperfluousMembers(context.TODO(), etcdutilchecker)
				Expect(err).To(BeNil())

				//Cleanup
				k8sClientset.Delete(context.TODO(), getStsWithName("test-etcd", 3))
			})

			It("Should not remove any member missing from replicas but with its pod present", func() {
				//Create sts object
				k8sClientset.Create(context.TODO(), getStsWithName("test-etcd", 2))

				//Create mocks
				etcdutilchecker := mocketcdutil.NewMockClusterCloser(ctrl)
				etcdutilchecker.EXPECT().MemberList(gomock.Any()).Return(getMemberListResponse(2), nil).Times(1)
				etcdutilchecker.EXPECT().MemberRemove(gomock.Any(), gomock.Any()).Return(nil, nil).Times(0)

				membergc, err := mgc.NewMemberGarbageCollector(logger, k8sClientset, etcdConnectionTimeout)
				Expect(err).To(BeNil())
				err = membergc.RemoveSuperfluousMembers(context.TODO(), etcdutilchecker)
				Expect(err).To(BeNil())

				//Cleanup
				k8sClientset.Delete(context.TODO(), getStsWithName("test-etcd", 2))
			})
		})

		Context("With two member pods present", func() {
			BeforeEach(func() {
				//Create two pods
				k8sClientset.Create(context.TODO(), getPodWithName("test-etcd-0"))
				k8sClientset.Create(context.TODO(), getPodWithName("test-etcd-1"))
			})
			AfterEach(func() {
				k8sClientset.Delete(context.TODO(), getPodWithName("test-etcd-0"))
				k8sClientset.Delete(context.TODO(), getPodWithName("test-etcd-1"))
			})

			It("Should not remove the member with the missing pod if it has a lease present", func() {
				//Create sts object
				k8sClientset.Create(context.TODO(), getStsWithName("test-etcd", 3))

				//Create lease
				k8sClientset.Create(context.TODO(), getLeaseWithName("test-etcd-2"))

				//Create mocks
				etcdutilchecker := mocketcdutil.NewMockClusterCloser(ctrl)
				etcdutilchecker.EXPECT().MemberList(gomock.Any()).Return(getMemberListResponse(3), nil).Times(1)
				etcdutilchecker.EXPECT().MemberRemove(gomock.Any(), gomock.Any()).Return(nil, nil).Times(0)

				membergc, err := mgc.NewMemberGarbageCollector(logger, k8sClientset, etcdConnectionTimeout)
				Expect(err).To(BeNil())
				err = membergc.RemoveSuperfluousMembers(context.TODO(), etcdutilchecker)
				Expect(err).To(BeNil())

				//Cleanup
				k8sClientset.Delete(context.TODO(), getStsWithName("test-etcd", 3))
				k8sClientset.Delete(context.TODO(), getLeaseWithName("test-etcd-2"))
			})

			It("Should remove the member with the missing pod if it has no lease present", func() {
				//Create sts object
				k8sClientset.Create(context.TODO(), getStsWithName("test-etcd", 2))

				//Create mocks
				etcdutilchecker := mocketcdutil.NewMockClusterCloser(ctrl)
				etcdutilchecker.EXPECT().MemberList(gomock.Any()).Return(getMemberListResponse(3), nil).Times(1)
				etcdutilchecker.EXPECT().MemberRemove(gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)

				membergc, err := mgc.NewMemberGarbageCollector(logger, k8sClientset, etcdConnectionTimeout)
				Expect(err).To(BeNil())
				err = membergc.RemoveSuperfluousMembers(context.TODO(), etcdutilchecker)
				Expect(err).To(BeNil())

				//Cleanup
				k8sClientset.Delete(context.TODO(), getStsWithName("test-etcd", 2))
			})
		})
	})
})

func getMemberListResponse(members int) *clientv3.MemberListResponse {
	response := &clientv3.MemberListResponse{
		Header: &etcdserverpb.ResponseHeader{
			ClusterId: 123,
			MemberId:  456,
			Revision:  1,
		},
	}

	for members > 0 {
		members--
		response.Members = append(response.Members, &etcdserverpb.Member{Name: "test-etcd-" + strconv.Itoa(members), ID: uint64(members)})
	}

	return response
}

func getPodWithName(name string) *corev1.Pod {
	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: os.Getenv("POD_NAMESPACE"),
		},
	}
}

func getLeaseWithName(name string) *coordv1.Lease {
	return &coordv1.Lease{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Lease",
			APIVersion: "coordination.k8s.io/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: os.Getenv("POD_NAMESPACE"),
		},
	}
}

func getStsWithName(name string, replicas int32) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		TypeMeta: metav1.TypeMeta{
			Kind:       "StatefulSet",
			APIVersion: "apps/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: os.Getenv("POD_NAMESPACE"),
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: ptr.To(int32(replicas)),
		},
		Status: appsv1.StatefulSetStatus{
			Replicas: replicas,
		},
	}
}
