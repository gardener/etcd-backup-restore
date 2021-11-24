package membergarbagecollector_test

import (
	"context"
	"os"
	"strconv"

	mgc "github.com/gardener/etcd-backup-restore/pkg/health/membergarbagecollector"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	mocketcdutil "github.com/gardener/etcd-backup-restore/pkg/mock/etcdutil/client"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/etcdserver/etcdserverpb"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = Describe("Membergarbagecollector", func() {
	var (
		ctrl *gomock.Controller
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
			_, err := mgc.NewMemberGarbageCollector(logger, miscellaneous.GetFakeKubernetesClientSet())
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("should return error when invalid clientset is passed", func() {
			_, err := mgc.NewMemberGarbageCollector(logger, nil)
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
			os.Setenv("POD_NAME", "test-pod")
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
				k8sClientset.Create(context.TODO(), getPodWithName("etcd-0"))
				k8sClientset.Create(context.TODO(), getPodWithName("etcd-1"))
				k8sClientset.Create(context.TODO(), getPodWithName("etcd-2"))
			})
			AfterEach(func() {
				k8sClientset.Delete(context.TODO(), getPodWithName("etcd-0"))
				k8sClientset.Delete(context.TODO(), getPodWithName("etcd-1"))
				k8sClientset.Delete(context.TODO(), getPodWithName("etcd-2"))
			})

			It("Should not remove any members if all members present in etcd.status.members", func() {
				//Create etcd object
				k8sClientset.Create(context.TODO(), getEtcdObjectWithMembers(3))

				//Create mocks
				etcdutilchecker := mocketcdutil.NewMockClusterCloser(ctrl)
				etcdutilchecker.EXPECT().MemberList(gomock.Any()).Return(getMemberListResponse(3), nil).Times(1)
				etcdutilchecker.EXPECT().MemberRemove(gomock.Any(), gomock.Any()).Return(nil, nil).Times(0)

				membergc, err := mgc.NewMemberGarbageCollector(logger, k8sClientset)
				Expect(err).To(BeNil())
				err = membergc.RemoveSuperfluousMembers(context.TODO(), etcdutilchecker)
				Expect(err).To(BeNil())

				//Cleanup
				k8sClientset.Delete(context.TODO(), getEtcdObjectWithMembers(3))
			})

			It("Should not remove any member missing from etcd.status.members", func() {
				//Create etcd object
				k8sClientset.Create(context.TODO(), getEtcdObjectWithMembers(2))

				//Create mocks
				etcdutilchecker := mocketcdutil.NewMockClusterCloser(ctrl)
				etcdutilchecker.EXPECT().MemberList(gomock.Any()).Return(getMemberListResponse(2), nil).Times(1)
				etcdutilchecker.EXPECT().MemberRemove(gomock.Any(), gomock.Any()).Return(nil, nil).Times(0)

				membergc, err := mgc.NewMemberGarbageCollector(logger, k8sClientset)
				Expect(err).To(BeNil())
				err = membergc.RemoveSuperfluousMembers(context.TODO(), etcdutilchecker)
				Expect(err).To(BeNil())

				//Cleanup
				k8sClientset.Delete(context.TODO(), getEtcdObjectWithMembers(2))
			})
		})

		Context("With two member pods present", func() {
			BeforeEach(func() {
				//Create three pods
				k8sClientset.Create(context.TODO(), getPodWithName("etcd-0"))
				k8sClientset.Create(context.TODO(), getPodWithName("etcd-1"))
			})
			AfterEach(func() {
				k8sClientset.Delete(context.TODO(), getPodWithName("etcd-0"))
				k8sClientset.Delete(context.TODO(), getPodWithName("etcd-1"))
			})

			It("Should not remove the member with the missing pod if it has an entry in etcd.status.members", func() {
				//Create etcd object
				k8sClientset.Create(context.TODO(), getEtcdObjectWithMembers(3))

				//Create mocks
				etcdutilchecker := mocketcdutil.NewMockClusterCloser(ctrl)
				etcdutilchecker.EXPECT().MemberList(gomock.Any()).Return(getMemberListResponse(3), nil).Times(1)
				etcdutilchecker.EXPECT().MemberRemove(gomock.Any(), gomock.Any()).Return(nil, nil).Times(0)

				membergc, err := mgc.NewMemberGarbageCollector(logger, k8sClientset)
				Expect(err).To(BeNil())
				err = membergc.RemoveSuperfluousMembers(context.TODO(), etcdutilchecker)
				Expect(err).To(BeNil())

				//Cleanup
				k8sClientset.Delete(context.TODO(), getEtcdObjectWithMembers(3))
			})

			It("Should remove the member with the missing pod if it has no entry in etcd.status.members", func() {
				//Create etcd object
				k8sClientset.Create(context.TODO(), getEtcdObjectWithMembers(2))

				//Create mocks
				etcdutilchecker := mocketcdutil.NewMockClusterCloser(ctrl)
				etcdutilchecker.EXPECT().MemberList(gomock.Any()).Return(getMemberListResponse(3), nil).Times(1)
				etcdutilchecker.EXPECT().MemberRemove(gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)

				membergc, err := mgc.NewMemberGarbageCollector(logger, k8sClientset)
				Expect(err).To(BeNil())
				err = membergc.RemoveSuperfluousMembers(context.TODO(), etcdutilchecker)
				Expect(err).To(BeNil())

				//Cleanup
				k8sClientset.Delete(context.TODO(), getEtcdObjectWithMembers(2))
			})
		})
	})
})

func getEtcdObjectWithMembers(members int) *druidv1alpha1.Etcd {
	etcd := &druidv1alpha1.Etcd{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Etcd",
			APIVersion: "druid.gardener.cloud/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test",
			Namespace: os.Getenv("POD_NAMESPACE"),
		},
	}

	for members > 0 {
		members--
		etcd.Status.Members = append(etcd.Status.Members, druidv1alpha1.EtcdMemberStatus{Name: "etcd-" + strconv.Itoa(members)})
	}

	return etcd
}

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
		response.Members = append(response.Members, &etcdserverpb.Member{Name: "etcd-" + strconv.Itoa(members), ID: uint64(members)})
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
