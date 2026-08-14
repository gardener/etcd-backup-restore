// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package member_test

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/member"
	mockfactory "github.com/gardener/etcd-backup-restore/pkg/mock/etcdutil/client"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/mock/gomock"
	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Membercontrol", func() {
	var (
		etcdConnectionConfig *brtypes.EtcdConnectionConfig
		ctrl                 *gomock.Controller
		factory              *mockfactory.MockFactory
		cl                   *mockfactory.MockClusterCloser
		memberNamePrefix     string
	)

	BeforeEach(func() {
		memberNamePrefix = ""
		etcdConnectionConfig = brtypes.NewEtcdConnectionConfig()
		etcdConnectionConfig.Endpoints = []string{etcd.Clients[0].Addr().String()}
		etcdConnectionConfig.ConnectionTimeout.Duration = 30 * time.Second
		etcdConnectionConfig.SnapshotTimeout.Duration = 30 * time.Second

		os.Setenv("POD_NAME", podName)
		os.Setenv("POD_NAMESPACE", podNamespace)

		ctrl = gomock.NewController(GinkgoT())
		factory = mockfactory.NewMockFactory(ctrl)
		cl = mockfactory.NewMockClusterCloser(ctrl)
	})

	JustBeforeEach(func() {
		urlKey := podName
		prefixLine := ""
		if memberNamePrefix != "" {
			urlKey = memberNamePrefix + "-" + podName
			prefixLine = "\nmember-name-prefix: " + memberNamePrefix
		}

		outfile := "/tmp/etcd.conf.yaml"
		etcdConfigYaml := `# Human-readable name for this member.
name: etcd1
data-dir: ` + os.Getenv("ETCD_DATA_DIR") + `
metrics: extensive
snapshot-count: 75000
quota-backend-bytes: 1073741824
listen-client-urls: http://0.0.0.0:2379
advertise-client-urls:
  ` + urlKey + `:
    - http://` + etcd.Clients[0].Addr().String() + `
initial-advertise-peer-urls:
  ` + urlKey + `:
    - http://` + etcd.Peers[0].Addr().String() + `
initial-cluster: etcd1=http://0.0.0.0:2380
initial-cluster-token: new
initial-cluster-state: new
auto-compaction-mode: periodic
auto-compaction-retention: 30m` + prefixLine

		err := os.WriteFile(outfile, []byte(etcdConfigYaml), 0755)
		Expect(err).ShouldNot(HaveOccurred())
		os.Setenv("ETCD_CONF", outfile)
	})

	AfterEach(func() {
		_ = os.Unsetenv("POD_NAME")
		_ = os.Unsetenv("ETCD_CONF")
		_ = os.Unsetenv("POD_NAMESPACE")
	})

	Describe("Creating NewMemberControl", func() {
		Context("With valid configuration", func() {
			It("should return memberControl", func() {
				ctrlMember := member.NewMemberControl(etcdConnectionConfig)
				Expect(ctrlMember).ShouldNot(BeNil())
			})
		})
	})

	Describe("While attempting to add a new member as a learner", func() {
		Context("Member is not already part of the cluster", func() {
			It("Should add member to the cluster as a learner", func() {
				mem := member.NewMemberControl(etcdConnectionConfig)
				err := mem.AddMemberAsLearner(context.TODO())
				Expect(err).To(BeNil())
				present, err := mem.IsLearnerPresent(context.TODO())
				Expect(err).To(BeNil())
				Expect(present).To(BeTrue())
			})
		})
	})

	Describe("While attempting to check if etcd is part of a cluster", func() {
		Context("If member is already part of a cluster", func() {
			It("Should return true", func() {
				mem := member.NewMemberControl(etcdConnectionConfig)
				present, err := mem.IsMemberInCluster(context.TODO())
				Expect(present).To(BeTrue())
				Expect(err).To(BeNil())
			})
		})
		Context("If member is not part of a cluster", func() {
			It("Should return false", func() {
				os.Setenv("POD_NAME", "default-0")

				mem := member.NewMemberControl(etcdConnectionConfig)
				present, err := mem.IsMemberInCluster(context.TODO())

				Expect(present).To(BeFalse())
				Expect(err).To(BeNil())
			})
		})
		Context("If member-name-prefix is set in config", func() {
			BeforeEach(func() {
				memberNamePrefix = "myprefix"
			})
			It("should apply the prefix to POD_NAME and use it for the member lookup", func() {
				mem := member.NewMemberControl(etcdConnectionConfig)
				present, err := mem.IsMemberInCluster(context.TODO())
				Expect(present).To(BeFalse())
				Expect(err).To(BeNil())
			})
		})
	})

	Describe("Update Etcd cluster member peer address", func() {
		var (
			dummyID = uint64(1111)
			m       member.Control
		)
		BeforeEach(func() {
			factory.EXPECT().NewCluster().Return(cl, nil)
		})
		JustBeforeEach(func() {
			m = member.NewMemberControl(etcdConnectionConfig)
		})

		Context("Able to connect to etcd member", func() {
			It("Should not return error", func() {
				client, err := factory.NewCluster()
				Expect(err).ShouldNot(HaveOccurred())

				cl.EXPECT().MemberList(gomock.Any()).DoAndReturn(func(_ context.Context) (*clientv3.MemberListResponse, error) {
					etcdMember1 := &etcdserverpb.Member{
						ID: dummyID,
					}
					etcdMember2 := &etcdserverpb.Member{
						ID: dummyID + 1,
					}
					response := new(clientv3.MemberListResponse)

					response.Members = append(response.Members, etcdMember1, etcdMember2)
					response.Members = []*etcdserverpb.Member{etcdMember1, etcdMember2}
					response.Header = &etcdserverpb.ResponseHeader{
						MemberId: dummyID,
					}
					return response, nil
				})

				cl.EXPECT().MemberUpdate(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)

				err = m.UpdateMemberPeerURL(context.TODO(), client)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

		Context("Unable to connect to etcd member for MemberUpdate api call", func() {
			It("Should return error", func() {
				client, err := factory.NewCluster()
				Expect(err).ShouldNot(HaveOccurred())

				cl.EXPECT().MemberList(gomock.Any()).DoAndReturn(func(_ context.Context) (*clientv3.MemberListResponse, error) {
					etcdMember1 := &etcdserverpb.Member{
						ID: dummyID,
					}
					etcdMember2 := &etcdserverpb.Member{
						ID: dummyID + 1,
					}
					response := new(clientv3.MemberListResponse)

					response.Members = append(response.Members, etcdMember1, etcdMember2)
					response.Members = []*etcdserverpb.Member{etcdMember1, etcdMember2}
					response.Header = &etcdserverpb.ResponseHeader{
						MemberId: dummyID,
					}
					return response, nil
				})

				cl.EXPECT().MemberUpdate(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("unable to connect to dummy etcd"))

				err = m.UpdateMemberPeerURL(context.TODO(), client)
				Expect(err).Should(HaveOccurred())
			})
		})
	})

	Describe("Cluster marked for scale-up", func() {
		var (
			m member.Control
		)
		JustBeforeEach(func() {
			m = member.NewMemberControl(etcdConnectionConfig)
		})
		Context("When cluster is up and member is not part of the list", func() {
			It("should return true", func() {
				os.Setenv("POD_NAME", "default-0")
				m = member.NewMemberControl(etcdConnectionConfig)

				isScaleUp, err := m.IsClusterScaledUp(testCtx)
				Expect(isScaleUp).Should(BeTrue())
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

		Context("When cluster is up and member is already a part of cluster", func() {
			It("should return false", func() {
				isScaleUp, err := m.IsClusterScaledUp(testCtx)
				Expect(isScaleUp).Should(BeFalse())
				Expect(err).ShouldNot(HaveOccurred())
			})
		})
	})

	Describe("Checking whether the member was permanently removed", func() {
		// WasPermanentlyRemoved resolves the local member ID (from the lease named
		// after the member, falling back to the member-id file) and then inspects
		// the boltdb members_removed tombstone bucket. memberName == podName ==
		// "etcd-test-0" and podNamespace == "test-podnamespace" here, matching the
		// env vars set in the outer BeforeEach.
		var (
			dataDir string
			m       member.Control
		)

		newLease := func(holderIdentity *string) *coordinationv1.Lease {
			return &coordinationv1.Lease{
				ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: podNamespace},
				Spec:       coordinationv1.LeaseSpec{HolderIdentity: holderIdentity},
			}
		}

		BeforeEach(func() {
			dataDir, err = os.MkdirTemp("", "was-removed-")
			Expect(err).NotTo(HaveOccurred())
		})

		JustBeforeEach(func() {
			m = member.NewMemberControl(etcdConnectionConfig)
		})

		AfterEach(func() {
			Expect(os.RemoveAll(dataDir)).To(Succeed())
		})

		Context("when the local member ID cannot be resolved (no lease, no file)", func() {
			It("returns false without inspecting the tombstone", func() {
				cl := fake.NewClientBuilder().Build()

				removed, err := m.WasPermanentlyRemoved(testCtx, dataDir, cl)
				Expect(err).NotTo(HaveOccurred())
				Expect(removed).To(BeFalse())
			})
		})

		Context("when the member ID is resolved from the lease and is in the tombstone", func() {
			It("reports the member as permanently removed", func() {
				identity := "abcdef:c1c1:Member"
				cl := fake.NewClientBuilder().WithObjects(newLease(&identity)).Build()
				writeTombstoneDB(dataDir, true, uint64(0xabcdef))

				removed, err := m.WasPermanentlyRemoved(testCtx, dataDir, cl)
				Expect(err).NotTo(HaveOccurred())
				Expect(removed).To(BeTrue())
			})
		})

		Context("when the member ID is resolved but is not in the tombstone", func() {
			It("reports the member as not removed", func() {
				identity := "abcdef:c1c1:Member"
				cl := fake.NewClientBuilder().WithObjects(newLease(&identity)).Build()
				writeTombstoneDB(dataDir, true, uint64(0xdef))

				removed, err := m.WasPermanentlyRemoved(testCtx, dataDir, cl)
				Expect(err).NotTo(HaveOccurred())
				Expect(removed).To(BeFalse())
			})
		})

		Context("when the member ID is resolved from the member-id file (lease absent)", func() {
			It("reports the member as permanently removed", func() {
				cl := fake.NewClientBuilder().Build() // lease NotFound
				Expect(member.WriteMemberIDFile(dataDir, "abcdef:c1c1:Member")).To(Succeed())
				writeTombstoneDB(dataDir, true, uint64(0xabcdef))

				removed, err := m.WasPermanentlyRemoved(testCtx, dataDir, cl)
				Expect(err).NotTo(HaveOccurred())
				Expect(removed).To(BeTrue())
			})
		})

		Context("when the member ID is resolved but the boltdb backend is missing", func() {
			It("fails closed (possible partial deletion)", func() {
				identity := "abcdef:c1c1:Member"
				cl := fake.NewClientBuilder().WithObjects(newLease(&identity)).Build()
				// No boltdb written under dataDir/member/snap/db.

				removed, err := m.WasPermanentlyRemoved(testCtx, dataDir, cl)
				Expect(err).To(HaveOccurred())
				Expect(err).To(MatchError(member.ErrMembershipCheckFailed))
				Expect(removed).To(BeFalse())
			})
		})
	})
})
