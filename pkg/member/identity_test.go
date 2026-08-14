// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package member_test

import (
	"context"
	"os"
	"path/filepath"

	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	"github.com/gardener/etcd-backup-restore/pkg/member"

	"github.com/sirupsen/logrus"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = DescribeTable("ParseMemberIDFromIdentity",
	func(identity string, expectErr bool, expectedID uint64) {
		nodeID, err := member.ParseMemberIDFromIdentity(identity)
		if expectErr {
			Expect(err).To(HaveOccurred())
		} else {
			Expect(err).NotTo(HaveOccurred())
			Expect(nodeID).To(Equal(expectedID))
		}
	},
	// empty string — no member ID field at all
	Entry("empty string", "", true, uint64(0)),
	// bare hex with no colons — treated as memberID directly
	Entry("bare hex no colon", "abcdef", false, uint64(0xabcdef)),
	// leading colon — memberIDHex is "", parse must fail
	Entry("leading colon makes empty memberIDHex", ":c1c1:Member", true, uint64(0)),
	// non-hex first field
	Entry("non-hex first field", "xyz:c1c1:Member", true, uint64(0)),
	// standard Member role — role field is irrelevant to ID extraction
	Entry("standard Member role", "abcdef:c1c1:Member", false, uint64(0xabcdef)),
	// Leader role — same ID, role ignored
	Entry("Leader role same as Member", "abcdef:c1c1:Leader", false, uint64(0xabcdef)),
	// extra colons — only the first field is used
	Entry("extra colons use first field only", "abcdef:b:c:d", false, uint64(0xabcdef)),
)

var _ = DescribeTable("FormatMemberIdentity",
	func(memberID, clusterID uint64, isLeader bool, expected string) {
		Expect(member.FormatMemberIdentity(memberID, clusterID, isLeader)).To(Equal(expected))
	},
	// non-leader → "Member" role suffix, IDs in lowercase hex
	Entry("member role", uint64(0xabcdef), uint64(0xc1c1), false, "abcdef:c1c1:Member"),
	// leader → "Leader" role suffix
	Entry("leader role", uint64(0xabcdef), uint64(0xc1c1), true, "abcdef:c1c1:Leader"),
	// zero IDs still format as "0"
	Entry("zero ids", uint64(0), uint64(0), false, "0:0:Member"),
)

// FormatMemberIdentity output must round-trip through ParseMemberIDFromIdentity.
var _ = DescribeTable("FormatMemberIdentity round-trips through ParseMemberIDFromIdentity",
	func(memberID, clusterID uint64, isLeader bool) {
		identity := member.FormatMemberIdentity(memberID, clusterID, isLeader)
		parsed, err := member.ParseMemberIDFromIdentity(identity)
		Expect(err).NotTo(HaveOccurred())
		Expect(parsed).To(Equal(memberID))
	},
	Entry("member", uint64(0xabcdef), uint64(0xc1c1), false),
	Entry("leader", uint64(0x1), uint64(0x2), true),
	Entry("large ids", uint64(0xffffffffffffffff), uint64(0x1234), false),
)

var _ = Describe("WriteMemberIDFile", func() {
	var dataDir string

	BeforeEach(func() {
		dataDir, err = os.MkdirTemp("", "identity-writefile-")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(os.RemoveAll(dataDir)).To(Succeed())
	})

	It("writes the identity string to the member-id file", func() {
		Expect(member.WriteMemberIDFile(dataDir, "abcdef:c1c1:Member")).To(Succeed())

		data, err := os.ReadFile(etcdutil.MemberIDFilePath(dataDir))
		Expect(err).NotTo(HaveOccurred())
		Expect(string(data)).To(Equal("abcdef:c1c1:Member"))
	})

	It("overwrites an existing member-id file atomically", func() {
		Expect(member.WriteMemberIDFile(dataDir, "aaaa:c1c1:Member")).To(Succeed())
		Expect(member.WriteMemberIDFile(dataDir, "bbbb:c1c1:Leader")).To(Succeed())

		data, err := os.ReadFile(etcdutil.MemberIDFilePath(dataDir))
		Expect(err).NotTo(HaveOccurred())
		Expect(string(data)).To(Equal("bbbb:c1c1:Leader"))
		// The scratch file must not be left behind.
		_, statErr := os.Stat(etcdutil.MemberIDFilePath(dataDir) + ".tmp")
		Expect(os.IsNotExist(statErr)).To(BeTrue())
	})

	It("returns an error when the data directory does not exist", func() {
		err := member.WriteMemberIDFile(filepath.Join(dataDir, "missing-subdir"), "abcdef:c1c1:Member")
		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("ReadLocalMemberIDFromFile", func() {
	var (
		dataDir  string
		logEntry = logrus.New().WithField("test", "read-file")
	)

	BeforeEach(func() {
		dataDir, err = os.MkdirTemp("", "identity-readfile-")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(os.RemoveAll(dataDir)).To(Succeed())
	})

	Context("when the member-id file does not exist", func() {
		It("returns (0, false, nil)", func() {
			id, ok, err := member.ReadLocalMemberIDFromFile(logEntry, dataDir)
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeFalse())
			Expect(id).To(Equal(uint64(0)))
		})
	})

	Context("when the member-id file holds a valid identity", func() {
		It("returns the parsed member ID", func() {
			Expect(member.WriteMemberIDFile(dataDir, "abcdef:c1c1:Member")).To(Succeed())

			id, ok, err := member.ReadLocalMemberIDFromFile(logEntry, dataDir)
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeTrue())
			Expect(id).To(Equal(uint64(0xabcdef)))
		})
	})

	Context("when the member-id file holds surrounding whitespace", func() {
		It("trims and parses the member ID", func() {
			Expect(member.WriteMemberIDFile(dataDir, "  abcdef:c1c1:Member\n")).To(Succeed())

			id, ok, err := member.ReadLocalMemberIDFromFile(logEntry, dataDir)
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeTrue())
			Expect(id).To(Equal(uint64(0xabcdef)))
		})
	})

	Context("when the member-id file holds an unparseable identity", func() {
		It("returns an error", func() {
			Expect(member.WriteMemberIDFile(dataDir, "not-hex:c1c1:Member")).To(Succeed())

			_, ok, err := member.ReadLocalMemberIDFromFile(logEntry, dataDir)
			Expect(err).To(HaveOccurred())
			Expect(ok).To(BeFalse())
		})
	})
})

var _ = Describe("ReadLocalMemberIDFromLease", func() {
	const (
		leaseNamespace = "test-ns"
		leaseName      = "etcd-test-0"
	)
	var logEntry = logrus.New().WithField("test", "read-lease")

	newLease := func(holderIdentity *string) *coordinationv1.Lease {
		return &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Name: leaseName, Namespace: leaseNamespace},
			Spec:       coordinationv1.LeaseSpec{HolderIdentity: holderIdentity},
		}
	}

	Context("when the k8s client is nil", func() {
		It("returns (0, false, nil)", func() {
			id, ok, err := member.ReadLocalMemberIDFromLease(context.Background(), logEntry, nil, leaseNamespace, leaseName)
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeFalse())
			Expect(id).To(Equal(uint64(0)))
		})
	})

	Context("when the lease does not exist", func() {
		It("returns a NotFound error", func() {
			cl := fake.NewClientBuilder().Build()

			_, ok, err := member.ReadLocalMemberIDFromLease(context.Background(), logEntry, cl, leaseNamespace, leaseName)
			Expect(err).To(HaveOccurred())
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
			Expect(ok).To(BeFalse())
		})
	})

	Context("when the lease exists but has no holder identity", func() {
		It("returns (0, false, nil)", func() {
			cl := fake.NewClientBuilder().WithObjects(newLease(nil)).Build()

			id, ok, err := member.ReadLocalMemberIDFromLease(context.Background(), logEntry, cl, leaseNamespace, leaseName)
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeFalse())
			Expect(id).To(Equal(uint64(0)))
		})
	})

	Context("when the lease holder identity is valid", func() {
		It("returns the parsed member ID", func() {
			identity := "abcdef:c1c1:Member"
			cl := fake.NewClientBuilder().WithObjects(newLease(&identity)).Build()

			id, ok, err := member.ReadLocalMemberIDFromLease(context.Background(), logEntry, cl, leaseNamespace, leaseName)
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeTrue())
			Expect(id).To(Equal(uint64(0xabcdef)))
		})
	})

	Context("when the lease holder identity is unparseable", func() {
		It("returns an error", func() {
			identity := "not-hex:c1c1:Member"
			cl := fake.NewClientBuilder().WithObjects(newLease(&identity)).Build()

			_, ok, err := member.ReadLocalMemberIDFromLease(context.Background(), logEntry, cl, leaseNamespace, leaseName)
			Expect(err).To(HaveOccurred())
			Expect(ok).To(BeFalse())
		})
	})
})

var _ = Describe("ResolveLocalMemberID", func() {
	const (
		ns        = "test-ns"
		leaseName = "etcd-test-0"
	)
	var (
		dataDir  string
		logEntry = logrus.New().WithField("test", "resolve")
	)

	newLease := func(holderIdentity *string) *coordinationv1.Lease {
		return &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Name: leaseName, Namespace: ns},
			Spec:       coordinationv1.LeaseSpec{HolderIdentity: holderIdentity},
		}
	}

	BeforeEach(func() {
		dataDir, err = os.MkdirTemp("", "identity-resolve-")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(os.RemoveAll(dataDir)).To(Succeed())
	})

	Context("when the lease holds a valid identity", func() {
		It("resolves from the lease and does not consult the file", func() {
			identity := "abcdef:c1c1:Member"
			cl := fake.NewClientBuilder().WithObjects(newLease(&identity)).Build()

			id, ok, err := member.ResolveLocalMemberID(context.Background(), logEntry, dataDir, cl, ns, leaseName)
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeTrue())
			Expect(id).To(Equal(uint64(0xabcdef)))
		})
	})

	Context("when the lease is absent but the file holds a valid identity", func() {
		It("falls back to the file", func() {
			cl := fake.NewClientBuilder().Build() // lease NotFound
			Expect(member.WriteMemberIDFile(dataDir, "beef:c1c1:Member")).To(Succeed())

			id, ok, err := member.ResolveLocalMemberID(context.Background(), logEntry, dataDir, cl, ns, leaseName)
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeTrue())
			Expect(id).To(Equal(uint64(0xbeef)))
		})
	})

	Context("when neither the lease nor the file has an identity", func() {
		It("treats the member as fresh (0, false, nil)", func() {
			cl := fake.NewClientBuilder().Build()

			id, ok, err := member.ResolveLocalMemberID(context.Background(), logEntry, dataDir, cl, ns, leaseName)
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeFalse())
			Expect(id).To(Equal(uint64(0)))
		})
	})

	Context("when the nil client falls through to a valid file", func() {
		It("resolves from the file", func() {
			Expect(member.WriteMemberIDFile(dataDir, "beef:c1c1:Member")).To(Succeed())

			id, ok, err := member.ResolveLocalMemberID(context.Background(), logEntry, dataDir, nil, ns, leaseName)
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeTrue())
			Expect(id).To(Equal(uint64(0xbeef)))
		})
	})

	Context("when the file holds an unparseable identity", func() {
		It("returns an error", func() {
			cl := fake.NewClientBuilder().Build()
			Expect(member.WriteMemberIDFile(dataDir, "not-hex:c1c1:Member")).To(Succeed())

			_, ok, err := member.ResolveLocalMemberID(context.Background(), logEntry, dataDir, cl, ns, leaseName)
			Expect(err).To(HaveOccurred())
			Expect(ok).To(BeFalse())
		})
	})
})
