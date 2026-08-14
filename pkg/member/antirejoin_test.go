// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package member_test

import (
	"os"
	"path/filepath"

	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	"github.com/gardener/etcd-backup-restore/pkg/member"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/server/v3/mvcc/buckets"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// writeTombstoneDB creates a boltdb backend file at BackendDBPath(dataDir),
// optionally creating the members_removed bucket and seeding it with the given
// member IDs (as tombstones). If createBucket is false the bucket is not created,
// simulating a cluster where no member has ever been removed.
func writeTombstoneDB(dataDir string, createBucket bool, removedIDs ...uint64) {
	dbPath := etcdutil.BackendDBPath(dataDir)
	ExpectWithOffset(1, os.MkdirAll(filepath.Dir(dbPath), 0700)).To(Succeed())

	db, err := bolt.Open(dbPath, 0600, nil)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	defer func() { _ = db.Close() }()

	ExpectWithOffset(1, db.Update(func(tx *bolt.Tx) error {
		if !createBucket {
			return nil
		}
		b, err := tx.CreateBucketIfNotExists(buckets.MembersRemoved.Name())
		if err != nil {
			return err
		}
		for _, id := range removedIDs {
			if err := b.Put([]byte(types.ID(id).String()), []byte("")); err != nil {
				return err
			}
		}
		return nil
	})).To(Succeed())
}

var _ = Describe("IsMemberRemoved", func() {
	var dataDir string

	BeforeEach(func() {
		dataDir, err = os.MkdirTemp("", "antirejoin-isremoved-")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(os.RemoveAll(dataDir)).To(Succeed())
	})

	Context("when the member ID is present in the members_removed bucket", func() {
		It("reports the member as removed", func() {
			writeTombstoneDB(dataDir, true, uint64(0xabc))

			removed, err := member.IsMemberRemoved(etcdutil.BackendDBPath(dataDir), uint64(0xabc))
			Expect(err).NotTo(HaveOccurred())
			Expect(removed).To(BeTrue())
		})
	})

	Context("when the member ID is not in the members_removed bucket", func() {
		It("reports the member as not removed", func() {
			writeTombstoneDB(dataDir, true, uint64(0xabc))

			removed, err := member.IsMemberRemoved(etcdutil.BackendDBPath(dataDir), uint64(0xdef))
			Expect(err).NotTo(HaveOccurred())
			Expect(removed).To(BeFalse())
		})
	})

	Context("when the members_removed bucket does not exist", func() {
		It("reports the member as not removed (no member ever removed)", func() {
			writeTombstoneDB(dataDir, false)

			removed, err := member.IsMemberRemoved(etcdutil.BackendDBPath(dataDir), uint64(0xabc))
			Expect(err).NotTo(HaveOccurred())
			Expect(removed).To(BeFalse())
		})
	})

	Context("when the boltdb backend file does not exist", func() {
		It("returns an error (fails closed)", func() {
			missing := filepath.Join(dataDir, "does-not-exist", "db")

			removed, err := member.IsMemberRemoved(missing, uint64(0xabc))
			Expect(err).To(HaveOccurred())
			Expect(removed).To(BeFalse())
		})
	})

	Context("when the boltdb backend file is corrupt", func() {
		It("returns an error instead of panicking", func() {
			dbPath := etcdutil.BackendDBPath(dataDir)
			Expect(os.MkdirAll(filepath.Dir(dbPath), 0700)).To(Succeed())
			// A non-boltdb file trips bbolt's page-header validation;
			// IsMemberRemoved must recover and return an error.
			Expect(os.WriteFile(dbPath, []byte("this is not a valid boltdb file"), 0600)).To(Succeed())

			removed, err := member.IsMemberRemoved(dbPath, uint64(0xabc))
			Expect(err).To(HaveOccurred())
			Expect(removed).To(BeFalse())
		})
	})
})
