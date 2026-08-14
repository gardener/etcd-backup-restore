// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package member

import (
	"errors"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/server/v3/mvcc/buckets"
)

var (
	// ErrMemberPermanentlyRemoved is returned when the local member ID is found
	// in etcd's members_removed tombstone bucket.
	ErrMemberPermanentlyRemoved = errors.New("member was permanently removed from the cluster; refusing to re-add as learner")

	// ErrMembershipCheckFailed is returned when the tombstone could not be
	// inspected. Fails closed on this error.
	ErrMembershipCheckFailed = errors.New("failed to inspect local membership tombstone")
)

const antiRejoinBoltDBTimeout = 10 * time.Second

// IsMemberRemoved reports whether the given member ID is in the members_removed
// bucket of the boltdb backend. On failure the caller (WasPermanentlyRemoved)
// returns ErrMembershipCheckFailed and the process is expected to restart via
// the crash-back-off loop rather than retrying here.
func IsMemberRemoved(dbPath string, memberID uint64) (removed bool, retErr error) {
	// Recover from bolt panics on corrupt db files, the same way verifyDB and
	// getLatestEtcdRevision do, so a hardware-corrupted backend returns an error
	// instead of crashing the process.
	defer func() {
		if r := recover(); r != nil {
			removed = false
			retErr = fmt.Errorf("bolt panic while reading %q: %v", dbPath, r)
		}
	}()

	db, err := bolt.Open(dbPath, 0400, &bolt.Options{Timeout: antiRejoinBoltDBTimeout, ReadOnly: true})
	if err != nil {
		return false, fmt.Errorf("unable to open boltdb backend %q read-only: %w", dbPath, err)
	}
	defer func() {
		if cerr := db.Close(); cerr != nil && retErr == nil {
			retErr = fmt.Errorf("failed to close boltdb backend %q: %w", dbPath, cerr)
		}
	}()

	memberKey := []byte(types.ID(memberID).String())
	if err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(buckets.MembersRemoved.Name())
		if b == nil {
			// No tombstone bucket means no member has ever been removed.
			return nil
		}
		removed = b.Get(memberKey) != nil
		return nil
	}); err != nil {
		return false, fmt.Errorf("unable to read %q bucket from boltdb backend %q: %w", buckets.MembersRemoved.Name(), dbPath, err)
	}
	return removed, nil
}
