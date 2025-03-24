// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore

import (
	"fmt"
	"io"

	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
)

// FailedSnapStore is snapstore with fake failed object store as backend
type FailedSnapStore struct {
	brtypes.SnapStore
}

// NewFailedSnapStore create new FailedSnapStore object.
func NewFailedSnapStore() *FailedSnapStore {
	return &FailedSnapStore{}
}

// Fetch should open reader for the snapshot file from store
func (f *FailedSnapStore) Fetch(snap brtypes.Snapshot) (io.ReadCloser, error) {
	return nil, fmt.Errorf("failed to fetch snapshot %s", snap.SnapName)
}

// Save will write the snapshot to store
func (f *FailedSnapStore) Save(snap brtypes.Snapshot, _ io.ReadCloser) error {
	return fmt.Errorf("failed to save snapshot %s", snap.SnapName)
}

// List will list the snapshots from store
func (f *FailedSnapStore) List(_ bool) (brtypes.SnapList, error) {
	var snapList brtypes.SnapList
	return snapList, fmt.Errorf("failed to list the snapshots")
}

// Delete should delete the snapshot file from store
func (f *FailedSnapStore) Delete(snap brtypes.Snapshot) error {
	return fmt.Errorf("failed to delete snapshot %s", snap.SnapName)
}
