// Copyright © 2018 The Gardener Authors.
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

package snapstore

import (
	"io"
	"time"
)

// SnapStore is the interface to be implemented for different
// storage backend like local file system, S3, ABS, GCS, Swift etc.
// Only purpose of these implementation to provide CPI layer to
// access files.
type SnapStore interface {
	// Fetch should open reader for the snapshot file from store
	Fetch(Snapshot) (io.ReadCloser, error)
	// List will list all snapshot files on store
	List() (SnapList, error)
	// Save will write the snapshot to store
	Save(Snapshot, io.Reader) error
	// Delete should delete the snapshot file from store
	Delete(Snapshot) error
	// Size returns the size of snapshot
	Size(Snapshot) (int64, error)
	// GetLates resturns the latet snapshot
	GetLatest() (*Snapshot, error)
}

// SnapStoreImpl is global structure with implmentation for common implementation of
// some of the API's defined
type SnapStoreImpl struct {
	SnapStore
}

// GetLatest returns the latest snapshot in snapstore
func (s *SnapStoreImpl) GetLatest() (*Snapshot, error) {
	snapList, err := s.List()
	if err != nil {
		return nil, err
	}
	if snapList.Len() == 0 {
		return nil, nil
	}
	return snapList[snapList.Len()-1], nil
}

const (
	// SnapstoreProviderLocal is constant for local disk storage provider
	SnapstoreProviderLocal = "Local"
	// SnapstoreProviderS3 is constant for aws S3 storage provider
	SnapstoreProviderS3 = "S3"

	// SnapshotKindFull is constant for full snapshot kind
	SnapshotKindFull = "Full"
)

// Snapshot structure represents the metadata of snapshot
type Snapshot struct {
	Kind          string //incr:incremental,full:full
	StartRevision int64
	LastRevision  int64 //latest revision on snapshot
	CreatedOn     time.Time
	SnapPath      string
}

// SnapList is list of snapshots
type SnapList []*Snapshot
