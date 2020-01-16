// Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
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
	"context"
	"fmt"
	"io"
)

// FailedSnapStore is snapstore with fake failed object store as backend
type FailedSnapStore struct {
	SnapStore
}

// NewFailedSnapStore create new FailedSnapStore object.
func NewFailedSnapStore() *FailedSnapStore {
	return &FailedSnapStore{}
}

// Fetch should open reader for the snapshot file from store
func (f *FailedSnapStore) Fetch(ctx context.Context, snap Snapshot) (io.ReadCloser, error) {
	return nil, fmt.Errorf("failed to fetch snapshot %s", snap.SnapName)
}

// Save will write the snapshot to store
func (f *FailedSnapStore) Save(ctx context.Context, snap Snapshot, rc io.ReadCloser) error {
	return fmt.Errorf("failed to save snapshot %s", snap.SnapName)
}

// List will list the snapshots from store
func (f *FailedSnapStore) List(ctx context.Context) (SnapList, error) {
	var snapList SnapList
	return snapList, fmt.Errorf("failed to list the snapshots")
}

// Delete should delete the snapshot file from store
func (f *FailedSnapStore) Delete(ctx context.Context, snap Snapshot) error {
	return fmt.Errorf("failed to delete snapshot %s", snap.SnapName)
}
