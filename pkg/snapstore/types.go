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
)

// SnapStore is the interface to be implemented for different
// storage backend like local file system, S3, ABS, GCS, Swift etc.
// Only purpose of these implementation to provide CPI layer to
// access files.
type SnapStore interface {
	// Fetch should open reader for the snapshot file from store
	Fetch(string) (io.ReadCloser, error)
	// List will list all snapshot files on store
	List() ([]string, error)
	// Save will write the snapshot to store
	Save(string, io.Reader) error
	// Delete should delete the snapshot file from store
	Delete(string) error
	// Size returns the size of snapshot
	Size(string) (int64, error)
}

const (
	// SnapstoreProviderLocal is constant for local disk storage provider
	SnapstoreProviderLocal = "Local"
	// SnapstoreProviderS3 is constant for aws S3 storage provider
	SnapstoreProviderS3 = "S3"
)
