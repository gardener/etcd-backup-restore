// Copyright (c) 2021 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
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

package types

import (
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	flag "github.com/spf13/pflag"
)

const (
	// SnapstoreProviderLocal is constant for local disk storage provider.
	SnapstoreProviderLocal = "Local"
	// SnapstoreProviderS3 is constant for aws S3 storage provider.
	SnapstoreProviderS3 = "S3"
	// SnapstoreProviderABS is constant for azure blob storage provider.
	SnapstoreProviderABS = "ABS"
	// SnapstoreProviderGCS is constant for GCS object storage provider.
	SnapstoreProviderGCS = "GCS"
	// SnapstoreProviderSwift is constant for Swift object storage.
	SnapstoreProviderSwift = "Swift"
	// SnapstoreProviderOSS is constant for Alicloud OSS storage provider.
	SnapstoreProviderOSS = "OSS"
	// SnapstoreProviderECS is constant for Dell EMC ECS S3 storage provider.
	SnapstoreProviderECS = "ECS"
	// SnapstoreProviderOCS is constant for OpenShift Container Storage S3 storage provider.
	SnapstoreProviderOCS = "OCS"
	// SnapstoreProviderFakeFailed is constant for fake failed storage provider.
	SnapstoreProviderFakeFailed = "FAILED"

	// SnapshotKindFull is constant for full snapshot kind.
	SnapshotKindFull = "Full"
	// SnapshotKindDelta is constant for delta snapshot kind.
	SnapshotKindDelta = "Incr"
	// SnapshotKindChunk is constant for chunk snapshot kind.
	SnapshotKindChunk = "Chunk"

	// AzureBlobStorageHostName is the host name for azure blob storage service.
	AzureBlobStorageHostName = "blob.core.windows.net"

	backupFormatVersion = "v1"
)

// SnapStore is the interface to be implemented for different
// storage backend like local file system, S3, ABS, GCS, Swift, OSS, ECS etc.
// Only purpose of these implementation to provide CPI layer to
// access files.
type SnapStore interface {
	// Fetch should open reader for the snapshot file from store.
	Fetch(Snapshot) (io.ReadCloser, error)
	// List will list all snapshot files on store.
	List() (SnapList, error)
	// Save will write the snapshot to store.
	Save(Snapshot, io.ReadCloser) error
	// Delete should delete the snapshot file from store.
	Delete(Snapshot) error
}

// Snapshot structure represents the metadata of snapshot.s
type Snapshot struct {
	Kind              string    `json:"kind"` //incr:incremental,full:full
	StartRevision     int64     `json:"startRevision"`
	LastRevision      int64     `json:"lastRevision"` //latest revision on snapshot
	CreatedOn         time.Time `json:"createdOn"`
	SnapDir           string    `json:"snapDir"`
	SnapName          string    `json:"snapName"`
	IsChunk           bool      `json:"isChunk"`
	CompressionSuffix string    `json:"compressionSuffix"` // CompressionSuffix depends on compessionPolicy
}

// GenerateSnapshotName prepares the snapshot name from metadata
func (s *Snapshot) GenerateSnapshotName() {
	s.SnapName = fmt.Sprintf("%s-%08d-%08d-%d%s", s.Kind, s.StartRevision, s.LastRevision, s.CreatedOn.Unix(), s.CompressionSuffix)
}

// GenerateSnapshotDirectory prepares the snapshot directory name from metadata
func (s *Snapshot) GenerateSnapshotDirectory() {
	s.SnapDir = fmt.Sprintf("Backup-%d", s.CreatedOn.Unix())
}

// GetSnapshotDirectoryCreationTimeInUnix returns the creation time for snapshot directory.
func (s *Snapshot) GetSnapshotDirectoryCreationTimeInUnix() (int64, error) {
	tok := strings.TrimPrefix(s.SnapDir, "Backup-")
	return strconv.ParseInt(tok, 10, 64)
}

// SnapList is list of snapshots.
type SnapList []*Snapshot

// SnapList override sorting related function
func (s SnapList) Len() int      { return len(s) }
func (s SnapList) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s SnapList) Less(i, j int) bool {
	// Ignoring errors here, as we assume at this stage the error won't happen.
	iCreationTime, err := s[i].GetSnapshotDirectoryCreationTimeInUnix()
	if err != nil {
		logrus.Errorf("Failed to get snapshot directory creation time for snapshot: %s, with error: %v", path.Join(s[i].SnapDir, s[i].SnapName), err)
	}
	jCreationTime, err := s[j].GetSnapshotDirectoryCreationTimeInUnix()
	if err != nil {
		logrus.Errorf("Failed to get snapshot directory creation time for snapshot: %s, with error: %v", path.Join(s[j].SnapDir, s[j].SnapName), err)
	}
	if iCreationTime < jCreationTime {
		return true
	}
	if iCreationTime > jCreationTime {
		return false
	}
	if s[i].CreatedOn.Unix() == s[j].CreatedOn.Unix() {
		if !s[i].IsChunk && s[j].IsChunk {
			return true
		}
		if s[i].IsChunk && !s[j].IsChunk {
			return false
		}
		if !s[i].IsChunk && !s[j].IsChunk {
			return (s[i].StartRevision < s[j].StartRevision)
		}
		// If both are chunks, ordering doesn't matter.
		return true
	}
	return (s[i].CreatedOn.Unix() < s[j].CreatedOn.Unix())
}

// SnapstoreConfig defines the configuration to create snapshot store.
type SnapstoreConfig struct {
	// Provider indicated the cloud provider.
	Provider string `json:"provider,omitempty"`
	// Container holds the name of bucket or container to which snapshot will be stored.
	Container string `json:"container"`
	// Prefix holds the prefix or directory under StorageContainer under which snapshot will be stored.
	Prefix string `json:"prefix,omitempty"`
	// MaxParallelChunkUploads hold the maximum number of parallel chunk uploads allowed.
	MaxParallelChunkUploads uint `json:"maxParallelChunkUploads,omitempty"`
	// Temporary Directory
	TempDir string `json:"tempDir,omitempty"`
}

// AddFlags adds the flags to flagset.
func (c *SnapstoreConfig) AddFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.Provider, "storage-provider", c.Provider, "snapshot storage provider")
	fs.StringVar(&c.Container, "store-container", c.Container, "container which will be used as snapstore")
	fs.StringVar(&c.Prefix, "store-prefix", c.Prefix, "prefix or directory inside container under which snapstore is created")
	fs.UintVar(&c.MaxParallelChunkUploads, "max-parallel-chunk-uploads", c.MaxParallelChunkUploads, "maximum number of parallel chunk uploads allowed ")
	fs.StringVar(&c.TempDir, "snapstore-temp-directory", c.TempDir, "temporary directory for processing")
}

// Validate validates the config.
func (c *SnapstoreConfig) Validate() error {
	if c.MaxParallelChunkUploads <= 0 {
		return fmt.Errorf("max parallel chunk uploads should be greater than zero")
	}
	return nil
}

// Complete completes the config.
func (c *SnapstoreConfig) Complete() {
	c.Prefix = path.Join(c.Prefix, backupFormatVersion)
}
