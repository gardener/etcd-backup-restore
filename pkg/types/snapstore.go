// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package types

import (
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

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

	// AzureBlobStorageGlobalDomain is the default domain for azure blob storage service.
	AzureBlobStorageGlobalDomain = "blob.core.windows.net"

	// FinalSuffix is the suffix appended to the names of final snapshots.
	FinalSuffix = ".final"

	// ChunkDirSuffix is the suffix appended to the name of chunk snapshot folder when using fakegcs emulator for testing.
	// Refer to this github issue for more details: https://github.com/fsouza/fake-gcs-server/issues/1434
	ChunkDirSuffix = ".chunk"

	backupFormatVersion = "v2"

	// MinChunkSize is set to 5Mib since it is lower chunk size limit for AWS.
	MinChunkSize int64 = 5 * (1 << 20) //5 MiB

	// ExcludeSnapshotMetadataKey is the tag that is to be added on snapshots in the object store if they are not to be included in SnapStore's List output.
	// Note: applicable for storage provider: ABS, GCS and S3.
	ExcludeSnapshotMetadataKey = "x-etcd-snapshot-exclude"
)

var (
	// ErrSnapshotDeleteFailDueToImmutability is the error returned when the Delete call fails due to immutability
	ErrSnapshotDeleteFailDueToImmutability = fmt.Errorf("ErrSnapshotDeleteFailDueToImmutability")
)

// SnapStore is the interface to be implemented for different
// storage backend like local file system, S3, ABS, GCS, Swift, OSS, ECS etc.
// Only purpose of these implementation to provide CPI layer to
// access files.
type SnapStore interface {
	// Fetch should open reader for the snapshot file from store.
	Fetch(Snapshot) (io.ReadCloser, error)
	// List returns a sorted list (based on the last revision, ascending) of all snapshots in the store.
	// includeAll specifies whether to include all snapshots while listing, including those with exclude tags.
	// Snapshots with exclude tags are not listed unless includeAll is set to true.
	// Note: "includeAll" boolean is only applicable for storage provider: ABS and GCS.
	List(includeAll bool) (SnapList, error)
	// Save will write the snapshot to store.
	Save(Snapshot, io.ReadCloser) error
	// Delete should delete the snapshot file from store.
	Delete(Snapshot) error
}

// Snapshot structure represents the metadata of snapshot.
type Snapshot struct {
	CreatedOn              time.Time `json:"createdOn"`
	ImmutabilityExpiryTime time.Time `json:"immutabilityExpriyTime"`
	VersionID              *string   `json:"versionID"` // It is used only for AWS S3 object lock immutability.
	Kind                   string    `json:"kind"`      // incr:incremental, full:full
	SnapDir                string    `json:"snapDir"`
	SnapName               string    `json:"snapName"`
	Prefix                 string    `json:"prefix"`            // Points to correct prefix of a snapshot in snapstore (Required for Backward Compatibility)
	CompressionSuffix      string    `json:"compressionSuffix"` // CompressionSuffix depends on compression policy
	StartRevision          int64     `json:"startRevision"`
	LastRevision           int64     `json:"lastRevision"` // latest revision of snapshot
	IsChunk                bool      `json:"isChunk"`
	IsFinal                bool      `json:"isFinal"`
}

// IsDeletable determines if the snapshot can be deleted.
// It checks if the immutability expiry time is set and whether the current time is after the immutability expiry time.
func (s *Snapshot) IsDeletable() bool {
	// Check if ImmutabilityExpiryTime is the zero value of time.Time, which means it is not set.
	// If ImmutabilityExpiryTime is not set, assume the snapshot can be deleted.
	if s.ImmutabilityExpiryTime.IsZero() {
		return true
	}
	// Otherwise, check if the current time is after the immutability expiry time.
	return time.Now().After(s.ImmutabilityExpiryTime)
}

// GenerateSnapshotName prepares the snapshot name from metadata
func (s *Snapshot) GenerateSnapshotName() {
	s.SnapName = fmt.Sprintf("%s-%08d-%08d-%d%s%s", s.Kind, s.StartRevision, s.LastRevision, s.CreatedOn.Unix(), s.CompressionSuffix, s.finalSuffix())
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

// SetFinal sets the IsFinal field of this snapshot to the given value.
func (s *Snapshot) SetFinal(final bool) {
	s.IsFinal = final
	if s.IsFinal {
		if !strings.HasSuffix(s.SnapName, FinalSuffix) {
			s.SnapName += FinalSuffix
		}
	} else {
		s.SnapName = strings.TrimSuffix(s.SnapName, FinalSuffix)
	}
}

// finalSuffix returns the final suffix of this snapshot, either ".final" or an empty string
func (s *Snapshot) finalSuffix() string {
	if s.IsFinal {
		return FinalSuffix
	}
	return ""
}

// SnapList is list of snapshots.
type SnapList []*Snapshot

// SnapList override sorting related function
func (s SnapList) Len() int      { return len(s) }
func (s SnapList) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s SnapList) Less(i, j int) bool {
	// Ignoring errors here, as we assume at this stage the error won't happen.
	iLastRevision := s[i].LastRevision
	jLastRevision := s[j].LastRevision

	if iLastRevision < jLastRevision {
		return true
	}

	if iLastRevision == jLastRevision {
		if !s[i].IsChunk && s[j].IsChunk {
			return true
		}
		if s[i].IsChunk && !s[j].IsChunk {
			return false
		}
		if !s[i].IsChunk && !s[j].IsChunk {
			return (s[i].CreatedOn.Unix() < s[j].CreatedOn.Unix())
		}
		// If both are chunks, ordering doesn't matter.
		return true
	}

	return false
}

// SnapstoreConfig defines the configuration to create snapshot store.
type SnapstoreConfig struct {
	SecondaryContainer               string        `json:"secondaryContainer,omitempty"`
	TempDir                          string        `json:"tempDir,omitempty"`
	SecondaryProvider                string        `json:"secondaryProvider,omitempty"`
	SecondaryPrefix                  string        `json:"secondaryPrefix,omitempty"`
	Provider                         string        `json:"provider,omitempty"`
	Container                        string        `json:"container"`
	Prefix                           string        `json:"prefix,omitempty"`
	MinChunkSize                     int64         `json:"minChunkSize,omitempty"`
	BackupSyncRetryBackoff           time.Duration `json:"backupSyncRetryBackoff,omitempty"`
	BackupSyncPeriod                 time.Duration `json:"backupSyncPeriod,omitempty"`
	SecondaryMinChunkSize            int64         `json:"secondaryMinChunkSize,omitempty"`
	MaxParallelChunkUploads          uint          `json:"maxParallelChunkUploads,omitempty"`
	SecondaryMaxParallelChunkUploads uint          `json:"secondaryMaxParallelChunkUploads,omitempty"`
	BackupSyncMaxRetries             int           `json:"backupSyncMaxRetries,omitempty"`
	BackupSyncConcurrentCopies       int           `json:"backupSyncConcurrentCopies,omitempty"`
	IsSource                         bool          `json:"isSource,omitempty"`
	IsEmulatorEnabled                bool          `json:"isEmulatorEnabled,omitempty"`
	IsSecondary                      bool          `json:"isSecondary,omitempty"`
	BackupSyncEnabled                bool          `json:"backupSyncEnabled,omitempty"`
}

// AddFlags adds the flags to flagset.
func (c *SnapstoreConfig) AddFlags(fs *flag.FlagSet) {
	c.addFlags(fs, "")
}

// AddSourceFlags adds the flags to flagset using `source-` prefix for all parameters.
func (c *SnapstoreConfig) AddSourceFlags(fs *flag.FlagSet) {
	c.addFlags(fs, "source-")
}

func (c *SnapstoreConfig) addFlags(fs *flag.FlagSet, parameterPrefix string) {
	fs.StringVar(&c.Provider, parameterPrefix+"storage-provider", c.Provider, "snapshot storage provider")
	fs.StringVar(&c.Container, parameterPrefix+"store-container", c.Container, "container which will be used as snapstore")
	fs.StringVar(&c.Prefix, parameterPrefix+"store-prefix", c.Prefix, "prefix or directory inside container under which snapstore is created")
	fs.UintVar(&c.MaxParallelChunkUploads, parameterPrefix+"max-parallel-chunk-uploads", c.MaxParallelChunkUploads, "maximum number of parallel chunk uploads allowed")
	fs.Int64Var(&c.MinChunkSize, parameterPrefix+"min-chunk-size", c.MinChunkSize, "Minimum size for multipart chunk upload")
	fs.StringVar(&c.TempDir, parameterPrefix+"snapstore-temp-directory", c.TempDir, "temporary directory for processing")

	// Secondary endpoint parameters
	fs.StringVar(&c.SecondaryProvider, parameterPrefix+"secondary-storage-provider", c.SecondaryProvider, "secondary snapshot storage provider")
	fs.StringVar(&c.SecondaryContainer, parameterPrefix+"secondary-store-container", c.SecondaryContainer, "secondary container which will be used as snapstore")
	fs.StringVar(&c.SecondaryPrefix, parameterPrefix+"secondary-store-prefix", c.SecondaryPrefix, "secondary prefix or directory inside container under which snapstore is created")
	fs.UintVar(&c.SecondaryMaxParallelChunkUploads, parameterPrefix+"secondary-max-parallel-chunk-uploads", c.SecondaryMaxParallelChunkUploads, "secondary maximum number of parallel chunk uploads allowed")
	fs.Int64Var(&c.SecondaryMinChunkSize, parameterPrefix+"secondary-min-chunk-size", c.SecondaryMinChunkSize, "secondary minimum size for multipart chunk upload")

	// Backup sync configuration flags
	fs.DurationVar(&c.BackupSyncPeriod, parameterPrefix+"backup-sync-period", 3*time.Minute, "period for checking and copying new backups to secondary endpoint")
	fs.IntVar(&c.BackupSyncMaxRetries, parameterPrefix+"backup-sync-max-retries", 5, "maximum number of retry attempts for failed backup copies")
	fs.DurationVar(&c.BackupSyncRetryBackoff, parameterPrefix+"backup-sync-retry-backoff", 10*time.Second, "backoff duration between backup copy retry attempts")
	fs.IntVar(&c.BackupSyncConcurrentCopies, parameterPrefix+"backup-sync-concurrent-copies", 10, "maximum number of concurrent backup copy operations")
	fs.BoolVar(&c.BackupSyncEnabled, parameterPrefix+"backup-sync-enabled", false, "enable background backup synchronization to secondary endpoint")
}

// Validate validates the config.
func (c *SnapstoreConfig) Validate() error {
	if c.MaxParallelChunkUploads <= 0 {
		return fmt.Errorf("max parallel chunk uploads should be greater than zero")
	}
	if c.MinChunkSize < MinChunkSize {
		return fmt.Errorf("min chunk size for multi-part chunk upload should be greater than or equal to 5 MiB")
	}
	return nil
}

// Complete completes the config.
func (c *SnapstoreConfig) Complete() {
	c.Prefix = path.Join(c.Prefix, backupFormatVersion)

	if c.TempDir == "" {
		c.TempDir = "/tmp"
	}
}

// MergeWith completes the config based on other config
func (c *SnapstoreConfig) MergeWith(other *SnapstoreConfig) {
	if c.Provider == "" {
		c.Provider = other.Provider
	}
	if c.Prefix == "" {
		c.Prefix = other.Prefix
	} else {
		c.Prefix = path.Join(c.Prefix, backupFormatVersion)
	}
	if c.TempDir == "" {
		c.TempDir = other.TempDir
	}
}

// HasSecondaryEndpoint returns true if secondary endpoint is configured.
func (c *SnapstoreConfig) HasSecondaryEndpoint() bool {
	// Check if SecondaryContainer is explicitly set
	if c.SecondaryContainer != "" {
		return true
	}
	// Also check environment variable as fallback (similar to primary container logic)
	return os.Getenv("SECONDARY_STORAGE_CONTAINER") != ""
}

// GetSecondaryConfig creates a SnapstoreConfig for the secondary endpoint.
func (c *SnapstoreConfig) GetSecondaryConfig() *SnapstoreConfig {
	if !c.HasSecondaryEndpoint() {
		return nil
	}

	secondaryContainer := c.SecondaryContainer
	// If SecondaryContainer is empty, check environment variable (similar to primary container logic)
	if secondaryContainer == "" {
		secondaryContainer = os.Getenv("SECONDARY_STORAGE_CONTAINER")
	}

	secondaryConfig := &SnapstoreConfig{
		Provider:                c.SecondaryProvider,
		Container:               secondaryContainer,
		Prefix:                  path.Join(c.SecondaryPrefix, backupFormatVersion),
		TempDir:                 c.TempDir, // Share temp directory
		MaxParallelChunkUploads: c.SecondaryMaxParallelChunkUploads,
		MinChunkSize:            c.SecondaryMinChunkSize,
		IsSource:                false,
		IsSecondary:             true, // Mark as secondary endpoint
		IsEmulatorEnabled:       c.IsEmulatorEnabled,
	}

	// Set defaults if not specified
	if secondaryConfig.Provider == "" {
		secondaryConfig.Provider = c.Provider // Default to same provider as primary
	}
	if secondaryConfig.MaxParallelChunkUploads == 0 {
		secondaryConfig.MaxParallelChunkUploads = c.MaxParallelChunkUploads
	}
	if secondaryConfig.MinChunkSize == 0 {
		secondaryConfig.MinChunkSize = c.MinChunkSize
	}

	return secondaryConfig
}

// OperationResult represents the result of an operation on a specific endpoint.
type OperationResult struct {
	Error    error  `json:"error,omitempty"`
	Endpoint string `json:"endpoint"`
	Success  bool   `json:"success"`
}

// DualOperationResult represents the results of an operation on both endpoints.
type DualOperationResult struct {
	// Primary holds the result for the primary endpoint.
	Primary OperationResult `json:"primary"`
	// Secondary holds the result for the secondary endpoint.
	Secondary OperationResult `json:"secondary"`
	// OverallSuccess indicates if at least one endpoint operation succeeded.
	OverallSuccess bool `json:"overallSuccess"`
}
