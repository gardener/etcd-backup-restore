// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"github.com/gardener/etcd-backup-restore/pkg/compressor"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
)

const (
	defaultServerPort              = 8080
	defaultDefragmentationSchedule = "0 0 */3 * *"
	// to enable backup-restore to use etcd-wrapper related functionality.
	usageOfEtcdWrapperEnabled = false
)

// BackupRestoreComponentConfig holds the component configuration.
type BackupRestoreComponentConfig struct {
	EtcdConnectionConfig     *brtypes.EtcdConnectionConfig     `json:"etcdConnectionConfig,omitempty"`
	ServerConfig             *HTTPServerConfig                 `json:"serverConfig,omitempty"`
	SnapshotterConfig        *brtypes.SnapshotterConfig        `json:"snapshotterConfig,omitempty"`
	SnapstoreConfig          *brtypes.SnapstoreConfig          `json:"snapstoreConfig,omitempty"`
	SecondarySnapstoreConfig *brtypes.SnapstoreConfig          `json:"secondarySnapstoreConfig,omitempty"`
	CompressionConfig        *compressor.CompressionConfig     `json:"compressionConfig,omitempty"`
	RestorationConfig        *brtypes.RestorationConfig        `json:"restorationConfig,omitempty"`
	HealthConfig             *brtypes.HealthConfig             `json:"healthConfig,omitempty"`
	LeaderElectionConfig     *brtypes.Config                   `json:"leaderElectionConfig,omitempty"`
	ExponentialBackoffConfig *brtypes.ExponentialBackoffConfig `json:"exponentialBackoffConfig,omitempty"`
	DefragmentationSchedule  string                            `json:"defragmentationSchedule"`
	BackupSyncEnabled        bool                              `json:"backupSyncEnabled,omitempty"`
	UseEtcdWrapper           bool                              `json:"useEtcdWrapper,omitempty"`
}

// latestSnapshotMetadata holds snapshot details of latest full and delta snapshots
type latestSnapshotMetadataResponse struct {
	FullSnapshot   *brtypes.Snapshot `json:"fullSnapshot"`
	DeltaSnapshots brtypes.SnapList  `json:"deltaSnapshots"`
}
