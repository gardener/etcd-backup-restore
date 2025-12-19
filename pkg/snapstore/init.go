// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore

import (
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/gardener/etcd-backup-restore/pkg/wrappers"
)

// NewSnapstoreConfig returns the snapstore config.
func NewSnapstoreConfig() *brtypes.SnapstoreConfig {
	return &brtypes.SnapstoreConfig{
		MaxParallelChunkUploads: 5,
		MinChunkSize:            brtypes.MinChunkSize,
		TempDir:                 "/tmp",
	}
}

// NewSecondarySnapstoreConfig returns the secondary-snapstore config.
func NewSecondarySnapstoreConfig() *brtypes.SecondarySnapstoreConfig {
	return &brtypes.SecondarySnapstoreConfig{
		StoreConfig:       NewSnapstoreConfig(),
		SyncPeriod:        wrappers.Duration{Duration: brtypes.DefaultSecondaryBackupSyncPeriod},
		BackupSyncEnabled: false,
	}
}
