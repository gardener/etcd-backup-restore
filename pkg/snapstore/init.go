// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore

import (
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
)

// NewSnapstoreConfig returns the snapstore config.
func NewSnapstoreConfig() *brtypes.SnapstoreConfig {
	return &brtypes.SnapstoreConfig{
		MaxParallelChunkUploads: 5,
		MinChunkSize:            brtypes.MinChunkSize,
		TempDir:                 "/tmp",
	}
}
