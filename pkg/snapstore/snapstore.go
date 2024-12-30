// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore

import "time"

const (
	// chunkUploadTimeout is timeout for uploading chunk.
	chunkUploadTimeout = 180 * time.Second
	// providerConnectionTimeout is timeout for connection/short queries to cloud provider.
	providerConnectionTimeout = 30 * time.Second
	// downloadTimeout is timeout for downloading chunk.
	downloadTimeout = 5 * time.Minute

	tmpBackupFilePrefix = "etcd-backup-"

	// maxRetryAttempts indicates the number of attempts to be retried in case of failure to upload chunk.
	maxRetryAttempts = 5

	backupVersionV1 = "v1"
	backupVersionV2 = "v2"
)

type chunk struct {
	offset  int64
	size    int64
	attempt uint
	id      int
}
type chunkUploadResult struct {
	err   error
	chunk *chunk
}
