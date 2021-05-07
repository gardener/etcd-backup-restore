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

package snapstore

import "time"

const (
	// minChunkSize is set to 5Mib since it is lower chunk size limit for AWS.
	minChunkSize int64 = 5 * (1 << 20) //5 MiB

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
