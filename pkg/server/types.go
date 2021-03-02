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

package server

import (
	"github.com/gardener/etcd-backup-restore/pkg/compressor"
	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
)

const (
	defaultServerPort              = 8080
	defaultDefragmentationSchedule = "0 0 */3 * *"
)

// BackupRestoreComponentConfig holds the component configuration.
type BackupRestoreComponentConfig struct {
	EtcdConnectionConfig    *etcdutil.EtcdConnectionConfig `json:"etcdConnectionConfig,omitempty"`
	ServerConfig            *HTTPServerConfig              `json:"serverConfig,omitempty"`
	SnapshotterConfig       *brtypes.SnapshotterConfig     `json:"snapshotterConfig,omitempty"`
	SnapstoreConfig         *brtypes.SnapstoreConfig       `json:"snapstoreConfig,omitempty"`
	CompressionConfig       *compressor.CompressionConfig  `json:"compressionConfig,omitempty"`
	RestorationConfig       *brtypes.RestorationConfig     `json:"restorationConfig,omitempty"`
	DefragmentationSchedule string                         `json:"defragmentationSchedule"`
}

// latestSnapshotMetadata holds snapshot details of latest full and delta snapshots
type latestSnapshotMetadataResponse struct {
	FullSnapshot   *brtypes.Snapshot `json:"fullSnapshot"`
	DeltaSnapshots brtypes.SnapList  `json:"deltaSnapshots"`
}
