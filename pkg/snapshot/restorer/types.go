// Copyright (c) 2018 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
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

package restorer

import (
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/pkg/types"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	"github.com/sirupsen/logrus"
)

const (
	tmpDir                  = "/tmp"
	tmpEventsDataFilePrefix = "etcd-restore-"

	defaultName                     = "default"
	defaultInitialAdvertisePeerURLs = "http://localhost:2380"
	defaultInitialClusterToken      = "etcd-cluster"
	defaultMaxFetchers              = 6
	defaultEmbeddedEtcdQuotaBytes   = 8 * 1024 * 1024 * 1024 //8Gib
)

// Restorer is a struct for etcd data directory restorer
type Restorer struct {
	logger *logrus.Entry
	store  snapstore.SnapStore
}

// RestoreOptions hold all snapshot restore related fields
type RestoreOptions struct {
	Config      *RestorationConfig
	ClusterURLs types.URLsMap
	PeerURLs    types.URLs
	// Base full snapshot + delta snapshots to restore from
	BaseSnapshot  snapstore.Snapshot
	DeltaSnapList snapstore.SnapList
}

// RestorationConfig holds the restoration configuration.
type RestorationConfig struct {
	InitialCluster           string   `json:"initialCluster"`
	InitialClusterToken      string   `json:"initialClusterToken,omitempty"`
	RestoreDataDir           string   `json:"restoreDataDir,omitempty"`
	InitialAdvertisePeerURLs []string `json:"initialAdvertisePeerURLs"`
	Name                     string   `json:"name"`
	SkipHashCheck            bool     `json:"skipHashCheck,omitempty"`
	MaxFetchers              uint     `json:"maxFetchers,omitempty"`
	EmbeddedEtcdQuotaBytes   int64    `json:"embeddedEtcdQuotaBytes,omitempty"`
}

type initIndex int

func (i *initIndex) ConsistentIndex() uint64 {
	return uint64(*i)
}

// event is wrapper over etcd event to keep track of time of event
type event struct {
	EtcdEvent *clientv3.Event `json:"etcdEvent"`
	Time      time.Time       `json:"time"`
}

type fetcherInfo struct {
	Snapshot  snapstore.Snapshot
	SnapIndex int
}

type applierInfo struct {
	EventsFilePath string
	SnapIndex      int
}
