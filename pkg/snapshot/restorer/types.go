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

// Restorer is a struct for etcd data directory restorer
type Restorer struct {
	logger *logrus.Logger
	store  snapstore.SnapStore
}

// RestoreOptions hold all snapshot restore related fields
type RestoreOptions struct {
	ClusterURLs    types.URLsMap
	ClusterToken   string
	RestoreDataDir string
	PeerURLs       types.URLs
	SkipHashCheck  bool
	Name           string
	// Base full snapshot + delta snapshots to restore from
	BaseSnapshot  snapstore.Snapshot
	DeltaSnapList snapstore.SnapList
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

type eventsInfo struct {
	Events    []event
	Data      []byte
	Snap      snapstore.Snapshot
	SnapIndex int
}
