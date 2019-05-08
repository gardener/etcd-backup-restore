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

package snapshotter

import (
	"context"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	"github.com/robfig/cron"
	"github.com/sirupsen/logrus"
)

const (
	// GarbageCollectionPolicyExponential defines the exponential policy for garbage collecting old backups
	GarbageCollectionPolicyExponential = "Exponential"
	// GarbageCollectionPolicyLimitBased defines the limit based policy for garbage collecting old backups
	GarbageCollectionPolicyLimitBased = "LimitBased"
	// DefaultMaxBackups is default number of maximum backups for limit based garbage collection policy.
	DefaultMaxBackups = 7

	// SnapshotterInactive is set when the snapshotter has not started taking snapshots.
	SnapshotterInactive State = 0
	// SnapshotterActive is set when the snapshotter has started taking snapshots.
	SnapshotterActive State = 1
	// DefaultDeltaSnapMemoryLimit is default memory limit for delta snapshots.
	DefaultDeltaSnapMemoryLimit = 10 * 1024 * 1024 //10Mib
	// DefaultDeltaSnapshotIntervalSeconds is the default interval for delta snapshots in seconds.
	DefaultDeltaSnapshotIntervalSeconds = 20
)

var emptyStruct struct{}

// State denotes the state the snapshotter would be in.
type State int

// Snapshotter is a struct for etcd snapshot taker
type Snapshotter struct {
	logger             *logrus.Logger
	prevSnapshot       *snapstore.Snapshot
	PrevFullSnapshot   *snapstore.Snapshot
	config             *Config
	fullSnapshotCh     chan struct{}
	fullSnapshotTimer  *time.Timer
	deltaSnapshotTimer *time.Timer
	events             []byte
	watchCh            clientv3.WatchChan
	etcdClient         *clientv3.Client
	cancelWatch        context.CancelFunc
	SsrStateMutex      *sync.Mutex
	SsrState           State
	lastEventRevision  int64
}

// Config stores the configuration parameters for the snapshotter.
type Config struct {
	schedule                       cron.Schedule
	store                          snapstore.SnapStore
	maxBackups                     int
	deltaSnapshotIntervalSeconds   int
	deltaSnapshotMemoryLimit       int
	etcdConnectionTimeout          time.Duration
	garbageCollectionPeriodSeconds time.Duration
	garbageCollectionPolicy        string
	tlsConfig                      *etcdutil.TLSConfig
}

// event is wrapper over etcd event to keep track of time of event
type event struct {
	EtcdEvent *clientv3.Event `json:"etcdEvent"`
	Time      time.Time       `json:"time"`
}
