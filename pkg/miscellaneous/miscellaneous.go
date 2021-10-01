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

package miscellaneous

import (
	"fmt"
	"net/url"
	"path/filepath"
	"sort"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/metrics"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/embed"
)

// GetLatestFullSnapshotAndDeltaSnapList returns the latest snapshot
func GetLatestFullSnapshotAndDeltaSnapList(store brtypes.SnapStore) (*brtypes.Snapshot, brtypes.SnapList, error) {
	var (
		fullSnapshot  *brtypes.Snapshot
		deltaSnapList brtypes.SnapList
	)
	snapList, err := store.List()
	if err != nil {
		return nil, nil, err
	}

	for index := len(snapList); index > 0; index-- {
		if snapList[index-1].IsChunk {
			continue
		}
		if snapList[index-1].Kind == brtypes.SnapshotKindFull {
			fullSnapshot = snapList[index-1]
			break
		}
		deltaSnapList = append(deltaSnapList, snapList[index-1])
	}

	sort.Sort(deltaSnapList) // ensures that the delta snapshot list is well formed
	metrics.SnapstoreLatestDeltasTotal.With(prometheus.Labels{}).Set(float64(len(deltaSnapList)))
	if len(deltaSnapList) == 0 {
		metrics.SnapstoreLatestDeltasRevisionsTotal.With(prometheus.Labels{}).Set(0)
	} else {
		revisionDiff := deltaSnapList[len(deltaSnapList)-1].LastRevision - deltaSnapList[0].StartRevision
		metrics.SnapstoreLatestDeltasRevisionsTotal.With(prometheus.Labels{}).Set(float64(revisionDiff))
	}
	return fullSnapshot, deltaSnapList, nil
}

type backup struct {
	FullSnapshot      *brtypes.Snapshot
	DeltaSnapshotList brtypes.SnapList
}

// GetFilteredBackups returns sorted by date (new -> old) SnapList. It will also filter the snapshots that should be included or not using the filter function.
// If the filter is nil it will return all snapshots. Also, maxBackups can be used to target only the last N snapshots (-1 = all).
func GetFilteredBackups(store brtypes.SnapStore, maxBackups int, filter func(snaps brtypes.Snapshot) bool) (brtypes.SnapList, error) {
	snapList, err := store.List()
	if err != nil {
		return nil, err
	}
	backups := getStructuredBackupList(snapList)
	sort.Slice(backups, func(i, j int) bool {
		return backups[i].FullSnapshot.CreatedOn.After(backups[j].FullSnapshot.CreatedOn)
	})

	list := brtypes.SnapList{}
	count := 0
	for _, backup := range backups {
		if maxBackups >= 0 && count == maxBackups {
			break
		}
		if filter != nil && !filter(*backup.FullSnapshot) {
			continue
		}
		list = append(list, backup.FullSnapshot)
		list = append(list, backup.DeltaSnapshotList...)
		count++
	}

	return list, nil
}

func getStructuredBackupList(snapList brtypes.SnapList) []backup {
	var (
		backups    []backup
		tempBackup = backup{}
	)

	for i := len(snapList) - 1; i >= 0; i-- {
		if snapList[i].IsChunk {
			continue
		}
		if snapList[i].Kind == brtypes.SnapshotKindFull {
			tempBackup.FullSnapshot = snapList[i]
			backups = append(backups, tempBackup)
			tempBackup = backup{}
			continue
		}
		tempBackup.DeltaSnapshotList = append(tempBackup.DeltaSnapshotList, snapList[i])
	}
	return backups
}

// StartEmbeddedEtcd starts the embedded etcd server.
func StartEmbeddedEtcd(logger *logrus.Entry, ro *brtypes.RestoreOptions) (*embed.Etcd, error) {
	cfg := embed.NewConfig()
	cfg.Dir = filepath.Join(ro.Config.RestoreDataDir)
	DefaultListenPeerURLs := "http://localhost:0"
	DefaultListenClientURLs := "http://localhost:0"
	DefaultInitialAdvertisePeerURLs := "http://localhost:0"
	DefaultAdvertiseClientURLs := "http://localhost:0"
	lpurl, _ := url.Parse(DefaultListenPeerURLs)
	apurl, _ := url.Parse(DefaultInitialAdvertisePeerURLs)
	lcurl, _ := url.Parse(DefaultListenClientURLs)
	acurl, _ := url.Parse(DefaultAdvertiseClientURLs)
	cfg.LPUrls = []url.URL{*lpurl}
	cfg.LCUrls = []url.URL{*lcurl}
	cfg.APUrls = []url.URL{*apurl}
	cfg.ACUrls = []url.URL{*acurl}
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
	cfg.QuotaBackendBytes = ro.Config.EmbeddedEtcdQuotaBytes
	cfg.MaxRequestBytes = ro.Config.MaxRequestBytes
	cfg.MaxTxnOps = ro.Config.MaxTxnOps
	cfg.AutoCompactionMode = ro.Config.AutoCompactionMode
	cfg.AutoCompactionRetention = ro.Config.AutoCompactionRetention
	cfg.Logger = "zap"
	e, err := embed.StartEtcd(cfg)
	if err != nil {
		return nil, err
	}
	select {
	case <-e.Server.ReadyNotify():
		logger.Infof("Embedded server is ready to listen client at: %s", e.Clients[0].Addr())
	case <-time.After(60 * time.Second):
		e.Server.Stop() // trigger a shutdown
		e.Close()
		return nil, fmt.Errorf("server took too long to start")
	}
	return e, nil
}
