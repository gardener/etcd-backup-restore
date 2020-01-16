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
	"math"
	"path"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/metrics"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

// RunGarbageCollector basically consider the older backups as garbage and deletes it
func (ssr *Snapshotter) RunGarbageCollector(ctx context.Context) {
	if ssr.config.GarbageCollectionPeriod.Duration <= time.Second {
		ssr.logger.Infof("GC: Not running garbage collector since GarbageCollectionPeriod [%s] set to less than 1 second.", ssr.config.GarbageCollectionPeriod)
		return
	}

	for {
		select {
		case <-ctx.Done():
			ssr.logger.Info("GC: Stop signal received. Closing garbage collector.")
			return
		case <-time.After(ssr.config.GarbageCollectionPeriod.Duration):
			total := 0
			ssr.logger.Info("GC: Executing garbage collection...")
			snapList, err := ssr.store.List(ctx)
			if err != nil {
				ssr.logger.Warnf("GC: Failed to list snapshots: %v", err)
				continue
			}

			snapStreamIndexList := getSnapStreamIndexList(snapList)

			switch ssr.config.GarbageCollectionPolicy {
			case GarbageCollectionPolicyExponential:
				// Overall policy:
				// Delete delta snapshots in all snapStream but the latest one.
				// Keep only the last 24 hourly backups and of all other backups only the last backup in a day.
				// Keep only the last 7 daily backups and of all other backups only the last backup in a week.
				// Keep only the last 4 weekly backups.
				var (
					deleteSnap bool
					threshold  int
					now        = time.Now().UTC()
					// Round off current time to EOD
					eod          = now.Truncate(24 * time.Hour).Add(23 * time.Hour).Add(59 * time.Minute).Add(59 * time.Second)
					trackingWeek = 0
				)
				// Here we start processing from second last snapstream, because we want to keep last snapstream
				// including delta snapshots in it.
				for snapStreamIndex := len(snapStreamIndexList) - 1; snapStreamIndex > 0; snapStreamIndex-- {
					snap := snapList[snapStreamIndexList[snapStreamIndex]]
					nextSnap := snapList[snapStreamIndexList[snapStreamIndex-1]]

					// garbage collect delta snapshots.
					deletedSnap, err := ssr.garbageCollectDeltaSnapshots(ctx, snapList[snapStreamIndexList[snapStreamIndex-1]:snapStreamIndexList[snapStreamIndex]])
					total += deletedSnap
					if err != nil {
						continue
					}

					delta := eod.Sub(nextSnap.CreatedOn)
					// Depending on how old the nextSnap is, decide what is the criteria of saving it (1 per hour or day or week)
					switch {
					case delta < time.Duration(24)*time.Hour:
						// Snapshot of current day
						if nextSnap.CreatedOn.Hour() == now.Hour() {
							// Save snapshot of current hour
							threshold = 0
							break
						}
						threshold = 1
					case delta < time.Duration(8*24)*time.Hour:
						// Snapshot of week ending with previous day
						threshold = 24
					case delta < time.Duration(5*7*24)*time.Hour:
						// Snapshot of month ending 8 days back (i.e., lesser than 5 weeks old)
						if trackingWeek == 0 {
							// As The week ends previous day, to keep track of change in week
							// we shift eod to previous day's EOD when start tracking week
							eod = eod.Add(-24 * time.Hour)
							trackingWeek = 1
						}
						threshold = 24 * 7
					default:
						// Delete snapshots older than 4 weeks
						threshold = math.MaxInt32
					}

					// Were snap and nextSnap created in different hour windows
					hourChange := int(eod.Sub(nextSnap.CreatedOn).Hours()) - int(eod.Sub(snap.CreatedOn).Hours())
					// Were snap and nextSnap created in different day windows
					dayChange := int(eod.Sub(nextSnap.CreatedOn).Hours()/24) - int(eod.Sub(snap.CreatedOn).Hours()/24)
					// Were snap and nextSnap created in different week windows
					weekChange := int(eod.Sub(nextSnap.CreatedOn).Hours()/(24*7)) - int(eod.Sub(snap.CreatedOn).Hours()/(24*7))

					if threshold == 0 || hourChange/threshold != 0 || dayChange*24/threshold != 0 || weekChange*24*7/threshold != 0 {
						// The change in parameter was more than the threshold, so don't delete the snapshot
						deleteSnap = false
					} else {
						// The change in parameter was less than the threshold, so delete the snapshot
						deleteSnap = true
					}

					if deleteSnap {
						ssr.logger.Infof("GC: Deleting old full snapshot: %s %v", nextSnap.CreatedOn.UTC(), deleteSnap)
						if err := ssr.store.Delete(ctx, *nextSnap); err != nil {
							ssr.logger.Warnf("GC: Failed to delete snapshot %s: %v", path.Join(nextSnap.SnapDir, nextSnap.SnapName), err)
							metrics.GCSnapshotCounter.With(prometheus.Labels{metrics.LabelKind: snapstore.SnapshotKindFull, metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Inc()
							continue
						}
						metrics.GCSnapshotCounter.With(prometheus.Labels{metrics.LabelKind: snapstore.SnapshotKindFull, metrics.LabelSucceeded: metrics.ValueSucceededTrue}).Inc()
						total++
						garbageCollectChunks(ctx, ssr.store, snapList, snapStreamIndexList[snapStreamIndex-1]+1, snapStreamIndexList[snapStreamIndex])
					}
				}

			case GarbageCollectionPolicyLimitBased:
				// Delete delta snapshots in all snapStream but the latest one.
				// Delete all snapshots beyond limit set by ssr.maxBackups.
				for snapStreamIndex := 0; snapStreamIndex < len(snapStreamIndexList)-1; snapStreamIndex++ {
					deletedSnap, err := ssr.garbageCollectDeltaSnapshots(ctx, snapList[snapStreamIndexList[snapStreamIndex]:snapStreamIndexList[snapStreamIndex+1]])
					total += deletedSnap
					if err != nil {
						continue
					}
					if snapStreamIndex < len(snapStreamIndexList)-int(ssr.config.MaxBackups) {
						snap := snapList[snapStreamIndexList[snapStreamIndex]]
						snapPath := path.Join(snap.SnapDir, snap.SnapName)
						ssr.logger.Infof("GC: Deleting old full snapshot: %s", snapPath)
						if err := ssr.store.Delete(ctx, *snap); err != nil {
							ssr.logger.Warnf("GC: Failed to delete snapshot %s: %v", snapPath, err)
							metrics.GCSnapshotCounter.With(prometheus.Labels{metrics.LabelKind: snapstore.SnapshotKindFull, metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Inc()
							continue
						}
						metrics.GCSnapshotCounter.With(prometheus.Labels{metrics.LabelKind: snapstore.SnapshotKindFull, metrics.LabelSucceeded: metrics.ValueSucceededTrue}).Inc()
						total++
						garbageCollectChunks(ctx, ssr.store, snapList, snapStreamIndexList[snapStreamIndex]+1, snapStreamIndexList[snapStreamIndex+1])
					}
				}
			}
			ssr.logger.Infof("GC: Total number garbage collected snapshots: %d", total)
		}
	}
}

// getSnapStreamIndexList lists the index of snapStreams in snapList which consist of collection of snapStream.
// snapStream indicates the list of snapshot, where first snapshot is base/full snapshot followed by
// list of incremental snapshots based on it.
func getSnapStreamIndexList(snapList snapstore.SnapList) []int {
	// At this stage, we assume the snapList is sorted in increasing order of time, i.e. older snapshot at
	// lower index and newer snapshot at higher index in list.
	snapLen := len(snapList)
	var snapStreamIndexList []int
	snapStreamIndexList = append(snapStreamIndexList, 0)
	for index := 1; index < snapLen; index++ {
		if snapList[index].Kind == snapstore.SnapshotKindFull && !snapList[index].IsChunk {
			snapStreamIndexList = append(snapStreamIndexList, index)
		}
	}
	return snapStreamIndexList
}

// garbageCollectChunks deletes the chunks in the store from snaplist starting at index low (inclusive) till high (exclusive).
func garbageCollectChunks(ctx context.Context, store snapstore.SnapStore, snapList snapstore.SnapList, low, high int) {
	for index := low; index < high; index++ {
		snap := snapList[index]
		if snap.Kind == snapstore.SnapshotKindDelta {
			continue
		}
		snapPath := path.Join(snap.SnapDir, snap.SnapName)
		logrus.Infof("GC: Deleting chunk for old full snapshot: %s", snapPath)
		if err := store.Delete(ctx, *snap); err != nil {
			logrus.Warnf("GC: Failed to delete snapshot %s: %v", snapPath, err)
			metrics.GCSnapshotCounter.With(prometheus.Labels{metrics.LabelKind: snapstore.SnapshotKindChunk, metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Inc()
			continue
		}
		metrics.GCSnapshotCounter.With(prometheus.Labels{metrics.LabelKind: snapstore.SnapshotKindChunk, metrics.LabelSucceeded: metrics.ValueSucceededTrue}).Inc()
	}
}

// garbageCollectDeltaSnapshots deletes only the delta snapshots from time sorted <snapStream>. It won't delete the full snapshot
// in snapstream which supposed to be at index 0 in <snapStream>.
func (ssr *Snapshotter) garbageCollectDeltaSnapshots(ctx context.Context, snapStream snapstore.SnapList) (int, error) {
	total := 0
	for i := len(snapStream) - 1; i > 0; i-- {
		if (*snapStream[i]).Kind != snapstore.SnapshotKindDelta {
			continue
		}
		snapPath := path.Join(snapStream[i].SnapDir, snapStream[i].SnapName)
		ssr.logger.Infof("GC: Deleting old delta snapshot: %s", snapPath)
		if err := ssr.store.Delete(ctx, *snapStream[i]); err != nil {
			ssr.logger.Warnf("GC: Failed to delete snapshot %s: %v", snapPath, err)
			metrics.GCSnapshotCounter.With(prometheus.Labels{metrics.LabelKind: snapstore.SnapshotKindDelta, metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Inc()
			return total, err
		}
		metrics.GCSnapshotCounter.With(prometheus.Labels{metrics.LabelKind: snapstore.SnapshotKindDelta, metrics.LabelSucceeded: metrics.ValueSucceededTrue}).Inc()
		total++
	}
	return total, nil
}
