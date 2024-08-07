// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapshotter

import (
	"math"
	"path"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/metrics"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

	"github.com/prometheus/client_golang/prometheus"
)

// RunGarbageCollector basically consider the older backups as garbage and deletes it
func (ssr *Snapshotter) RunGarbageCollector(stopCh <-chan struct{}) {
	if ssr.config.GarbageCollectionPeriod.Duration <= time.Second {
		ssr.logger.Infof("GC: Not running garbage collector since GarbageCollectionPeriod [%s] set to less than 1 second.", ssr.config.GarbageCollectionPeriod)
		return
	}

	for {
		select {
		case <-stopCh:
			ssr.logger.Info("GC: Stop signal received. Closing garbage collector.")
			return
		case <-time.After(ssr.config.GarbageCollectionPeriod.Duration):

			var err error
			// Update the snapstore object before taking any action on object storage bucket.
			// Refer: https://github.com/gardener/etcd-backup-restore/issues/422
			ssr.store, err = snapstore.GetSnapstore(ssr.snapstoreConfig)
			if err != nil {
				ssr.logger.Warnf("GC: Failed to create snapstore from configured storage provider: %v", err)
				continue
			}

			total := 0
			ssr.logger.Info("GC: Executing garbage collection...")
			snapList, err := ssr.store.List()
			if err != nil {
				metrics.SnapshotterOperationFailure.With(prometheus.Labels{metrics.LabelError: err.Error()}).Inc()
				ssr.logger.Warnf("GC: Failed to list snapshots: %v", err)
				continue
			}

			// Skip chunk deletion for openstack swift provider, since the manifest object is a virtual
			// representation of the object, and the actual data is stored in the segment objects, aka chunks
			// Chunk deletion for this provider is handled in regular snapshot deletion
			if ssr.snapstoreConfig.Provider == brtypes.SnapstoreProviderSwift {
				var filteredSnapList brtypes.SnapList
				for _, snap := range snapList {
					if !snap.IsChunk {
						filteredSnapList = append(filteredSnapList, snap)
					}
				}
				snapList = filteredSnapList
			} else {
				// chunksDeleted stores the no of chunks deleted in the current iteration of GC.
				var chunksDeleted int
				chunksDeleted, snapList = ssr.GarbageCollectChunks(snapList)
				ssr.logger.Infof("GC: Total number garbage collected chunks: %d", chunksDeleted)
			}

			snapStreamIndexList := getSnapStreamIndexList(snapList)

			switch ssr.config.GarbageCollectionPolicy {
			case brtypes.GarbageCollectionPolicyExponential:
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
					deletedSnap, err := ssr.GarbageCollectDeltaSnapshots(snapList[snapStreamIndexList[snapStreamIndex-1]:snapStreamIndexList[snapStreamIndex]])
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
						if !nextSnap.IsDeletable() {
							ssr.logger.Infof("GC: Skipping : %s, since it is immutable", nextSnap.SnapName)
							continue
						}
						ssr.logger.Infof("GC: Deleting old full snapshot: %s %v", nextSnap.CreatedOn.UTC(), deleteSnap)
						if err := ssr.store.Delete(*nextSnap); err != nil {
							ssr.logger.Warnf("GC: Failed to delete snapshot %s: %v", path.Join(nextSnap.SnapDir, nextSnap.SnapName), err)
							metrics.SnapshotterOperationFailure.With(prometheus.Labels{metrics.LabelError: err.Error()}).Inc()
							metrics.GCSnapshotCounter.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindFull, metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Inc()
							continue
						}
						metrics.GCSnapshotCounter.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindFull, metrics.LabelSucceeded: metrics.ValueSucceededTrue}).Inc()
						total++
					}
				}

			case brtypes.GarbageCollectionPolicyLimitBased:
				// Delete delta snapshots in all snapStream but the latest one.
				// Delete all snapshots beyond limit set by ssr.maxBackups.
				for snapStreamIndex := 0; snapStreamIndex < len(snapStreamIndexList)-1; snapStreamIndex++ {
					deletedSnap, err := ssr.GarbageCollectDeltaSnapshots(snapList[snapStreamIndexList[snapStreamIndex]:snapStreamIndexList[snapStreamIndex+1]])
					total += deletedSnap
					if err != nil {
						continue
					}
					if snapStreamIndex < len(snapStreamIndexList)-int(ssr.config.MaxBackups) {
						snap := snapList[snapStreamIndexList[snapStreamIndex]]
						snapPath := path.Join(snap.SnapDir, snap.SnapName)
						ssr.logger.Infof("GC: Deleting old full snapshot: %s", snapPath)
						if err := ssr.store.Delete(*snap); err != nil {
							ssr.logger.Warnf("GC: Failed to delete snapshot %s: %v", snapPath, err)
							metrics.SnapshotterOperationFailure.With(prometheus.Labels{metrics.LabelError: err.Error()}).Inc()
							metrics.GCSnapshotCounter.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindFull, metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Inc()
							continue
						}
						metrics.GCSnapshotCounter.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindFull, metrics.LabelSucceeded: metrics.ValueSucceededTrue}).Inc()
						total++
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
func getSnapStreamIndexList(snapList brtypes.SnapList) []int {
	// At this stage, we assume the snapList is sorted in increasing order of last revision number, i.e. snapshot with lower
	// last revision at lower index and snapshot with higher last revision at higher index in list.
	snapLen := len(snapList)
	var snapStreamIndexList []int
	snapStreamIndexList = append(snapStreamIndexList, 0)
	for index := 1; index < snapLen; index++ {
		if snapList[index].Kind == brtypes.SnapshotKindFull && !snapList[index].IsChunk {
			snapStreamIndexList = append(snapStreamIndexList, index)
		}
	}
	return snapStreamIndexList
}

// GarbageCollectChunks removes obsolete chunks based on the latest recorded snapshot.
// It eliminates chunks associated with snapshots that have already been uploaded.
// Additionally, it avoids deleting chunks linked to snapshots currently being uploaded to prevent the garbage collector from removing chunks before the composite is formed.
func (ssr *Snapshotter) GarbageCollectChunks(snapList brtypes.SnapList) (int, brtypes.SnapList) {
	var nonChunkSnapList brtypes.SnapList
	chunksDeleted := 0
	for _, snap := range snapList {
		// If not chunk, add to list and continue
		if !snap.IsChunk {
			nonChunkSnapList = append(nonChunkSnapList, snap)
			continue
		}
		// Skip the chunk deletion if it's corresponding full/delta snapshot is not uploaded yet
		if ssr.PrevSnapshot.LastRevision == 0 || snap.StartRevision > ssr.PrevSnapshot.LastRevision {
			continue
		}
		// delete the chunk object
		snapPath := path.Join(snap.SnapDir, snap.SnapName)
		ssr.logger.Infof("GC: Deleting chunk for old snapshot: %s", snapPath)
		if err := ssr.store.Delete(*snap); err != nil {
			ssr.logger.Warnf("GC: Failed to delete chunk %s: %v", snapPath, err)
			metrics.SnapshotterOperationFailure.With(prometheus.Labels{metrics.LabelError: err.Error()}).Inc()
			metrics.GCSnapshotCounter.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindChunk, metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Inc()
			continue
		}
		chunksDeleted++
		metrics.GCSnapshotCounter.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindChunk, metrics.LabelSucceeded: metrics.ValueSucceededTrue}).Inc()
	}
	return chunksDeleted, nonChunkSnapList
}

/*
GarbageCollectDeltaSnapshots traverses the list of snapshots and removes delta snapshots that are older than the retention period specified in the Snapshotter's configuration.

Parameters:

	snapStream brtypes.SnapList - List of snapshots to perform garbage collection on.

Returns:

	int - Total number of delta snapshots deleted.
	error - Error information, if any error occurred during the garbage collection. Returns 'nil' if operation is successful.
*/
func (ssr *Snapshotter) GarbageCollectDeltaSnapshots(snapStream brtypes.SnapList) (int, error) {
	totalDeleted := 0
	cutoffTime := time.Now().UTC().Add(-ssr.config.DeltaSnapshotRetentionPeriod.Duration)
	for i := len(snapStream) - 1; i >= 0; i-- {
		if (*snapStream[i]).Kind == brtypes.SnapshotKindDelta && snapStream[i].CreatedOn.Before(cutoffTime) {

			snapPath := path.Join(snapStream[i].SnapDir, snapStream[i].SnapName)
			ssr.logger.Infof("GC: Deleting old delta snapshot: %s", snapPath)
			if !snapStream[i].IsDeletable() {
				ssr.logger.Infof("GC: Skipping : %s, since it is immutable", snapPath)
				continue
			}
			if err := ssr.store.Delete(*snapStream[i]); err != nil {
				ssr.logger.Warnf("GC: Failed to delete snapshot %s: %v", snapPath, err)
				metrics.SnapshotterOperationFailure.With(prometheus.Labels{metrics.LabelError: err.Error()}).Inc()
				metrics.GCSnapshotCounter.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindDelta, metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Inc()

				return totalDeleted, err
			}

			metrics.GCSnapshotCounter.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindDelta, metrics.LabelSucceeded: metrics.ValueSucceededTrue}).Inc()
			totalDeleted++
		}
	}

	return totalDeleted, nil
}
