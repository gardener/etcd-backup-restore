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
	"path"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
)

// GarbageCollector basically consider the older backups as garbage and deletes it
func (ssr *Snapshotter) GarbageCollector(stopCh <-chan bool) {
	for {
		select {
		case <-stopCh:
			ssr.logger.Infoln("GC: Stop signal received. Closing garbage collector.")
			return
		case <-time.After(ssr.garbageCollectionPeriodSeconds * time.Second):
			ssr.logger.Infoln("GC: Executing garbage collection...")
			snapList, err := ssr.store.List()
			if err != nil {
				ssr.logger.Warnf("GC: Failed to list snapshots: %v", err)
				continue
			}

			snapLen := len(snapList)
			var snapStreamIndexList []int
			snapStreamIndexList = append(snapStreamIndexList, 0)
			for index := 1; index < snapLen; index++ {
				if snapList[index].Kind == snapstore.SnapshotKindFull {
					snapStreamIndexList = append(snapStreamIndexList, index)
				}
			}

			switch ssr.garbageCollectionPolicy {
			case GarbageCollectionPolicyExponential:
				// Delete delta snapshots in all snapStream but the latest one.
				// Keep only the last 24 hourly backups and of all other backups only the last backup in a day.
				// Keep only the last 7 daily backups and of all other backups only the last backup in a week.
				// Keep only the last 4 weekly backups.

				now := time.Now()
				var (
					deleteSnap  = true
					backupMode  = "None"
					backupCount = -1
				)
				for snapStreamIndex := len(snapStreamIndexList) - 1; snapStreamIndex >= 0; snapStreamIndex-- {
					snap := snapList[snapStreamIndexList[snapStreamIndex]]
					if err := ssr.garbageCollectDeltaSnapshots(snapList[snapStreamIndexList[snapStreamIndex]:snapStreamIndexList[snapStreamIndex+1]]); err != nil {
						continue
					}

					switch backupMode {
					case "None":
						deleteSnap = false
						if !now.Truncate(time.Hour).Equal(snap.CreatedOn.Truncate(time.Hour)) {
							break
						}
						backupMode = "Hour"
						backupCount = 23
						fallthrough

					case "Hour":
						for backupCount >= 0 {
							rounded := time.Date(snap.CreatedOn.Year(), snap.CreatedOn.Month(), snap.CreatedOn.Day(), backupCount, 0, 0, 0, snap.CreatedOn.Location())
							diff := rounded.Sub(snap.CreatedOn)
							if diff == 0 {
								deleteSnap = false
								backupCount--
								break
							} else if diff < 0 {
								deleteSnap = true
								break
							} else {
								backupCount--
							}
						}
						if backupCount >= 0 {
							break
						}
						backupMode = "Day"
						backupCount = 6
						fallthrough

					case "Day":
						for backupCount >= 0 {
							rounded := time.Date(snap.CreatedOn.Year(), snap.CreatedOn.Month(), snap.CreatedOn.Day()-7+backupCount, 0, 0, 0, 0, snap.CreatedOn.Location())
							diff := rounded.Sub(snap.CreatedOn)
							if diff == 0 {
								deleteSnap = false
								backupCount--
								break
							} else if diff < 0 {
								deleteSnap = true
								break
							} else {
								backupCount--
							}
						}
						if backupCount >= 0 {
							break
						}
						backupMode = "Week"
						backupCount = 5
						fallthrough

					case "Week":
						week := 2
						for week <= backupCount {
							rounded := time.Date(snap.CreatedOn.Year(), snap.CreatedOn.Month(), snap.CreatedOn.Day()-7*week, 0, 0, 0, 0, snap.CreatedOn.Location())
							diff := rounded.Sub(snap.CreatedOn)
							if diff == 0 {
								deleteSnap = false
								week++
								break
							} else if diff < 0 {
								deleteSnap = true
								break
							} else {
								week++
							}
						}
						if week > backupCount {
							deleteSnap = true
						}

					}
					if deleteSnap {
						ssr.logger.Infof("GC: Deleting old full snapshot: %s", path.Join(snap.SnapDir, snap.SnapName))
						if err := ssr.store.Delete(*snap); err != nil {
							ssr.logger.Warnf("GC: Failed to delete snapshot %s: %v", path.Join(snap.SnapDir, snap.SnapName), err)
						}
					}
				}
			case GarbageCollectionPolicyLimitBased:
				// Delete delta snapshots in all snapStream but the latest one.
				// Delete all snapshots beyond limit set by ssr.maxBackups.
				for snapStreamIndex := 0; snapStreamIndex < len(snapStreamIndexList)-1; snapStreamIndex++ {
					if err := ssr.garbageCollectDeltaSnapshots(snapList[snapStreamIndexList[snapStreamIndex]:snapStreamIndexList[snapStreamIndex+1]]); err != nil {
						continue
					}
					if snapStreamIndex < len(snapStreamIndexList)-ssr.maxBackups {
						snap := snapList[snapStreamIndexList[snapStreamIndex]]
						ssr.logger.Infof("GC: Deleting old full snapshot: %s", path.Join(snap.SnapDir, snap.SnapName))
						if err := ssr.store.Delete(*snap); err != nil {
							ssr.logger.Warnf("GC: Failed to delete snapshot %s: %v", path.Join(snap.SnapDir, snap.SnapName), err)
						}
					}
				}
			}
		}
	}
}

// garbageCollectDeltaSnapshots deletes only the delta snapshots from time sorted <snapStream>. It won't delete the full snapshot
// in snapstream which supposed to be at index 0 in <snapStream>.
func (ssr *Snapshotter) garbageCollectDeltaSnapshots(snapStream snapstore.SnapList) error {
	for i := len(snapStream) - 1; i > 0; i-- {
		ssr.logger.Infof("GC: Deleting old delta snapshot: %s", path.Join(snapStream[i].SnapDir, snapStream[i].SnapName))
		if err := ssr.store.Delete(*snapStream[i]); err != nil {
			ssr.logger.Warnf("GC: Failed to delete snapshot %s: %v", path.Join(snapStream[i].SnapDir, snapStream[i].SnapName), err)
			return err
		}
	}
	return nil
}
