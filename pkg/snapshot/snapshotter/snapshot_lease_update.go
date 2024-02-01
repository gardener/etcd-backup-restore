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

package snapshotter

import (
	"context"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/health/heartbeat"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
)

var (
	fullSnapshotLeaseUpdateRetryInterval = 3 * time.Minute // retry interval for updating full snapshot lease. Ideally should be >= 1 minute
)

// RenewFullSnapshotLeasePeriodically has a timer and will periodically call FullSnapshotCaseLeaseUpdate to renew the fullsnapshot lease until it is updated or stopped.
// The timer starts upon snapshotter initialization and is reset after every full snapshot is taken.
func (ssr *Snapshotter) RenewFullSnapshotLeasePeriodically() {
	ssr.FullSnapshotLeaseUpdateTimer = time.NewTimer(fullSnapshotLeaseUpdateRetryInterval)
	fullSnapshotLeaseUpdateCtx, fullSnapshotLeaseUpdateCancel := context.WithCancel(context.TODO())
	defer fullSnapshotLeaseUpdateCancel()
	for {
		select {
		case <-ssr.FullSnapshotLeaseUpdateTimer.C:
			if ssr.healthConfig.SnapshotLeaseRenewalEnabled {
				if ssr.PrevFullSnapshot != nil {
					ctx, cancel := context.WithTimeout(fullSnapshotLeaseUpdateCtx, brtypes.LeaseUpdateTimeoutDuration)
					if _, err := heartbeat.FullSnapshotCaseLeaseUpdate(ctx, ssr.logger, ssr.PrevFullSnapshot, ssr.K8sClientset, ssr.healthConfig.FullSnapshotLeaseName); err != nil {
						ssr.logger.Warnf("FullSnapshot lease update failed : %v", err)
						ssr.logger.Infof("Resetting the FullSnapshot lease to retry updating with revision %d after %v", ssr.PrevSnapshot.LastRevision, fullSnapshotLeaseUpdateRetryInterval.String())
						ssr.FullSnapshotLeaseUpdateTimer.Stop()
						ssr.FullSnapshotLeaseUpdateTimer.Reset(fullSnapshotLeaseUpdateRetryInterval)
					} else {
						ssr.logger.Infof("FullSnapshot lease successfully updated with revision %d", ssr.PrevSnapshot.LastRevision)
						ssr.logger.Infof("Stopping the FullSnapshot lease update")
						ssr.FullSnapshotLeaseUpdateTimer.Stop()
					}
					cancel()
				} else {
					ssr.logger.Infof("Skipping the FullSnapshot lease update since no full snapshot has been taken yet")
					ssr.logger.Infof("Resetting the FullSnapshot lease to retry updating after %v", fullSnapshotLeaseUpdateRetryInterval.String())
					ssr.FullSnapshotLeaseUpdateTimer.Stop()
					ssr.FullSnapshotLeaseUpdateTimer.Reset(fullSnapshotLeaseUpdateRetryInterval)
				}
			}

		case <-ssr.FullSnapshotLeaseStopCh:
			ssr.logger.Info("Closing the full snapshot lease renewal")
			if ssr.FullSnapshotLeaseUpdateTimer != nil {
				ssr.FullSnapshotLeaseUpdateTimer.Stop()
				ssr.FullSnapshotLeaseUpdateTimer = nil
			}
			return
		}
	}
}
