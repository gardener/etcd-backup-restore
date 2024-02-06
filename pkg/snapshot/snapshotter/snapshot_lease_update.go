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
	"github.com/sirupsen/logrus"
)

var (
	logger = logrus.NewEntry(logrus.New()).WithField("actor", "FullSnapLeaseUpdater")
)

// RenewFullSnapshotLeasePeriodically has a timer and will periodically call FullSnapshotCaseLeaseUpdate to renew the fullsnapshot lease until it is updated or stopped.
// The timer starts upon snapshotter initialization and is reset after every full snapshot is taken.
func (ssr *Snapshotter) RenewFullSnapshotLeasePeriodically(fullSnapshotLeaseUpdateRetryInterval time.Duration) {
	ssr.FullSnapshotLeaseUpdateTimer = time.NewTimer(fullSnapshotLeaseUpdateRetryInterval)
	fullSnapshotLeaseUpdateCtx, fullSnapshotLeaseUpdateCancel := context.WithCancel(context.TODO())
	defer fullSnapshotLeaseUpdateCancel()
	for {
		select {
		case <-ssr.FullSnapshotLeaseUpdateTimer.C:
			if ssr.healthConfig.SnapshotLeaseRenewalEnabled {
				if ssr.PrevFullSnapshot != nil {
					ctx, cancel := context.WithTimeout(fullSnapshotLeaseUpdateCtx, brtypes.LeaseUpdateTimeoutDuration)
					if _, err := heartbeat.FullSnapshotCaseLeaseUpdate(ctx, logger, ssr.PrevFullSnapshot, ssr.K8sClientset, ssr.healthConfig.FullSnapshotLeaseName); err != nil {
						logger.Warnf("FullSnapshot lease update failed : %v", err)
						logger.Infof("Resetting the FullSnapshot lease to retry updating with revision %d after %v", ssr.PrevFullSnapshot.LastRevision, fullSnapshotLeaseUpdateRetryInterval.String())
						ssr.FullSnapshotLeaseUpdateTimer.Stop()
						ssr.FullSnapshotLeaseUpdateTimer.Reset(fullSnapshotLeaseUpdateRetryInterval)
					} else {
						logger.Infof("FullSnapshot lease successfully updated with revision %d", ssr.PrevFullSnapshot.LastRevision)
						logger.Infof("Stopping the FullSnapshot lease update")
						ssr.FullSnapshotLeaseUpdateTimer.Stop()
					}
					cancel()
				} else {
					logger.Infof("Skipping the FullSnapshot lease update since no full snapshot has been taken yet")
					logger.Infof("Resetting the FullSnapshot lease to retry updating after %v", fullSnapshotLeaseUpdateRetryInterval.String())
					ssr.FullSnapshotLeaseUpdateTimer.Stop()
					ssr.FullSnapshotLeaseUpdateTimer.Reset(fullSnapshotLeaseUpdateRetryInterval)
				}
			}

		case <-ssr.FullSnapshotLeaseStopCh:
			logger.Info("Closing the full snapshot lease renewal")
			if ssr.FullSnapshotLeaseUpdateTimer != nil {
				ssr.FullSnapshotLeaseUpdateTimer.Stop()
				ssr.FullSnapshotLeaseUpdateTimer = nil
			}
			return
		}
	}
}
