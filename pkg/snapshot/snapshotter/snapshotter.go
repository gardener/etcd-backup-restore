// Copyright © 2018 The Gardener Authors.
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
	"fmt"
	"path"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/gardener/etcd-backup-restore/pkg/errors"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	"github.com/robfig/cron"
	"github.com/sirupsen/logrus"
)

// NewSnapshotter returns the snapshotter object.
func NewSnapshotter(endpoints string, schedule string, store snapstore.SnapStore, logger *logrus.Logger, maxBackups int, etcdConnectionTimeout time.Duration) (*Snapshotter, error) {
	logger.Printf("Validating schedule...")
	sdl, err := cron.ParseStandard(schedule)
	if err != nil {
		return nil, fmt.Errorf("invalid schedule provied %s : %v", schedule, err)
	}
	return &Snapshotter{
		logger:                logger,
		schedule:              sdl,
		endpoints:             endpoints,
		store:                 store,
		maxBackups:            maxBackups,
		etcdConnectionTimeout: etcdConnectionTimeout,
	}, nil
}

// Run process loop for scheduled backup
func (ssr *Snapshotter) Run(stopCh <-chan struct{}) error {
	now := time.Now()
	effective := ssr.schedule.Next(now)
	if effective.IsZero() {
		ssr.logger.Infoln("There are no backup scheduled for future. Stopping now.")
		return nil
	}
	ssr.logger.Infof("Will take next snapshot at time: %s", effective)
	for {
		select {
		case <-stopCh:
			//TODO: Cleanup work here
			ssr.logger.Infof("Stop signal received. Terminating scheduled snapshot.")
			return nil
		case <-time.After(effective.Sub(now)):
			ssr.logger.Infof("Taking scheduled snapshot for time: %s", time.Now().Local())
			err := ssr.takeFullSnapshot()
			if err != nil {
				// As per design principle, in business critical service if backup is not working,
				// it's better to fail the process. So, we are quiting here.
				return err
			}
			now = time.Now()
			effective = ssr.schedule.Next(now)

			ssr.garbageCollector()
			if effective.IsZero() {
				ssr.logger.Infoln("There are no backup scheduled for future. Stopping now.")
				return nil
			}
			ssr.logger.Infof("Will take next snapshot at time: %s", effective)
		}
	}
}

// takeFullSnapshot will store full snapshot of etcd to snapstore.
// It basically will connect to etcd. Then ask for snapshot. And finally
// store it to underlying snapstore on the fly.
func (ssr *Snapshotter) takeFullSnapshot() error {
	var err error
	client, err := clientv3.NewFromURL(ssr.endpoints)
	if err != nil {
		return &errors.EtcdError{
			Message: fmt.Sprintf("failed to create etcd client: %v", err),
		}
	}

	ctx, cancel := context.WithTimeout(context.TODO(), ssr.etcdConnectionTimeout*time.Second)
	defer cancel()
	resp, err := client.Get(ctx, "", clientv3.WithLastRev()...)
	if err != nil {
		return &errors.EtcdError{
			Message: fmt.Sprintf("failed to get etcd latest revision: %v", err),
		}
	}
	lastRevision := resp.Header.Revision
	rc, err := client.Snapshot(ctx)
	if err != nil {
		return &errors.EtcdError{
			Message: fmt.Sprintf("failed to create etcd snapshot: %v", err),
		}
	}
	ssr.logger.Infof("Successfully opened snapshot reader on etcd")
	s := snapstore.Snapshot{
		Kind:          snapstore.SnapshotKindFull,
		CreatedOn:     time.Now(),
		StartRevision: 0,
		LastRevision:  lastRevision,
	}
	s.GenerateSnapshotDirectory()
	s.GenerateSnapshotName()

	err = ssr.store.Save(s, rc)
	if err != nil {
		return &errors.SnapstoreError{
			Message: fmt.Sprintf("failed to save snapshot: %v", err),
		}
	}
	ssr.logger.Infof("Successfully saved full snapshot at: %s", path.Join(s.SnapDir, s.SnapName))
	return nil
}

// garbageCollector basically consider the older backups as garbage and deletes it
func (ssr *Snapshotter) garbageCollector() {
	ssr.logger.Infoln("Executing garbage collection...")
	snapList, err := ssr.store.List()
	if err != nil {
		ssr.logger.Warnf("Failed to list snapshots: %v", err)
		return
	}
	snapLen := len(snapList)
	for i := 0; i < (snapLen - ssr.maxBackups); i++ {
		ssr.logger.Infof("Deleting old snapshot: %s", path.Join(snapList[i].SnapDir, snapList[i].SnapName))
		err = ssr.store.Delete(*snapList[i])
		if err != nil {
			ssr.logger.Warnf("Failed to delete snapshot: %s", path.Join(snapList[i].SnapDir, snapList[i].SnapName))
		}
	}
}
