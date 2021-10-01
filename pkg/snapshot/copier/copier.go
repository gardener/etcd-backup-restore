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

package copier

import (
	"fmt"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

	"github.com/sirupsen/logrus"
)

// GetSourceAndDestinationStores returns the source and destination stores for the given source and destination and store configs.
func GetSourceAndDestinationStores(sourceSnapStoreConfig *brtypes.SnapstoreConfig, destSnapStoreConfig *brtypes.SnapstoreConfig) (brtypes.SnapStore, brtypes.SnapStore, error) {
	sourceSnapStore, err := snapstore.GetSnapstore(sourceSnapStoreConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("could not get source snapstore: %v", err)
	}

	destSnapStore, err := snapstore.GetSnapstore(destSnapStoreConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("could not get destination snapstore: %v", err)
	}

	return sourceSnapStore, destSnapStore, nil
}

// Copier can be used to copy backups from a source to a destination store.
type Copier struct {
	logger          *logrus.Entry
	sourceSnapStore brtypes.SnapStore
	destSnapStore   brtypes.SnapStore
	maxBackups      int
	maxBackupAge    int
}

// NewCopier creates a new copier.
func NewCopier(sourceSnapStore brtypes.SnapStore, destSnapStore brtypes.SnapStore, logger *logrus.Entry, maxBackups int, maxBackupAge int) *Copier {
	return &Copier{
		logger:          logger.WithField("actor", "copier"),
		sourceSnapStore: sourceSnapStore,
		destSnapStore:   destSnapStore,
		maxBackups:      maxBackups,
		maxBackupAge:    maxBackupAge,
	}
}

// Run executes the copy command.
func (c *Copier) Run() error {
	return c.CopyBackups()
}

// CopyBackups copies all backups from the source store to the destination store.
func (c *Copier) CopyBackups() error {
	// Get source backups
	c.logger.Info("Getting source backups...")
	sourceSnapshot, err := c.getSnapshots()
	if err != nil {
		return fmt.Errorf("could not get source backups: %v", err)
	}

	// If there are no source backups, do nothing
	if len(sourceSnapshot) == 0 {
		c.logger.Info("No source backups found")
		return nil
	}

	// Get destination snapshots and build a map keyed by name
	c.logger.Info("Getting destination snapshots...")
	destSnapshots, err := c.destSnapStore.List()
	if err != nil {
		return fmt.Errorf("could not get destination snapshots: %v", err)
	}
	destSnapshotsMap := make(map[string]*brtypes.Snapshot)
	for _, snapshot := range destSnapshots {
		destSnapshotsMap[snapshot.SnapName] = snapshot
	}

	for _, snapshot := range sourceSnapshot {
		// Copy all the snapshots (if not already copied)
		if _, ok := destSnapshotsMap[snapshot.SnapName]; !ok {
			c.logger.Infof("Copying %s snapshot %s...", snapshot.Kind, snapshot.SnapName)
			if err := c.copySnapshot(snapshot); err != nil {
				return err
			}
		} else {
			c.logger.Infof("Skipping %s snapshot %s as it already exists", snapshot.Kind, snapshot.SnapName)
		}
	}
	return nil
}

func (c *Copier) getSnapshots() (brtypes.SnapList, error) {
	if c.maxBackupAge >= 0 {
		return miscellaneous.GetFilteredBackups(c.sourceSnapStore, c.maxBackups, func(snap brtypes.Snapshot) bool {
			return snap.CreatedOn.After(time.Now().UTC().AddDate(0, 0, -c.maxBackupAge))
		})
	}
	return miscellaneous.GetFilteredBackups(c.sourceSnapStore, c.maxBackups, nil)
}

func (c *Copier) copySnapshot(snapshot *brtypes.Snapshot) error {
	rc, err := c.sourceSnapStore.Fetch(*snapshot)
	if err != nil {
		return fmt.Errorf("could not fetch snapshot %s from source store: %v", snapshot.SnapName, err)
	}

	if err := c.destSnapStore.Save(*snapshot, rc); err != nil {
		return fmt.Errorf("could not save snapshot %s to destination store: %v", snapshot.SnapName, err)
	}

	return nil
}
