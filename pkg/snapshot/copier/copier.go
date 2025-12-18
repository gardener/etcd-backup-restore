// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package copier

import (
	"context"
	"fmt"
	"strings"
	"sync"
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
	logger                      *logrus.Entry
	sourceSnapStore             brtypes.SnapStore
	destSnapStore               brtypes.SnapStore
	maxBackups                  int
	maxBackupAge                int
	maxParallelCopyOperations   int
	waitForFinalSnapshotTimeout time.Duration
	mu                          sync.Mutex
	running                     bool
	waitForFinalSnapshot        bool
}

// NewCopier creates a new copier.
func NewCopier(
	logger *logrus.Entry,
	sourceSnapStore brtypes.SnapStore,
	destSnapStore brtypes.SnapStore,
	maxBackups int,
	maxBackupAge int,
	maxParallelCopyOperations int,
	waitForFinalSnapshot bool,
	waitForFinalSnapshotTimeout time.Duration,
) *Copier {
	return &Copier{
		logger:                      logger.WithField("actor", "copier"),
		sourceSnapStore:             sourceSnapStore,
		destSnapStore:               destSnapStore,
		maxBackups:                  maxBackups,
		maxBackupAge:                maxBackupAge,
		maxParallelCopyOperations:   maxParallelCopyOperations,
		waitForFinalSnapshot:        waitForFinalSnapshot,
		waitForFinalSnapshotTimeout: waitForFinalSnapshotTimeout,
	}
}

// Run executes the copy command.
func (c *Copier) Run(ctx context.Context) error {
	return c.CopyBackups(ctx)
}

const (
	// finalSnapshotCheckInterval is the interval between checks for a final full snapshot.
	finalSnapshotCheckInterval = 15 * time.Second
)

// CopyBackups copies all backups from the source store to the destination store
// when a final full snapshot is detected in the source store.
func (c *Copier) CopyBackups(ctx context.Context) error {
	if c.waitForFinalSnapshot {
		c.logger.Info("Waiting for final full snapshot...")
		if c.waitForFinalSnapshotTimeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, c.waitForFinalSnapshotTimeout)
			defer cancel()
		}
		if _, err := c.doWaitForFinalSnapshot(ctx, finalSnapshotCheckInterval, c.sourceSnapStore); err != nil {
			return fmt.Errorf("could not wait for final full snapshot: %v", err)
		}
		if ctx.Err() != nil {
			c.logger.Info("Timed out waiting for final full snapshot")
		} else {
			c.logger.Info("Final full snapshot detected")
		}
	}

	c.logger.Info("Copying backups ...")
	if err := c.copyBackups(); err != nil {
		return fmt.Errorf("could not copy backups: %v", err)
	}
	c.logger.Info("Backups copied")

	return nil
}

// copyBackups copies all backups from the source store to the destination store.
func (c *Copier) copyBackups() error {
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
	destSnapshots, err := c.destSnapStore.List(false)
	if err != nil {
		return fmt.Errorf("could not get destination snapshots: %v", err)
	}
	destSnapshotsMap := make(map[string]*brtypes.Snapshot)
	for _, snapshot := range destSnapshots {
		destSnapshotsMap[snapshot.SnapName] = snapshot
	}

	// find snapshots missing in destination
	var snapshotsToCopy brtypes.SnapList
	for _, snapshot := range sourceSnapshot {
		snapNameWithoutSuffix := strings.TrimSuffix(snapshot.SnapName, brtypes.FinalSuffix)
		if _, ok := destSnapshotsMap[snapNameWithoutSuffix]; !ok {
			snapshotsToCopy = append(snapshotsToCopy, snapshot)
		} else {
			c.logger.Infof("Skipping %s snapshot %s as it already exists", snapshot.Kind, snapshot.SnapName)
		}
	}

	if len(snapshotsToCopy) == 0 {
		return nil
	}

	// copy all missing snapshots with configured concurrency
	var (
		wg     sync.WaitGroup
		queue  = make(chan *brtypes.Snapshot, c.maxParallelCopyOperations)
		errors = make(chan error)
	)

	// Enqueue all work items.
	// This needs to happen in a goroutine, as the queue's buffer might not suffice to hold all work items.
	// Close the queue and the errors channel once all snapshots have been copied.
	go func() {
		defer close(errors)
		defer close(queue)

		for _, snapshot := range snapshotsToCopy {
			wg.Add(1)
			queue <- snapshot
		}

		wg.Wait()
	}()

	// Spawn configured amount of workers for copying snapshots.
	// Each worker takes one item at a time from the queue and exits once the queue is closed.
	for i := 0; i < c.maxParallelCopyOperations; i++ {
		go func() {
			for snapshot := range queue {
				func() {
					defer wg.Done()

					c.logger.Infof("Copying %s snapshot %s...", snapshot.Kind, snapshot.SnapName)
					if err := c.copySnapshot(snapshot); err != nil {
						errors <- err
						return
					}

					c.logger.Infof("Successfully copied %s snapshot %s...", snapshot.Kind, snapshot.SnapName)
				}()
			}
		}()
	}

	// Collect all errors and wait for operations to finish.
	var allErrors []error
	for err := range errors {
		allErrors = append(allErrors, err)
	}

	if len(allErrors) > 0 {
		return fmt.Errorf("%s", allErrors)
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

	snapshot.SetFinal(false)
	if err := c.destSnapStore.Save(*snapshot, rc); err != nil {
		return fmt.Errorf("could not save snapshot %s to destination store: %v", snapshot.SnapName, err)
	}

	return nil
}

// doWaitForFinalSnapshot waits for a final full snapshot in the given store.
func (c *Copier) doWaitForFinalSnapshot(ctx context.Context, interval time.Duration, ss brtypes.SnapStore) (*brtypes.Snapshot, error) {
	c.logger.Debug("Starting waiting for final full snapshot")
	defer c.logger.Debug("Stopping waiting for final full snapshot")

	for {
		fullSnapshot, _, err := miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(ss)
		if err != nil {
			return nil, err
		}
		if fullSnapshot != nil && fullSnapshot.IsFinal {
			return fullSnapshot, nil
		}

		select {
		case <-ctx.Done():
			return nil, nil
		case <-time.After(interval):
		}
	}
}

// SyncBackups periodically synchronizes backups to the secondary snapstore.
func (c *Copier) SyncBackups(ctx context.Context, interval time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.running {
		c.logger.Info("Backup copier is already running")
		return nil
	}

	c.running = true
	c.logger.Info("Starting backup copier...")

	go c.syncBackups(ctx, interval)
	return nil
}

func (c *Copier) syncBackups(ctx context.Context, interval time.Duration) {
	defer func() {
		c.mu.Lock()
		c.running = false
		c.mu.Unlock()
	}()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	// start an initial sync first
	if err := c.CopyBackups(ctx); err != nil {
		c.logger.Errorf("could not perform the initial copy of backups: %v", err)
	}
	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Backup copier is shutting down")
			return
		case <-ticker.C:
			if err := c.CopyBackups(ctx); err != nil {
				c.logger.Errorf("could not copy backups: %v", err)
			}
		}
	}
}
