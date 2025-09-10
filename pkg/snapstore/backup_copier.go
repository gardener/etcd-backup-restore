// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

	"github.com/sirupsen/logrus"
)

// BackupCopier handles copying unique backups from primary to secondary snapstore
type BackupCopier struct {
	primary           brtypes.SnapStore
	secondary         brtypes.SnapStore
	logger            *logrus.Entry
	config            *BackupCopierConfig
	stopCh            chan struct{}
	lastSyncTimestamp time.Time
	mu                sync.RWMutex
	running           bool
}

// BackupCopierConfig holds configuration for the backup copier
type BackupCopierConfig struct {
	// SyncPeriod defines how often to check for new backups to copy
	SyncPeriod time.Duration
	// MaxRetries defines maximum number of retries for failed copy operations
	MaxRetries int
	// RetryBackoff defines the backoff duration between retries
	RetryBackoff time.Duration
	// ConcurrentCopies defines maximum number of concurrent copy operations
	ConcurrentCopies int
}

// DefaultBackupCopierConfig returns default configuration for backup copier
func DefaultBackupCopierConfig() *BackupCopierConfig {
	return &BackupCopierConfig{
		SyncPeriod:       5 * time.Minute,
		MaxRetries:       3,
		RetryBackoff:     30 * time.Second,
		ConcurrentCopies: 2,
	}
}

// NewBackupCopier creates a new BackupCopier instance
func NewBackupCopier(primary, secondary brtypes.SnapStore, config *BackupCopierConfig, logger *logrus.Entry) *BackupCopier {
	if config == nil {
		config = DefaultBackupCopierConfig()
	}

	return &BackupCopier{
		primary:   primary,
		secondary: secondary,
		logger:    logger.WithField("actor", "backup-copier"),
		config:    config,
		stopCh:    make(chan struct{}),
	}
}

// Start begins the background copier goroutine
func (bc *BackupCopier) Start(ctx context.Context) {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	if bc.running {
		bc.logger.Info("Backup copier is already running")
		return
	}

	bc.running = true
	bc.logger.Info("Starting backup copier...")

	go bc.run(ctx)
}

// Stop stops the background copier goroutine
func (bc *BackupCopier) Stop() {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	if !bc.running {
		return
	}

	bc.logger.Info("Stopping backup copier...")
	close(bc.stopCh)
	bc.running = false
}

// IsRunning returns whether the backup copier is currently running
func (bc *BackupCopier) IsRunning() bool {
	bc.mu.RLock()
	defer bc.mu.RUnlock()
	return bc.running
}

// GetLastSyncTimestamp returns the timestamp of the last successful sync
func (bc *BackupCopier) GetLastSyncTimestamp() time.Time {
	bc.mu.RLock()
	defer bc.mu.RUnlock()
	return bc.lastSyncTimestamp
}

// run is the main loop for the backup copier
func (bc *BackupCopier) run(ctx context.Context) {
	defer func() {
		bc.mu.Lock()
		bc.running = false
		bc.mu.Unlock()
	}()

	ticker := time.NewTicker(bc.config.SyncPeriod)
	defer ticker.Stop()

	// Perform initial sync
	if err := bc.syncBackups(ctx); err != nil {
		bc.logger.Errorf("Initial backup sync failed: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			bc.logger.Info("Context cancelled, stopping backup copier")
			return
		case <-bc.stopCh:
			bc.logger.Info("Stop signal received, stopping backup copier")
			return
		case <-ticker.C:
			if err := bc.syncBackups(ctx); err != nil {
				bc.logger.Errorf("Backup sync failed: %v", err)
			}
		}
	}
}

// syncBackups performs the actual sync operation
func (bc *BackupCopier) syncBackups(ctx context.Context) error {
	bc.logger.Info("Starting backup synchronization...")

	// List snapshots from primary
	primarySnapshots, err := bc.primary.List(true)
	if err != nil {
		return fmt.Errorf("failed to list primary snapshots: %w", err)
	}

	// List snapshots from secondary
	secondarySnapshots, err := bc.secondary.List(true)
	if err != nil {
		return fmt.Errorf("failed to list secondary snapshots: %w", err)
	}

	// Create maps for efficient snapshot comparison
	primaryMap, secondaryMap := bc.createSnapshotMaps(primarySnapshots, secondarySnapshots)

	// Find snapshots to copy and delete
	missingSnapshots := bc.findMissingSnapshots(primarySnapshots, secondaryMap)
	snapshotsToDelete := bc.findMissingSnapshots(secondarySnapshots, primaryMap)

	if len(missingSnapshots) == 0 && len(snapshotsToDelete) == 0 {
		bc.logger.Info("No snapshots to sync")
		bc.updateLastSyncTimestamp()
		return nil
	}

	bc.logger.Infof("Found %d snapshots to copy and %d to delete", len(missingSnapshots), len(snapshotsToDelete))

	return bc.reconcileSnapshots(ctx, missingSnapshots, snapshotsToDelete)
}

// reconcileSnapshots copies and deletes snapshots with concurrency control
func (bc *BackupCopier) reconcileSnapshots(ctx context.Context, snapshots []*brtypes.Snapshot, snapshotsToDelete []*brtypes.Snapshot) error {
	if len(snapshots) > 0 {
		if err := bc.executeSnapshotOperations(ctx, snapshots, "copy", bc.copySnapshot); err != nil {
			return err
		}
		bc.logger.Infof("Successfully copied all %d snapshots to secondary", len(snapshots))
		bc.updateLastSyncTimestamp()
	}

	if len(snapshotsToDelete) > 0 {
		if err := bc.executeSnapshotOperations(ctx, snapshotsToDelete, "delete", bc.deleteSnapshot); err != nil {
			return err
		}
		bc.logger.Infof("Successfully deleted all %d snapshots from secondary", len(snapshotsToDelete))
	}

	return nil
}

// executeSnapshotOperations executes operations on snapshots concurrently
func (bc *BackupCopier) executeSnapshotOperations(ctx context.Context, snapshots []*brtypes.Snapshot, operation string, operationFn func(context.Context, *brtypes.Snapshot) error) error {
	semaphore := make(chan struct{}, bc.config.ConcurrentCopies)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errors []error

	for _, snap := range snapshots {
		select {
		case <-ctx.Done():
			bc.logger.Infof("Context cancelled during snapshot %s", operation)
			return ctx.Err()
		case semaphore <- struct{}{}:
			wg.Add(1)
			go func(snapshot *brtypes.Snapshot) {
				defer func() {
					<-semaphore
					wg.Done()
				}()

				if err := operationFn(ctx, snapshot); err != nil {
					mu.Lock()
					errors = append(errors, err)
					mu.Unlock()
					bc.logger.Errorf("Failed to %s snapshot %s: %v", operation, snapshot.SnapName, err)
				} else {
					bc.logger.Infof("Successfully %sd snapshot %s", operation, snapshot.SnapName)
				}
			}(snap)
		}
	}

	wg.Wait()

	if len(errors) > 0 {
		bc.logger.Errorf("%s operation completed with %d errors out of %d snapshots", operation, len(errors), len(snapshots))
		return fmt.Errorf("failed to %s %d snapshots", operation, len(errors))
	}

	return nil
}

// retryOperation executes an operation with retry logic
func (bc *BackupCopier) retryOperation(ctx context.Context, snapshot *brtypes.Snapshot, operation string, operationFn func() error) error {
	var lastErr error

	for attempt := 0; attempt <= bc.config.MaxRetries; attempt++ {
		if attempt > 0 {
			bc.logger.Infof("Retrying %s of snapshot %s (attempt %d/%d)", operation, snapshot.SnapName, attempt+1, bc.config.MaxRetries+1)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(bc.config.RetryBackoff):
			}
		}

		if err := operationFn(); err != nil {
			lastErr = err
			continue
		}

		return nil
	}

	return fmt.Errorf("failed to %s snapshot %s after %d attempts: %w", operation, snapshot.SnapName, bc.config.MaxRetries+1, lastErr)
}

// copySnapshot copies a single snapshot from primary to secondary with retry logic
func (bc *BackupCopier) copySnapshot(ctx context.Context, snapshot *brtypes.Snapshot) error {
	return bc.retryOperation(ctx, snapshot, "copy", func() error {
		reader, err := bc.primary.Fetch(*snapshot)
		if err != nil {
			return fmt.Errorf("failed to fetch snapshot from primary: %w", err)
		}
		defer reader.Close()

		if err := bc.secondary.Save(*snapshot, reader); err != nil {
			return fmt.Errorf("failed to save snapshot to secondary: %w", err)
		}

		return nil
	})
}

// deleteSnapshot deletes a single snapshot from secondary with retry logic
func (bc *BackupCopier) deleteSnapshot(ctx context.Context, snapshot *brtypes.Snapshot) error {
	return bc.retryOperation(ctx, snapshot, "delete", func() error {
		if err := bc.secondary.Delete(*snapshot); err != nil {
			return fmt.Errorf("failed to delete snapshot from secondary: %w", err)
		}
		return nil
	})
}

// getSnapshotKey generates a unique key for a snapshot for comparison
func (bc *BackupCopier) getSnapshotKey(snapshot *brtypes.Snapshot) string {
	// Use a combination of directory, name, and revision for uniqueness
	return fmt.Sprintf("%s/%s:%d-%d", strings.TrimPrefix(snapshot.SnapDir, "/"), snapshot.SnapName, snapshot.StartRevision, snapshot.LastRevision)
}

// createSnapshotMaps creates maps for efficient snapshot lookup
func (bc *BackupCopier) createSnapshotMaps(primarySnapshots, secondarySnapshots []*brtypes.Snapshot) (map[string]*brtypes.Snapshot, map[string]*brtypes.Snapshot) {
	primaryMap := make(map[string]*brtypes.Snapshot)
	for _, snap := range primarySnapshots {
		key := bc.getSnapshotKey(snap)
		primaryMap[key] = snap
	}

	secondaryMap := make(map[string]*brtypes.Snapshot)
	for _, snap := range secondarySnapshots {
		key := bc.getSnapshotKey(snap)
		secondaryMap[key] = snap
	}

	return primaryMap, secondaryMap
}

// findMissingSnapshots finds snapshots that exist in source but not in target map
func (bc *BackupCopier) findMissingSnapshots(sourceSnapshots []*brtypes.Snapshot, targetMap map[string]*brtypes.Snapshot) []*brtypes.Snapshot {
	var missing []*brtypes.Snapshot
	for _, snap := range sourceSnapshots {
		key := bc.getSnapshotKey(snap)
		if _, exists := targetMap[key]; !exists {
			missing = append(missing, snap)
		}
	}
	return missing
}

// updateLastSyncTimestamp updates the last sync timestamp
func (bc *BackupCopier) updateLastSyncTimestamp() {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	bc.lastSyncTimestamp = time.Now().UTC()
}

// SyncOnce performs a one-time sync operation
func (bc *BackupCopier) SyncOnce(ctx context.Context) error {
	bc.logger.Info("Performing one-time backup synchronization...")
	return bc.syncBackups(ctx)
}
