// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"sort"
	"strings"
	"sync"

	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

	"github.com/sirupsen/logrus"
)

// DualSnapStore implements the SnapStore interface with dual endpoint support.
type DualSnapStore struct {
	primary          brtypes.SnapStore
	secondary        brtypes.SnapStore
	logger           *logrus.Entry
	primaryFailed    bool // Track if primary has failed with transient errors
	primaryFailCount int  // Track consecutive primary failures
	mu               sync.RWMutex
}

// NewDualSnapStore creates a new DualSnapStore with primary and secondary endpoints.
func NewDualSnapStore(primary, secondary brtypes.SnapStore, logger *logrus.Entry) *DualSnapStore {
	return &DualSnapStore{
		primary:   primary,
		secondary: secondary,
		logger:    logger.WithField("actor", "dual-snapstore"),
	}
}

// Save writes the snapshot with intelligent failover logic.
// It tries primary first, and if primary fails with transient errors (DNS, network),
// it switches to secondary-only mode to avoid wasting time on repeated primary failures.
func (ds *DualSnapStore) Save(snap brtypes.Snapshot, rc io.ReadCloser) error {
	// Create a tee reader to duplicate the stream for both endpoints
	data, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("failed to read snapshot data: %v", err)
	}
	rc.Close()

	var primaryResult, secondaryResult brtypes.OperationResult

	// Try primary first (unless it's disabled due to repeated failures)
	if !ds.isPrimaryDisabled() {
		ds.logger.Infof("Attempting to save snapshot %s to primary endpoint", snap.SnapName)
		primaryRC := io.NopCloser(bytes.NewReader(data))
		err := ds.primary.Save(snap, primaryRC)
		if err != nil {
			ds.logger.Errorf("Failed to save snapshot %s to primary endpoint: %v", snap.SnapName, err)
			primaryResult = brtypes.OperationResult{Endpoint: "primary", Success: false, Error: err}
			ds.recordPrimaryFailure(err)
		} else {
			ds.logger.Infof("Successfully saved snapshot %s to primary endpoint", snap.SnapName)
			primaryResult = brtypes.OperationResult{Endpoint: "primary", Success: true}
			ds.resetPrimaryFailureState()
			return nil // Success on primary, no need to try secondary
		}
	} else {
		ds.logger.Infof("Skipping primary endpoint (disabled due to repeated failures)")
		primaryResult = brtypes.OperationResult{Endpoint: "primary", Success: false, Error: fmt.Errorf("primary endpoint disabled due to repeated failures")}
	}

	// Try secondary endpoint (either because primary failed or we're in secondary-only mode)
	ds.logger.Infof("Attempting to save snapshot %s to secondary endpoint", snap.SnapName)
	secondaryRC := io.NopCloser(bytes.NewReader(data))
	err = ds.secondary.Save(snap, secondaryRC)
	if err != nil {
		ds.logger.Errorf("Failed to save snapshot %s to secondary endpoint: %v", snap.SnapName, err)
		secondaryResult = brtypes.OperationResult{Endpoint: "secondary", Success: false, Error: err}
	} else {
		ds.logger.Infof("Successfully saved snapshot %s to secondary endpoint", snap.SnapName)
		secondaryResult = brtypes.OperationResult{Endpoint: "secondary", Success: true}
		return nil // Success on secondary
	}

	// Both endpoints failed
	return fmt.Errorf("failed to save snapshot %s to both endpoints - Primary: %v, Secondary: %v",
		snap.SnapName, primaryResult.Error, secondaryResult.Error)
}

// Fetch attempts to fetch the snapshot from primary first, then secondary if primary fails.
// It respects the primary failure state to avoid wasting time on known-failed endpoints.
func (ds *DualSnapStore) Fetch(snap brtypes.Snapshot) (io.ReadCloser, error) {
	// Check if we should skip primary due to repeated failures
	if !ds.isPrimaryDisabled() {
		// Try primary first
		rc, err := ds.primary.Fetch(snap)
		if err == nil {
			ds.logger.Infof("Successfully fetched snapshot %s from primary endpoint", snap.SnapName)
			ds.resetPrimaryFailureState() // Reset failure state on success
			return rc, nil
		}

		ds.logger.Warnf("Failed to fetch snapshot %s from primary endpoint: %v, trying secondary", snap.SnapName, err)
		ds.recordPrimaryFailure(err) // Track the failure
	} else {
		ds.logger.Infof("Skipping primary endpoint for fetch (disabled due to failures)")
	}

	// Try secondary endpoint
	rc, err := ds.secondary.Fetch(snap)
	if err == nil {
		ds.logger.Infof("Successfully fetched snapshot %s from secondary endpoint", snap.SnapName)
		return rc, nil
	}

	ds.logger.Errorf("Failed to fetch snapshot %s from both endpoints", snap.SnapName)
	return nil, fmt.Errorf("failed to fetch snapshot %s from both endpoints - Primary: %v, Secondary: %v",
		snap.SnapName, fmt.Errorf("disabled due to failures"), err)
}

// List returns a merged and deduplicated list of snapshots from both endpoints.
func (ds *DualSnapStore) List(includeAll bool) (brtypes.SnapList, error) {
	var wg sync.WaitGroup
	primarySnapsCh := make(chan brtypes.SnapList, 1)
	secondarySnapsCh := make(chan brtypes.SnapList, 1)
	primaryErrCh := make(chan error, 1)
	secondaryErrCh := make(chan error, 1)

	// List from primary endpoint
	wg.Add(1)
	go func() {
		defer wg.Done()
		snaps, err := ds.primary.List(includeAll)
		if err != nil {
			if ds.isTransientError(err) {
				ds.logger.Warnf("Transient error listing snapshots from primary endpoint: %v", err)
			} else {
				ds.logger.Errorf("Failed to list snapshots from primary endpoint: %v", err)
			}
			primaryErrCh <- err
		} else {
			ds.logger.Debugf("Listed %d snapshots from primary endpoint", len(snaps))
			primarySnapsCh <- snaps
		}
	}()

	// List from secondary endpoint
	wg.Add(1)
	go func() {
		defer wg.Done()
		snaps, err := ds.secondary.List(includeAll)
		if err != nil {
			if ds.isTransientError(err) {
				ds.logger.Warnf("Transient error listing snapshots from secondary endpoint: %v", err)
			} else {
				ds.logger.Errorf("Failed to list snapshots from secondary endpoint: %v", err)
			}
			secondaryErrCh <- err
		} else {
			ds.logger.Debugf("Listed %d snapshots from secondary endpoint", len(snaps))
			secondarySnapsCh <- snaps
		}
	}()

	wg.Wait()
	close(primarySnapsCh)
	close(secondarySnapsCh)
	close(primaryErrCh)
	close(secondaryErrCh)

	// Collect results
	var primarySnaps, secondarySnaps brtypes.SnapList
	var primaryErr, secondaryErr error

	select {
	case primarySnaps = <-primarySnapsCh:
	case primaryErr = <-primaryErrCh:
	default:
	}

	select {
	case secondarySnaps = <-secondarySnapsCh:
	case secondaryErr = <-secondaryErrCh:
	default:
	}

	// Check if both errors are transient - if so, treat as fatal since we have no working endpoints
	if primaryErr != nil && secondaryErr != nil {
		if ds.isTransientError(primaryErr) && ds.isTransientError(secondaryErr) {
			ds.logger.Errorf("Both endpoints have transient errors, treating as fatal: Primary: %v, Secondary: %v", primaryErr, secondaryErr)
			return nil, fmt.Errorf("both endpoints temporarily unavailable - Primary: %v, Secondary: %v", primaryErr, secondaryErr)
		}
		// If at least one error is fatal, return the error
		return nil, fmt.Errorf("failed to list snapshots from both endpoints - Primary: %v, Secondary: %v",
			primaryErr, secondaryErr)
	}

	// Merge and deduplicate snapshots
	mergedSnaps := ds.mergeSnapshots(primarySnaps, secondarySnaps)

	ds.logger.Infof("Merged snapshot list: %d snapshots total (Primary: %d, Secondary: %d)",
		len(mergedSnaps), len(primarySnaps), len(secondarySnaps))

	return mergedSnaps, nil
}

// Delete deletes the snapshot from both endpoints.
func (ds *DualSnapStore) Delete(snap brtypes.Snapshot) error {
	var wg sync.WaitGroup
	results := make(chan brtypes.OperationResult, 2)

	// Delete from primary endpoint
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := ds.primary.Delete(snap)
		if err != nil {
			ds.logger.Errorf("Failed to delete snapshot %s from primary endpoint: %v", snap.SnapName, err)
			results <- brtypes.OperationResult{Endpoint: "primary", Success: false, Error: err}
		} else {
			ds.logger.Infof("Successfully deleted snapshot %s from primary endpoint", snap.SnapName)
			results <- brtypes.OperationResult{Endpoint: "primary", Success: true}
		}
	}()

	// Delete from secondary endpoint
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := ds.secondary.Delete(snap)
		if err != nil {
			ds.logger.Errorf("Failed to delete snapshot %s from secondary endpoint: %v", snap.SnapName, err)
			results <- brtypes.OperationResult{Endpoint: "secondary", Success: false, Error: err}
		} else {
			ds.logger.Infof("Successfully deleted snapshot %s from secondary endpoint", snap.SnapName)
			results <- brtypes.OperationResult{Endpoint: "secondary", Success: true}
		}
	}()

	wg.Wait()
	close(results)

	// Process results
	var primaryResult, secondaryResult brtypes.OperationResult
	for result := range results {
		if result.Endpoint == "primary" {
			primaryResult = result
		} else {
			secondaryResult = result
		}
	}

	// Log the overall result
	if primaryResult.Success && secondaryResult.Success {
		ds.logger.Infof("Snapshot %s successfully deleted from both endpoints", snap.SnapName)
		return nil
	} else if !primaryResult.Success && !secondaryResult.Success {
		return fmt.Errorf("failed to delete snapshot %s from both endpoints - Primary: %v, Secondary: %v",
			snap.SnapName, primaryResult.Error, secondaryResult.Error)
	} else {
		ds.logger.Warnf("Snapshot %s deleted from only one endpoint - Primary: %t, Secondary: %t",
			snap.SnapName, primaryResult.Success, secondaryResult.Success)
		return nil // Consider partial success as success for delete operations
	}
}

// mergeSnapshots merges and deduplicates snapshots from both endpoints.
// Priority is given to snapshots from the primary endpoint when duplicates exist.
func (ds *DualSnapStore) mergeSnapshots(primarySnaps, secondarySnaps brtypes.SnapList) brtypes.SnapList {
	snapMap := make(map[string]*brtypes.Snapshot)

	// Add primary snapshots first (they take priority)
	for _, snap := range primarySnaps {
		key := fmt.Sprintf("%s-%s", snap.SnapDir, snap.SnapName)
		snapMap[key] = snap
	}

	// Add secondary snapshots only if not already present
	for _, snap := range secondarySnaps {
		key := fmt.Sprintf("%s-%s", snap.SnapDir, snap.SnapName)
		if _, exists := snapMap[key]; !exists {
			snapMap[key] = snap
		}
	}

	// Convert map back to slice
	var mergedSnaps brtypes.SnapList
	for _, snap := range snapMap {
		mergedSnaps = append(mergedSnaps, snap)
	}

	// Sort the merged list
	sort.Sort(mergedSnaps)

	return mergedSnaps
}

// isTransientError checks if an error is transient (DNS, network issues) vs fatal
func (ds *DualSnapStore) isTransientError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()

	// DNS resolution failures
	if strings.Contains(errStr, "no such host") ||
		strings.Contains(errStr, "dial tcp: lookup") ||
		strings.Contains(errStr, "request send failed") {
		return true
	}

	// Network connectivity issues
	if strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "connection timeout") ||
		strings.Contains(errStr, "i/o timeout") {
		return true
	}

	// Check for specific network error types
	if netErr, ok := err.(net.Error); ok {
		return netErr.Timeout() || netErr.Temporary()
	}

	return false
}

// resetPrimaryFailureState resets the primary failure tracking
func (ds *DualSnapStore) resetPrimaryFailureState() {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.primaryFailed = false
	ds.primaryFailCount = 0
}

// isPrimaryDisabled returns true if primary endpoint should be skipped
func (ds *DualSnapStore) isPrimaryDisabled() bool {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return ds.primaryFailed && ds.primaryFailCount >= 3
}

// recordPrimaryFailure records a primary endpoint failure
func (ds *DualSnapStore) recordPrimaryFailure(err error) {
	if ds.isTransientError(err) {
		ds.mu.Lock()
		ds.primaryFailed = true
		ds.primaryFailCount++
		ds.mu.Unlock()
		ds.logger.Warnf("Primary endpoint failure recorded (count: %d): %v", ds.primaryFailCount, err)
	} else {
		// Reset failure count for non-transient errors
		ds.mu.Lock()
		ds.primaryFailCount = 0
		ds.mu.Unlock()
	}
}
