// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sirupsen/logrus"
)

// ResilientS3SnapStore wraps S3SnapStore with resilient error handling
type ResilientS3SnapStore struct {
	*S3SnapStore
	logger *logrus.Entry
}

// NewResilientS3SnapStore creates a new ResilientS3SnapStore
func NewResilientS3SnapStore(config *brtypes.SnapstoreConfig) (*ResilientS3SnapStore, error) {
	s3Store, err := NewS3SnapStore(config)
	if err != nil {
		return nil, err
	}

	return &ResilientS3SnapStore{
		S3SnapStore: s3Store,
		logger:      logrus.NewEntry(logrus.New()).WithField("actor", "resilient-s3-snapstore"),
	}, nil
}

// IsTransientError checks if an error is transient (DNS, network issues) vs fatal
func (r *ResilientS3SnapStore) IsTransientError(err error) bool {
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

// List with resilient error handling
func (r *ResilientS3SnapStore) List(includeAll bool) (brtypes.SnapList, error) {
	snapList, err := r.S3SnapStore.List(includeAll)

	if err != nil && r.IsTransientError(err) {
		r.logger.Warnf("Transient error during List operation: %v", err)
		// For transient errors during List, we can return empty list and let dual store handle it
		return brtypes.SnapList{}, nil
	}

	return snapList, err
}

// Fetch with resilient error handling
func (r *ResilientS3SnapStore) Fetch(snap brtypes.Snapshot) (io.ReadCloser, error) {
	rc, err := r.S3SnapStore.Fetch(snap)

	if err != nil && r.IsTransientError(err) {
		r.logger.Warnf("Transient error during Fetch operation for snapshot %s: %v", snap.SnapName, err)
		return nil, err // Let dual store try the other endpoint
	}

	return rc, err
}

// Save with resilient error handling
func (r *ResilientS3SnapStore) Save(snap brtypes.Snapshot, rc io.ReadCloser) error {
	err := r.S3SnapStore.Save(snap, rc)

	if err != nil && r.IsTransientError(err) {
		r.logger.Warnf("Transient error during Save operation for snapshot %s: %v", snap.SnapName, err)
		return err // Let dual store try the other endpoint
	}

	return err
}

// Delete with resilient error handling
func (r *ResilientS3SnapStore) Delete(snap brtypes.Snapshot) error {
	err := r.S3SnapStore.Delete(snap)

	if err != nil && r.IsTransientError(err) {
		r.logger.Warnf("Transient error during Delete operation for snapshot %s: %v", snap.SnapName, err)
		return err // Let dual store try the other endpoint
	}

	return err
}

// HealthCheck performs a lightweight health check on the S3 endpoint
func (r *ResilientS3SnapStore) HealthCheck() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Try a simple operation like checking bucket versioning
	_, err := r.client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{
		Bucket: &r.bucket,
	})

	if err != nil {
		if r.IsTransientError(err) {
			return fmt.Errorf("transient error during health check: %v", err)
		}
		return fmt.Errorf("fatal error during health check: %v", err)
	}

	return nil
}
