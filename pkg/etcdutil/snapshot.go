package etcdutil

import (
	"context"
	"fmt"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/compressor"
	"github.com/gardener/etcd-backup-restore/pkg/errors"
	"github.com/gardener/etcd-backup-restore/pkg/etcdaccess"
	"github.com/gardener/etcd-backup-restore/pkg/metrics"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

// TakeAndSaveFullSnapshot takes full snapshot and save it to store
func TakeAndSaveFullSnapshot(ctx context.Context, client etcdaccess.MaintenanceCloser, store brtypes.SnapStore, lastRevision int64, cc *compressor.CompressionConfig, suffix string, isFinal bool, logger *logrus.Entry) (*brtypes.Snapshot, error) {
	startTime := time.Now()
	rc, err := client.Snapshot(ctx)
	if err != nil {
		return nil, &errors.EtcdError{
			Message: fmt.Sprintf("failed to create etcd snapshot: %v", err),
		}
	}
	timeTaken := time.Since(startTime)
	logger.Infof("Total time taken by Snapshot API: %f seconds.", timeTaken.Seconds())

	if cc.Enabled {
		startTimeCompression := time.Now()
		rc, err = compressor.CompressSnapshot(rc, cc.CompressionPolicy)
		if err != nil {
			return nil, fmt.Errorf("unable to obtain reader for compressed file: %v", err)
		}
		timeTakenCompression := time.Since(startTimeCompression)
		logger.Infof("Total time taken in full snapshot compression: %f seconds.", timeTakenCompression.Seconds())
	}
	defer func() {
		if err = rc.Close(); err != nil {
			logger.Errorf("Failed to close snapshot reader: %v", err)
		}
	}()

	logger.Infof("Successfully opened snapshot reader on etcd")

	// Then save the snapshot to the store.
	snapshot := snapstore.NewSnapshot(brtypes.SnapshotKindFull, 0, lastRevision, suffix, isFinal)
	if err := store.Save(*snapshot, rc); err != nil {
		timeTaken := time.Since(startTime)
		metrics.SnapshotDurationSeconds.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindFull, metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Observe(timeTaken.Seconds())
		return nil, &errors.SnapstoreError{
			Message: fmt.Sprintf("failed to save snapshot: %v", err),
		}
	}

	timeTaken = time.Since(startTime)
	metrics.SnapshotDurationSeconds.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindFull, metrics.LabelSucceeded: metrics.ValueSucceededTrue}).Observe(timeTaken.Seconds())
	logger.Infof("Total time to save full snapshot: %f seconds.", timeTaken.Seconds())

	return snapshot, nil
}
