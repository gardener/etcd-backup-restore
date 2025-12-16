// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package compactor

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/compressor"
	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	"github.com/gardener/etcd-backup-restore/pkg/health/heartbeat"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/restorer"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	tmpDir                          = "/tmp"
	defaultName                     = "default"
	defaultInitialAdvertisePeerURLs = "http://localhost:2384"
	defaultInitialClusterToken      = "etcd-cluster"
	defaultMaxFetchers              = 6
	defaultMaxCallSendMsgSize       = 10 * 1024 * 1024 //10Mib
	defaultMaxRequestBytes          = 10 * 1024 * 1024 //10Mib
	defaultMaxTxnOps                = 10 * 1024
	defaultEmbeddedEtcdQuotaBytes   = 8 * 1024 * 1024 * 1024 //8Gib
	etcdDialTimeout                 = time.Second * 30
	etcdDir                         = tmpDir + "/compaction"
	restoreClusterToken             = "etcd-cluster"
)

// Compactor holds the necessary details for compacting ETCD
type Compactor struct {
	logger       *logrus.Entry
	store        brtypes.SnapStore
	k8sClientset client.Client
}

// NewCompactor creates compactor
func NewCompactor(store brtypes.SnapStore, logger *logrus.Entry, clientSet client.Client) *Compactor {
	return &Compactor{
		logger:       logger,
		store:        store,
		k8sClientset: clientSet,
	}
}

// Compact is mainly responsible for applying snapshots (full + delta), compacting, drefragmenting, taking the snapshot and saving it sequentially.
func (cp *Compactor) Compact(ctx context.Context, opts *brtypes.CompactOptions) (*brtypes.Snapshot, error) {
	cp.logger.Info("Start compacting")

	// Deepcopy restoration options ro to avoid any mutation of the passing object
	compactorRestoreOptions := opts.RestoreOptions.DeepCopy()

	// If no base snapshot is found, abort compaction as there would be nothing to compact
	if compactorRestoreOptions.BaseSnapshot == nil {
		cp.logger.Error("No base snapshot found. Nothing is available for compaction")
		return nil, fmt.Errorf("no base snapshot found. Nothing is available for compaction")
	}

	// Then restore from the snapshots
	r, err := restorer.NewRestorer(cp.store, cp.logger)
	if err != nil {
		return nil, err
	}
	embeddedEtcd, err := r.Restore(*compactorRestoreOptions, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to restore snapshots during compaction: %v", err)
	}

	defer func() {
		if err := os.RemoveAll(compactorRestoreOptions.Config.DataDir); err != nil {
			cp.logger.Errorf("Failed to remove temporary etcd directory %s: %v", compactorRestoreOptions.Config.DataDir, err)
		}
	}()

	cp.logger.Info("Restoration for compaction is done.")
	// There is a possibility that restore operation may not start an embedded ETCD.
	if embeddedEtcd == nil {
		embeddedEtcd, err = miscellaneous.StartEmbeddedEtcd(cp.logger, compactorRestoreOptions)
		if err != nil {
			return nil, err
		}
	}

	defer embeddedEtcd.Close()

	ep := []string{embeddedEtcd.Clients[0].Addr().String()}

	// Then compact ETCD

	// Build Client
	clientFactory := etcdutil.NewClientFactory(compactorRestoreOptions.NewClientFactory, brtypes.EtcdConnectionConfig{
		MaxCallSendMsgSize: compactorRestoreOptions.Config.MaxCallSendMsgSize,
		Endpoints:          ep,
		InsecureTransport:  true,
	})
	clientKV, err := clientFactory.NewKV()
	if err != nil {
		return nil, fmt.Errorf("failed to build etcd KV client")
	}
	defer clientKV.Close()

	clientMaintenance, err := clientFactory.NewMaintenance()
	if err != nil {
		return nil, fmt.Errorf("failed to build etcd maintenance client")
	}
	defer clientMaintenance.Close()

	revCheckCtx, cancel := context.WithTimeout(ctx, etcdDialTimeout)
	getResponse, err := clientKV.Get(revCheckCtx, "foo")
	cancel()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to etcd KV client: %v", err)
	}
	etcdRevision := getResponse.Header.GetRevision()

	// Compact
	// Please refer below issue for why physical compaction was necessary
	// https://github.com/gardener/etcd-backup-restore/issues/451
	if _, err := clientKV.Compact(ctx, etcdRevision, clientv3.WithCompactPhysical()); err != nil {
		return nil, fmt.Errorf("failed to compact: %v", err)
	}

	// Then defrag ETCD
	if opts.NeedDefragmentation {
		client, err := clientFactory.NewCluster()
		if err != nil {
			return nil, fmt.Errorf("failed to build etcd cluster client")
		}
		defer client.Close()

		err = etcdutil.DefragmentData(ctx, clientMaintenance, client, ep, opts.DefragTimeout.Duration, cp.logger)
		if err != nil {
			cp.logger.Errorf("failed to defragment: %v", err)
		}
	}

	// Then take snapshot of ETCD
	snapshotReqCtx, cancel := context.WithTimeout(ctx, opts.SnapshotTimeout.Duration)
	defer cancel()

	// Determine suffix of compacted snapshot that will be result of this compaction
	suffix := compactorRestoreOptions.BaseSnapshot.CompressionSuffix
	if len(compactorRestoreOptions.DeltaSnapList) > 0 {
		suffix = compactorRestoreOptions.DeltaSnapList[compactorRestoreOptions.DeltaSnapList.Len()-1].CompressionSuffix
	}

	isCompressed, compressionPolicy, err := compressor.IsSnapshotCompressed(suffix)
	if err != nil {
		return nil, fmt.Errorf("unable to determine if snapshot is compressed: %v", compactorRestoreOptions.BaseSnapshot.CompressionSuffix)
	}

	isFinal := compactorRestoreOptions.BaseSnapshot.IsFinal

	cc := &compressor.CompressionConfig{Enabled: isCompressed, CompressionPolicy: compressionPolicy}
	snapshot, err := etcdutil.TakeAndSaveFullSnapshot(snapshotReqCtx, clientMaintenance, cp.store, opts.TempDir, etcdRevision, cc, suffix, isFinal, cp.logger)
	if err != nil {
		return nil, err
	}

	// Update snapshot lease only if lease update flag is enabled
	if opts.EnabledLeaseRenewal {
		// Update revisions in holder identity of full snapshot lease.
		ctx, cancel := context.WithTimeout(ctx, brtypes.LeaseUpdateTimeoutDuration)
		if err := heartbeat.FullSnapshotCaseLeaseUpdate(ctx, cp.logger, snapshot, cp.k8sClientset, opts.FullSnapshotLeaseName, snapshot.CreatedOn); err != nil {
			cp.logger.Warnf("Snapshot lease update failed : %v", err)
		}
		cancel()
	}

	// Add a sleep command so that prometheus can collect necessary metrics related to the uploading of snapshots. see https://github.com/gardener/etcd-druid/issues/648
	err = sleepWithContext(ctx, opts.MetricsScrapeWaitDuration.Duration)
	if err != nil {
		cp.logger.Warnf("Could not sleep for specified duration: %v", err)
	}

	return snapshot, nil
}

func sleepWithContext(ctx context.Context, sleepFor time.Duration) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleepFor):
			return nil
		}
	}
}
