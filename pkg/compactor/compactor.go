package compactor

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/compressor"
	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	"github.com/gardener/etcd-backup-restore/pkg/health/heartbeat"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/restorer"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

	"github.com/sirupsen/logrus"

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
	logger          *logrus.Entry
	store           brtypes.SnapStore
	k8sClientset    client.Client
	snapstoreConfig *brtypes.SnapstoreConfig
}

// NewCompactor creates compactor
func NewCompactor(store brtypes.SnapStore, storeConfig *brtypes.SnapstoreConfig, logger *logrus.Entry, clientSet client.Client) *Compactor {
	return &Compactor{
		logger:          logger,
		store:           store,
		k8sClientset:    clientSet,
		snapstoreConfig: storeConfig,
	}
}

// Compact is mainly responsible for applying snapshots (full + delta), compacting, drefragmenting, taking the snapshot and saving it sequentially.
func (cp *Compactor) Compact(ctx context.Context, opts *brtypes.CompactOptions) (*brtypes.Snapshot, error) {
	cp.logger.Info("Start compacting")

	// Deepcopy restoration options ro to avoid any mutation of the passing object
	cmpctOptions := opts.RestoreOptions.DeepCopy()

	// If no basesnapshot is found, abort compaction as there would be nothing to compact
	if cmpctOptions.BaseSnapshot == nil {
		cp.logger.Error("No base snapshot found. Nothing is available for compaction")
		return nil, fmt.Errorf("no base snapshot found. Nothing is available for compaction")
	}

	// Set a temporary etcd data directory for embedded etcd
	prefix := cmpctOptions.Config.RestoreDataDir
	if prefix == "" {
		prefix = "/tmp"
	}
	cmpctDir, err := ioutil.TempDir(prefix, "compactor-")
	if err != nil {
		cp.logger.Errorf("Unable to create temporary etcd directory for compaction: %s", err.Error())
		return nil, err
	}

	defer os.RemoveAll(cmpctDir)

	cmpctOptions.Config.RestoreDataDir = cmpctDir

	// Update the snapstore object before taking any action on object storage bucket.
	// Refer: https://github.com/gardener/etcd-backup-restore/issues/422
	cp.store, err = snapstore.GetSnapstore(cp.snapstoreConfig)
	if err != nil {
		cp.logger.Errorf("unable to update snapstore object: %v", err)
		return nil, err
	}

	// Then restore from the snapshots
	r := restorer.NewRestorer(cp.store, cp.logger)
	embeddedEtcd, err := r.Restore(*cmpctOptions)
	if err != nil {
		return nil, fmt.Errorf("unable to restore snapshots during compaction: %v", err)
	}

	cp.logger.Info("Restoration for compaction is over")
	// There is a possibility that restore operation may not start an embedded ETCD.
	if embeddedEtcd == nil {
		embeddedEtcd, err = miscellaneous.StartEmbeddedEtcd(cp.logger, cmpctOptions)
		if err != nil {
			return nil, err
		}
	}

	defer func() {
		embeddedEtcd.Server.Stop()
		embeddedEtcd.Close()
	}()

	ep := []string{embeddedEtcd.Clients[0].Addr().String()}

	// Then compact ETCD

	// Build Client
	clientFactory := etcdutil.NewClientFactory(cmpctOptions.NewClientFactory, brtypes.EtcdConnectionConfig{
		MaxCallSendMsgSize: cmpctOptions.Config.MaxCallSendMsgSize,
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
	if _, err := clientKV.Compact(ctx, etcdRevision); err != nil {
		return nil, fmt.Errorf("failed to compact: %v", err)
	}

	// Then defrag the ETCD
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

	// Then take snapeshot of ETCD
	snapshotReqCtx, cancel := context.WithTimeout(ctx, opts.SnapshotTimeout.Duration)
	defer cancel()

	// Determine suffix of compacted snapshot that will be result of this compaction
	suffix := cmpctOptions.BaseSnapshot.CompressionSuffix
	if len(cmpctOptions.DeltaSnapList) > 0 {
		suffix = cmpctOptions.DeltaSnapList[cmpctOptions.DeltaSnapList.Len()-1].CompressionSuffix
	}

	isCompressed, compressionPolicy, err := compressor.IsSnapshotCompressed(suffix)
	if err != nil {
		return nil, fmt.Errorf("unable to determine if snapshot is compressed: %v", cmpctOptions.BaseSnapshot.CompressionSuffix)
	}

	isFinal := cmpctOptions.BaseSnapshot.IsFinal

	cc := &compressor.CompressionConfig{Enabled: isCompressed, CompressionPolicy: compressionPolicy}
	snapshot, err := etcdutil.TakeAndSaveFullSnapshot(snapshotReqCtx, clientMaintenance, cp.store, etcdRevision, cc, suffix, isFinal, cp.logger)
	if err != nil {
		return nil, err
	}

	// Update snapshot lease only if lease update flag is enabled
	if opts.EnabledLeaseRenewal {
		// Update revisions in holder identity of full snapshot lease.
		ctx, cancel := context.WithTimeout(ctx, brtypes.LeaseUpdateTimeoutDuration)
		if err := heartbeat.FullSnapshotCaseLeaseUpdate(ctx, cp.logger, snapshot, cp.k8sClientset, opts.FullSnapshotLeaseName, opts.DeltaSnapshotLeaseName); err != nil {
			cp.logger.Warnf("Snapshot lease update failed : %v", err)
		}
		cancel()
	}

	return snapshot, nil
}
