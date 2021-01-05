package compactor

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/compressor"
	"github.com/gardener/etcd-backup-restore/pkg/metrics"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/restorer"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
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
	logger *logrus.Entry
	store  snapstore.SnapStore
}

// NewCompactor creates compactor
func NewCompactor(store snapstore.SnapStore, logger *logrus.Entry) *Compactor {
	return &Compactor{
		logger: logger,
		store:  store,
	}
}

// Compact is mainly responsible for applying snapshots (full + delta), compacting, drefragmenting, taking the snapshot and saving it sequentially.
func (cp *Compactor) Compact(ro *brtypes.RestoreOptions, needDefragmentation bool) (*brtypes.CompactionResult, error) {
	cp.logger.Info("Start compacting")

	// If no basesnapshot is found, abort compaction as there would be nothing to compact
	if ro.BaseSnapshot == nil {
		cp.logger.Error("No base snapshot found. Nothing is available for compaction")
		return nil, fmt.Errorf("No base snapshot found. Nothing is available for compaction")
	}

	// Set suffix of compacted snapshot that will be result of this compaction
	suffix := ro.BaseSnapshot.CompressionSuffix
	if len(ro.DeltaSnapList) > 0 {
		suffix = ro.DeltaSnapList[ro.DeltaSnapList.Len()-1].CompressionSuffix
	}

	// TODO: Remove this provision of saving the compacted snapshot in current directory when we change directory structure
	cwd, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("Unable to create file to save compacted snapshot %v", err)
	}

	cmpctdFileName := filepath.Join(cwd, filepath.Base(fmt.Sprintf("%s-%d%s", "compacted", time.Now().UTC().Unix(), suffix)))
	cmpctdFile, err := os.Create(cmpctdFileName)
	defer cmpctdFile.Close()
	cp.logger.Infof("Created compacted snapshot in: %s", cmpctdFileName)
	// Remove till here

	// Set a temporary etcd data directory for embedded etcd
	cmpctDir, err := ioutil.TempDir("/tmp", "compactor")
	if err != nil {
		cp.logger.Errorf("Unable to create temporary etcd directory for compaction: %s", err.Error())
		return nil, err
	}

	defer os.RemoveAll(cmpctDir)

	ro.Config.RestoreDataDir = cmpctDir

	var client *clientv3.Client
	var ep []string

	// Then restore from the base snapshot
	r := restorer.NewRestorer(cp.store, cp.logger)
	/*if err := r.Restore(*ro); err != nil {
		return nil, fmt.Errorf("Unable to restore snapshots during compaction: %v", err)
	}*/

	if err := r.RestoreFromBaseSnapshot(*ro); err != nil {
		return nil, fmt.Errorf("Failed to restore from the base snapshot :%v", err)
	}

	cp.logger.Infof("Starting embedded etcd server for compaction...")
	e, err := miscellaneous.StartEmbeddedEtcd(cp.logger, ro)
	if err != nil {
		return nil, err
	}
	defer func() {
		e.Server.Stop()
		e.Close()
	}()

	ep = []string{e.Clients[0].Addr().String()}
	cfg := clientv3.Config{MaxCallSendMsgSize: ro.Config.MaxCallSendMsgSize, Endpoints: ep}
	client, err = clientv3.New(cfg)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	if len(ro.DeltaSnapList) > 0 {
		cp.logger.Infof("Applying delta snapshots...")
		if err := r.ApplyDeltaSnapshots(client, *ro); err != nil {
			cp.logger.Warnf("Could not apply the delta snapshots: %v", err)
		}
	} else {
		cp.logger.Infof("No delta snapshots present over base snapshot.")
	}

	// Then compact ETCD
	ctx := context.TODO()
	revCheckCtx, cancel := context.WithTimeout(ctx, etcdDialTimeout)
	getResponse, err := client.Get(revCheckCtx, "foo")
	cancel()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to client: %v", err)
	}
	etcdRevision := getResponse.Header.GetRevision()

	client.Compact(ctx, etcdRevision)

	// Then defrag the ETCD
	if needDefragmentation {
		var dbSizeBeforeDefrag, dbSizeAfterDefrag int64
		statusReqCtx, cancel := context.WithTimeout(ctx, etcdDialTimeout)
		status, err := client.Status(statusReqCtx, ep[0])
		cancel()
		if err != nil {
			cp.logger.Warnf("Failed to get status of etcd member[%s] with error: %v", ep, err)
		} else {
			dbSizeBeforeDefrag = status.DbSize
		}

		start := time.Now()
		defragCtx, cancel := context.WithTimeout(ctx, time.Duration(60*time.Second))
		_, err = client.Defragment(defragCtx, ep[0])
		cancel()
		if err != nil {
			cp.logger.Errorf("Total time taken to defragment: %v", time.Now().Sub(start).Seconds())
			cp.logger.Errorf("Failed to defragment etcd member[%s] with error: %v", ep, err)
		}
		cp.logger.Infof("Total time taken to defragment: %v", time.Now().Sub(start).Seconds())
		cp.logger.Infof("Finished defragmenting etcd member[%s]", ep)

		statusReqCtx, cancel = context.WithTimeout(ctx, etcdDialTimeout)
		status, err = client.Status(statusReqCtx, ep[0])
		cancel()
		if err != nil {
			cp.logger.Warnf("Failed to get status of etcd member[%s] with error: %v", ep, err)
		} else {
			dbSizeAfterDefrag = status.DbSize
			cp.logger.Infof("Probable DB size change for etcd member [%s]:  %dB -> %dB after defragmentation", ep, dbSizeBeforeDefrag, dbSizeAfterDefrag)
		}
	}

	// Then take snapeshot of ETCD
	snapshotReqCtx, cancel := context.WithTimeout(ctx, etcdDialTimeout)
	defer cancel()

	rc, err := client.Snapshot(snapshotReqCtx)
	if err != nil {
		return nil, fmt.Errorf("Failed to create etcd snapshot out of compacted DB: %v", err)
	}

	isCompressed, compressionPolicy, err := compressor.IsSnapshotCompressed(suffix)
	if err != nil {
		return nil, fmt.Errorf("Unable to determine if snapshot is compressed: %v", ro.BaseSnapshot.CompressionSuffix)
	}
	if isCompressed {
		rc, err = compressor.CompressSnapshot(rc, compressionPolicy)
		if err != nil {
			return nil, fmt.Errorf("Unable to obtain reader for compressed file: %v", err)
		}
	}
	defer rc.Close()

	cp.logger.Infof("Successfully opened snapshot reader on etcd")

	// TODO: Save the snapshots to store when we change the directory structure
	// Then save the snapshot to the store.
	//s := snapstore.NewSnapshot(snapstore.SnapshotKindFull, 0, etcdRevision, ro.BaseSnapshot.CompressionSuffix)
	startTime := time.Now()
	/*if err := cp.store.Save(*s, rc); err != nil {
		timeTaken := time.Now().Sub(startTime).Seconds()
		metrics.SnapshotDurationSeconds.With(prometheus.Labels{metrics.LabelKind: snapstore.SnapshotKindFull, metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Observe(timeTaken)
		return nil, fmt.Errorf("failed to save snapshot: %v", err)
	}*/

	// TODO: Remove this copy when directory structure is changed
	if _, err = io.Copy(cmpctdFile, rc); err != nil {
		return nil, fmt.Errorf("Unable to create compacted snapshot: %v", err)
	}

	timeTaken := time.Now().Sub(startTime).Seconds()
	metrics.SnapshotDurationSeconds.With(prometheus.Labels{metrics.LabelKind: snapstore.SnapshotKindFull, metrics.LabelSucceeded: metrics.ValueSucceededTrue}).Observe(timeTaken)
	cp.logger.Infof("Total time to save snapshot: %f seconds.", timeTaken)

	compactionDuration, err := time.ParseDuration(fmt.Sprintf("%fs", timeTaken))
	if err != nil {
		cp.logger.Warnf("Could not record compaction duration: %v", err)
	}
	return &brtypes.CompactionResult{
		//Snapshot:               s,
		Path:                   cmpctdFileName,
		LastCompactionDuration: compactionDuration,
	}, nil
}
