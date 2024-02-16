// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package validator

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	"github.com/gardener/etcd-backup-restore/pkg/etcdutil/client"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/sirupsen/logrus"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal"
	"go.etcd.io/etcd/wal/walpb"
	"go.uber.org/zap"
)

const (
	defaultMaxRequestBytes = 10 * 1024 * 1024 //10Mib
	defaultMaxTxnOps       = 10 * 1024
)

var (
	// ErrPathRequired is returned when the path to a Bolt database is not specified.
	ErrPathRequired = errors.New("path required")

	// ErrFileNotFound is returned when a Bolt database does not exist.
	ErrFileNotFound = errors.New("file not found")

	// ErrCorrupt is returned when a checking a data file finds errors.
	ErrCorrupt = errors.New("invalid value")

	// isBoltDBPanic is a flag which indicates the whether the panic occurs while opening the Bolt database.
	isBoltDBPanic = false
)

func (d *DataValidator) memberDir() string { return filepath.Join(d.Config.DataDir, "member") }

func (d *DataValidator) walDir() string { return filepath.Join(d.memberDir(), "wal") }

func (d *DataValidator) snapDir() string { return filepath.Join(d.memberDir(), "snap") }

func (d *DataValidator) backendPath() string { return filepath.Join(d.snapDir(), "db") }

// Validate performs the steps required to validate data for Etcd instance.
func (d *DataValidator) Validate(mode Mode, failBelowRevision int64) (DataDirStatus, error) {
	status, err := d.sanityCheck(failBelowRevision)
	if status != DataDirectoryValid {
		return d.checkStatus(status, err)
	}

	if mode == Full {
		d.Logger.Info("Checking for data directory files corruption...")
		if err := d.checkForDataCorruption(); err != nil {
			if errors.Is(err, bolt.ErrTimeout) {
				d.Logger.Errorf("another etcd process is using %v and holds the file lock", d.backendPath())
				return FailToOpenBoltDBError, err
			}
			d.Logger.Infof("Data directory corrupt. %v", err)
			return DataDirectoryCorrupt, nil
		}
	}

	d.Logger.Info("Data directory valid.")
	return DataDirectoryValid, nil
}

func (d *DataValidator) sanityCheck(failBelowRevision int64) (DataDirStatus, error) {
	mntDataDir := path.Dir(d.Config.DataDir)
	path := mntDataDir + "/" + safeGuard
	namespace := os.Getenv(podNamespace)
	if namespace == "" {
		d.Logger.Warn("POD_NAMESPACE environment variable is not set. The variable is used to safe guard against wrong volume mount")
	} else {
		// create the file `safe_guard` if it doesn't exist
		if _, err := os.Stat(path); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				data := []byte(namespace)
				err := os.WriteFile(path, data, 0600)
				if err != nil {
					d.Logger.Fatalf("can't create `safe_guard` file because : %v", err)
				}
			} else {
				d.Logger.Fatalf("can't check if the `safe_guard` file exists or not because : %v", err)
			}
		}

		// read the content of the file safe_guard and match it with the environment variable
		content, err := os.ReadFile(path)
		if err != nil {
			return WrongVolumeMounted, fmt.Errorf("can't read the content of the `safe_guard` file to determine if a wrong volume is mounted: %v", err)
		}
		if string(content) != namespace {
			return WrongVolumeMounted, fmt.Errorf("wrong volume is mounted. The shoot name derived from namespace is %s and the content of `safe_guard` file at %s is %s", namespace, path, string(content))
		}
	}

	dataDir := d.Config.DataDir
	dirExists, err := directoryExist(dataDir)
	if err != nil {
		return DataDirectoryStatusUnknown, err
	}
	if !dirExists {
		return DataDirectoryNotExist, fmt.Errorf("directory does not exist: %s", dataDir)
	}

	d.Logger.Info("Checking for data directory structure validity...")
	etcdDirStructValid, err := d.hasEtcdDirectoryStructure()
	if err != nil {
		return DataDirectoryStatusUnknown, err
	}
	if !etcdDirStructValid {
		d.Logger.Infof("Data directory structure invalid.")
		return DataDirectoryInvStruct, nil
	}

	if d.Config.SnapstoreConfig == nil || len(d.Config.SnapstoreConfig.Provider) == 0 {
		d.Logger.Info("Skipping check for revision consistency, since no snapstore configured.")
		return DataDirectoryValid, nil
	}

	d.Logger.Info("Checking for Etcd revision...")
	etcdRevision, err := getLatestEtcdRevision(d.backendPath())
	if err != nil && errors.Is(err, bolt.ErrTimeout) {
		d.Logger.Errorf("another etcd process is using %v and holds the file lock", d.backendPath())
		return FailToOpenBoltDBError, err
	} else if err != nil {
		d.Logger.Infof("unable to get current etcd revision from backend db file: %v", err)
		return DataDirectoryCorrupt, nil
	}

	if isBoltDBPanic {
		d.Logger.Info("Bolt database panic: database file found to be invalid.")
		// reset the isBoltDBPanic
		isBoltDBPanic = false
		return BoltDBCorrupt, nil
	}

	if d.OriginalClusterSize > 1 {
		d.Logger.Info("Skipping check for revision consistency of etcd member as it will get in sync with etcd leader.")
		return DataDirectoryValid, nil
	}

	d.Logger.Info("Checking for etcd revision consistency...")
	etcdRevisionStatus, latestSnapshotRevision, err := d.checkEtcdDataRevisionConsistency(etcdRevision, failBelowRevision)

	// if etcd revision is inconsistent with latest snapshot revision then
	//   check the etcd revision consistency by starting an embedded etcd since the WALs file can have uncommited data which it was unable to flush to Bolt DB
	if etcdRevisionStatus == RevisionConsistencyError {
		d.Logger.Info("Checking for Full revision consistency...")
		fullRevisionConsistencyStatus, err := d.checkFullRevisionConsistency(dataDir, latestSnapshotRevision)
		return fullRevisionConsistencyStatus, err
	}

	return etcdRevisionStatus, err
}

// checkStatus checks/filter the status of sanity check.
func (d *DataValidator) checkStatus(status DataDirStatus, err error) (DataDirStatus, error) {
	if status == WrongVolumeMounted || status == FailToOpenBoltDBError {
		return status, err
	}

	if d.OriginalClusterSize > 1 && !miscellaneous.IsBackupBucketEmpty(d.Config.SnapstoreConfig, d.Logger) {
		return DataDirStatusInvalidInMultiNode, nil
	}

	return status, err
}

// checkForDataCorruption will check for corruption of different files used by etcd.
func (d *DataValidator) checkForDataCorruption() error {
	var walsnap walpb.Snapshot
	d.Logger.Info("Verifying snap directory...")
	snapshot, err := d.verifySnapDir()
	if err != nil && err != snap.ErrNoSnapshot {
		return fmt.Errorf("invalid snapshot files: %v", err)
	}
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}

	d.Logger.Info("Verifying WAL directory...")
	if err := verifyWALDir(d.ZapLogger, d.walDir(), walsnap); err != nil {
		return fmt.Errorf("invalid wal files: %v", err)
	}

	d.Logger.Info("Verifying DB file...")
	if err := verifyDB(d.backendPath()); err != nil {
		if errors.Is(err, bolt.ErrTimeout) {
			return err
		}
		return fmt.Errorf("invalid db files: %v", err)
	}
	if isBoltDBPanic {
		d.Logger.Info("Bolt database panic: database file found to be invalid.")
		// reset the isBoltDBPanic
		isBoltDBPanic = false
		return fmt.Errorf("invalid db files")
	}
	return nil
}

// hasEtcdDirectoryStructure checks for existence of the required sub-directories.
func (d *DataValidator) hasEtcdDirectoryStructure() (bool, error) {
	var memberExist, snapExist, walExist bool
	var err error
	if memberExist, err = directoryExist(d.memberDir()); err != nil {
		return false, err
	}
	if snapExist, err = directoryExist(d.snapDir()); err != nil {
		return false, err
	}
	if walExist, err = directoryExist(d.walDir()); err != nil {
		return false, err
	}
	return memberExist && snapExist && walExist, nil
}

func directoryExist(dir string) (bool, error) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	} else {
		return true, nil
	}
}

func (d *DataValidator) verifySnapDir() (*raftpb.Snapshot, error) {
	ssr := snap.New(d.ZapLogger, d.snapDir())
	return ssr.Load()
}

func verifyWALDir(logger *zap.Logger, waldir string, snap walpb.Snapshot) error {
	var err error

	repaired := false
	for {
		if err = wal.Verify(logger, waldir, snap); err != nil {
			// we can only repair ErrUnexpectedEOF and we never repair twice.
			if repaired || err != io.ErrUnexpectedEOF {
				fmt.Printf("read wal error (%v) and cannot be repaired.\n", err)
				return err
			}
			if !wal.Repair(logger, waldir) {
				fmt.Printf("WAL error (%v) cannot be repaired.\n", err)
				return err
			}
			fmt.Printf("repaired WAL error (%v).\n", err)
			repaired = true

			continue
		}
		break
	}
	return err
}

func verifyDB(path string) error {

	if path == "" {
		return ErrPathRequired
	} else if _, err := os.Stat(path); os.IsNotExist(err) {
		return ErrFileNotFound
	}

	defer func() {
		if err := recover(); err != nil {
			// set the flag: isBoltDBPanic
			isBoltDBPanic = true
		}
	}()

	// Open database.
	db, err := bolt.Open(path, 0666, &bolt.Options{Timeout: timeoutToOpenBoltDB})
	if err != nil {
		return err
	}
	defer db.Close()

	// Perform consistency check.
	return db.View(func(tx *bolt.Tx) error {
		var count int
		for range tx.Check() {
			count++
		}

		// Print summary of errors.
		if count > 0 {
			return ErrCorrupt
		}

		// Notify user that database is valid.
		return nil
	})
}

// checkEtcdDataRevisionConsistency compares the latest revision of the etcd db file and the latest snapshot revision to verify that the etcd revision is not lesser than snapshot revision.
// Return DataDirStatus indicating whether it is due to failBelowRevision or latest snapshot revision for snapstore and also return the latest snapshot revision.
func (d *DataValidator) checkEtcdDataRevisionConsistency(etcdRevision, failBelowRevision int64) (DataDirStatus, int64, error) {
	var latestSnapshotRevision int64
	latestSnapshotRevision = 0

	store, err := snapstore.GetSnapstore(d.Config.SnapstoreConfig)
	if err != nil {
		return DataDirectoryStatusUnknown, latestSnapshotRevision, fmt.Errorf("unable to fetch snapstore: %v", err)
	}

	fullSnap, deltaSnaps, err := miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
	if err != nil {
		return DataDirectoryStatusUnknown, latestSnapshotRevision, fmt.Errorf("unable to get snapshots from store: %v", err)
	}
	if len(deltaSnaps) != 0 {
		latestSnapshotRevision = deltaSnaps[len(deltaSnaps)-1].LastRevision
	} else if fullSnap != nil {
		latestSnapshotRevision = fullSnap.LastRevision
	} else {
		d.Logger.Infof("No snapshot found.")
		if etcdRevision < failBelowRevision {
			d.Logger.Infof("current etcd revision (%d) is less than fail below revision (%d): possible data loss", etcdRevision, failBelowRevision)
			return FailBelowRevisionConsistencyError, latestSnapshotRevision, nil
		}
		return DataDirectoryValid, latestSnapshotRevision, nil
	}

	if etcdRevision < latestSnapshotRevision {
		d.Logger.Infof("current etcd revision (%d) is less than latest snapshot revision (%d)", etcdRevision, latestSnapshotRevision)
		return RevisionConsistencyError, latestSnapshotRevision, nil
	}

	return DataDirectoryValid, latestSnapshotRevision, nil
}

// checkFullRevisionConsistency starts an embedded etcd and then compares the latest revision of etcd db file and the latest snapshot revision to verify that the etcd revision is not lesser than snapshot revision.
// Return DataDirStatus indicating whether WALs file have uncommited data which it was unable to flush to DB or latest DB revision is still less than snapshot revision.
func (d *DataValidator) checkFullRevisionConsistency(dataDir string, latestSnapshotRevision int64) (DataDirStatus, error) {
	var latestSyncedEtcdRevision int64

	d.Logger.Info("Starting embedded etcd server...")
	ro := &brtypes.RestoreOptions{
		Config: &brtypes.RestorationConfig{
			DataDir:                dataDir,
			EmbeddedEtcdQuotaBytes: d.Config.EmbeddedEtcdQuotaBytes,
			MaxRequestBytes:        defaultMaxRequestBytes,
			MaxTxnOps:              defaultMaxTxnOps,
		},
	}
	e, err := miscellaneous.StartEmbeddedEtcd(logrus.NewEntry(d.Logger), ro)
	if err != nil {
		d.Logger.Infof("unable to start embedded etcd: %v", err)
		return DataDirectoryCorrupt, err
	}
	defer func() {
		e.Server.Stop()
		e.Close()
	}()

	clientFactory := etcdutil.NewClientFactory(nil, brtypes.EtcdConnectionConfig{
		Endpoints:         []string{e.Clients[0].Addr().String()},
		InsecureTransport: true,
	})
	clientKV, err := clientFactory.NewKV()
	if err != nil {
		d.Logger.Infof("unable to get the embedded etcd KV client: %v", err)
		return DataDirectoryCorrupt, err
	}
	defer clientKV.Close()

	timer := time.NewTimer(embeddedEtcdPingLimitDuration)

waitLoop:
	for {
		select {
		case <-timer.C:
			break waitLoop
		default:
			latestSyncedEtcdRevision, err = getLatestSyncedRevision(clientKV, d.Logger)
			if err == nil && latestSyncedEtcdRevision >= latestSnapshotRevision {
				d.Logger.Infof("After starting embeddedEtcd backend DB file revision (%d) is greater than or equal to latest snapshot revision (%d): no data loss", latestSyncedEtcdRevision, latestSnapshotRevision)
				break waitLoop
			}
			time.Sleep(1 * time.Second)
		}
	}
	defer timer.Stop()

	if latestSyncedEtcdRevision < latestSnapshotRevision {
		d.Logger.Infof("After starting embeddedEtcd backend DB file revision (%d) is less than  latest snapshot revision (%d): possible data loss", latestSyncedEtcdRevision, latestSnapshotRevision)
		return RevisionConsistencyError, nil
	}

	return DataDirectoryValid, nil
}

// getLatestEtcdRevision finds out the latest revision on the etcd db file without starting etcd server or an embedded etcd server.
func getLatestEtcdRevision(path string) (int64, error) {
	if _, err := os.Stat(path); err != nil {
		return -1, fmt.Errorf("unable to stat backend db file: %v", err)
	}

	defer func() {
		if err := recover(); err != nil {
			// set the flag: isBoltDBPanic
			isBoltDBPanic = true
		}
	}()

	db, err := bolt.Open(path, 0400, &bolt.Options{Timeout: timeoutToOpenBoltDB, ReadOnly: true})
	if err != nil {
		return -1, err
	}
	defer db.Close()

	var rev int64

	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("key"))
		if b == nil {
			return fmt.Errorf("cannot get hash of bucket \"key\"")
		}

		c := b.Cursor()
		k, _ := c.Last()
		if len(k) < 8 {
			rev = 1
			return nil
		}
		rev = int64(binary.BigEndian.Uint64(k[0:8]))

		return nil
	})

	if err != nil {
		return -1, err
	}

	return rev, nil
}

// getLatestSyncedRevision finds out the latest revision on etcd db file when embedded etcd is started to double check the latest revision of etcd db file.
func getLatestSyncedRevision(client client.KVCloser, logger *logrus.Logger) (int64, error) {
	var latestSyncedRevision int64

	ctx, cancel := context.WithTimeout(context.TODO(), connectionTimeout)
	defer cancel()
	resp, err := client.Get(ctx, "", clientv3.WithLastRev()...)
	if err != nil {
		logger.Errorf("failed to get the latest etcd revision: %v\n", err)
		return latestSyncedRevision, err
	}
	latestSyncedRevision = resp.Header.Revision

	return latestSyncedRevision, nil
}
