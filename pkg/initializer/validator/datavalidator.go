// Copyright (c) 2018 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
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

package validator

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"time"

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
	// A map of valid files that can be present in the snap folder.
	validFiles = map[string]bool{
		"db": true,
	}
	crcTable = crc32.MakeTable(crc32.Castagnoli)

	// ErrPathRequired is returned when the path to a Bolt database is not specified.
	ErrPathRequired = errors.New("path required")

	// ErrFileNotFound is returned when a Bolt database does not exist.
	ErrFileNotFound = errors.New("file not found")

	// ErrCorrupt is returned when a checking a data file finds errors.
	ErrCorrupt = errors.New("invalid value")

	logger *logrus.Logger
)

func init() {
	logger = logrus.New()
}

func (d *DataValidator) memberDir() string { return filepath.Join(d.Config.DataDir, "member") }

func (d *DataValidator) walDir() string { return filepath.Join(d.memberDir(), "wal") }

func (d *DataValidator) snapDir() string { return filepath.Join(d.memberDir(), "snap") }

func (d *DataValidator) backendPath() string { return filepath.Join(d.snapDir(), "db") }

// Validate performs the steps required to validate data for Etcd instance.
func (d *DataValidator) Validate(mode Mode, failBelowRevision int64) (DataDirStatus, error) {
	status, err := d.sanityCheck(failBelowRevision)
	if status != DataDirectoryValid {
		return status, err
	}

	if mode == Full {
		d.Logger.Info("Checking for data directory files corruption...")
		if err := d.checkForDataCorruption(); err != nil {
			d.Logger.Infof("Data directory corrupt. %v", err)
			return DataDirectoryCorrupt, nil
		}
	}

	d.Logger.Info("Data directory valid.")
	return DataDirectoryValid, nil
}

func (d *DataValidator) sanityCheck(failBelowRevision int64) (DataDirStatus, error) {
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

	etcdRevision, err := getLatestEtcdRevision(d.backendPath())
	if err != nil {
		d.Logger.Infof("unable to get current etcd revision from backend db file: %v", err)
		return DataDirectoryCorrupt, nil
	}

	d.Logger.Info("Checking for etcd revision consistency...")
	etcdRevisionStatus, latestSnapshotRevision, err := d.checkEtcdDataRevisionConsistency(etcdRevision, failBelowRevision)

	// if etcd revision is inconsistent with latest snapshot revision then
	//   check the etcd revision consistency by starting an embedded etcd since the WALs file can have uncommited data which it was unable to flush to Bolt DB.
	if etcdRevisionStatus == RevisionConsistencyError {
		d.Logger.Info("Checking for Full revision consistency...")
		fullRevisionConsistencyStatus, err := d.checkFullRevisionConsistency(dataDir, latestSnapshotRevision)
		return fullRevisionConsistencyStatus, err
	}

	return etcdRevisionStatus, err
}

// checkForDataCorruption will check for corruption of different files used by etcd.
func (d *DataValidator) checkForDataCorruption() error {
	var walsnap walpb.Snapshot
	d.Logger.Info("Verifying snap directory...")
	snapshot, err := d.verifySnapDir()
	if err != nil && err != snap.ErrNoSnapshot {
		return fmt.Errorf("Invalid snapshot files: %v", err)
	}
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	d.Logger.Info("Verifying WAL directory...")
	if err := verifyWALDir(d.ZapLogger, d.walDir(), walsnap); err != nil {
		return fmt.Errorf("Invalid wal files: %v", err)
	}
	d.Logger.Info("Verifying DB file...")
	err = verifyDB(d.backendPath())
	if err != nil {
		return fmt.Errorf("Invalid db files: %v", err)
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

	// Open database.
	db, err := bolt.Open(path, 0666, nil)
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
			RestoreDataDir:         dataDir,
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
	client, err := clientv3.NewFromURL(e.Clients[0].Addr().String())
	if err != nil {
		d.Logger.Infof("unable to get the embedded etcd client: %v", err)
		return DataDirectoryCorrupt, err
	}
	defer client.Close()

	timer := time.NewTimer(embeddedEtcdPingLimitDuration)

waitLoop:
	for {
		select {
		case <-timer.C:
			break waitLoop
		default:
			latestSyncedEtcdRevision, _ = getLatestSyncedRevision(client)
			if latestSyncedEtcdRevision >= latestSnapshotRevision {
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

	db, err := bolt.Open(path, 0400, &bolt.Options{ReadOnly: true})
	if err != nil {
		return -1, fmt.Errorf("unable to open backend boltdb file: %v", err)
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
func getLatestSyncedRevision(client *clientv3.Client) (int64, error) {
	var latestSyncedRevision int64

	ctx, cancel := context.WithTimeout(context.TODO(), connectionTimeout)
	defer cancel()
	resp, err := client.Get(ctx, "", clientv3.WithLastRev()...)
	if err != nil {
		fmt.Printf("Failed to get the latest etcd revision: %v\n", err)
		return latestSyncedRevision, err
	}
	latestSyncedRevision = resp.Header.Revision

	return latestSyncedRevision, nil
}
