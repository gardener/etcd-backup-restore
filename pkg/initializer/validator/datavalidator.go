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

	bolt "github.com/coreos/bbolt"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/snap"
	"github.com/coreos/etcd/wal"
	"github.com/coreos/etcd/wal/walpb"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	"github.com/sirupsen/logrus"
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
func (d *DataValidator) Validate(ctx context.Context, mode Mode, failBelowRevision int64) (DataDirStatus, error) {
	status, err := d.sanityCheck(ctx, failBelowRevision)
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

func (d *DataValidator) sanityCheck(ctx context.Context, failBelowRevision int64) (DataDirStatus, error) {
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
	d.Logger.Info("Checking for revision consistency...")
	return d.checkRevisionConsistency(ctx, etcdRevision, failBelowRevision)
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
	if err := verifyWALDir(d.walDir(), walsnap); err != nil {
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
	ssr := snap.New(d.snapDir())
	return ssr.Load()
}

func verifyWALDir(waldir string, snap walpb.Snapshot) error {
	var err error

	repaired := false
	for {
		if err = wal.Verify(waldir, snap); err != nil {
			// we can only repair ErrUnexpectedEOF and we never repair twice.
			if repaired || err != io.ErrUnexpectedEOF {
				fmt.Printf("read wal error (%v) and cannot be repaired.\n", err)
				return err
			}
			if !wal.Repair(waldir) {
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

// checkRevisionConsistency compares the latest revisions on the etcd db file and the latest snapshot to verify that the etcd revision is not lesser than snapshot revision.
// Return DataDirStatus indicating whether it is due to failBelowRevision or latest snapshot revision for snapstore.
func (d *DataValidator) checkRevisionConsistency(ctx context.Context, etcdRevision, failBelowRevision int64) (DataDirStatus, error) {
	store, err := snapstore.GetSnapstore(d.Config.SnapstoreConfig)
	if err != nil {
		return DataDirectoryStatusUnknown, fmt.Errorf("unable to fetch snapstore: %v", err)
	}

	var latestSnapshotRevision int64
	fullSnap, deltaSnaps, err := miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(ctx, store)
	if err != nil {
		return DataDirectoryStatusUnknown, fmt.Errorf("unable to get snapshots from store: %v", err)
	}
	if len(deltaSnaps) != 0 {
		latestSnapshotRevision = deltaSnaps[len(deltaSnaps)-1].LastRevision
	} else if fullSnap != nil {
		latestSnapshotRevision = fullSnap.LastRevision
	} else {
		d.Logger.Infof("No snapshot found.")
		if etcdRevision < failBelowRevision {
			d.Logger.Infof("current etcd revision (%d) is less than fail below revision (%d): possible data loss", etcdRevision, failBelowRevision)
			return FailBelowRevisionConsistencyError, nil
		}
		return DataDirectoryValid, nil
	}

	if etcdRevision < latestSnapshotRevision {
		d.Logger.Infof("current etcd revision (%d) is less than latest snapshot revision (%d): possible data loss", etcdRevision, latestSnapshotRevision)
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
