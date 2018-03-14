// Copyright © 2018 The Gardener Authors.
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
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"

	bolt "github.com/coreos/bbolt"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/snap"
	"github.com/coreos/etcd/snap/snappb"
	"github.com/coreos/etcd/wal"
	"github.com/coreos/etcd/wal/walpb"
	"github.com/sirupsen/logrus"
)

const (
	// DataDirectoryValid indicates data directory is valid.
	DataDirectoryValid = iota
	// DataDirectoryNotExist indicates data directory is non-existant.
	DataDirectoryNotExist
	// DataDirectoryInvStruct indicates data directory has invalid structure.
	DataDirectoryInvStruct
	// DataDirectoryCorrupt indicates data directory is corrupt.
	DataDirectoryCorrupt
	// DataDirectoryError indicates unknown error while validation.
	DataDirectoryError
)

const (
	snapSuffix = ".snap"
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

// DataDirStatus represents the status of the etcd data directory.
type DataDirStatus int

func init() {
	logger = logrus.New()
}

func (d *DataValidator) memberDir() string { return filepath.Join(d.Config.DataDir, "member") }

func (d *DataValidator) walDir() string { return filepath.Join(d.memberDir(), "wal") }

func (d *DataValidator) snapDir() string { return filepath.Join(d.memberDir(), "snap") }

func (d *DataValidator) backendPath() string { return filepath.Join(d.snapDir(), "db") }

//Validate performs the steps required to validate data for Etcd instance.
// The steps involed are:
//   * Check if data directory exists.
//     - If data directory exists
//		 * Check for data directory structure.
//			- If data directory structure is invalid return DataDirectoryInvStruct status.
//       * Check for data corruption.
//			- return data directory corruption status.
func (d *DataValidator) Validate() (DataDirStatus, error) {
	dataDir := d.Config.DataDir
	dirExists, err := directoryExist(dataDir)
	if err != nil {
		return DataDirectoryError, err
	} else if !dirExists {
		err = fmt.Errorf("Directory does not exist: %s", dataDir)
		return DataDirectoryNotExist, err
	}

	d.Logger.Info("Checking for data directory structure validity...")
	etcdDirStructValid, err := d.hasEtcdDirectoryStructure()
	if err != nil {
		return DataDirectoryError, err
	}
	if !etcdDirStructValid {
		d.Logger.Infof("Data directory structure invalid.")
		return DataDirectoryInvStruct, nil
	}
	d.Logger.Info("Checking for data directory files corruption...")
	err = d.checkForDataCorruption()
	if err != nil {
		d.Logger.Infof("Data directory corrupt. %v", err)
		return DataDirectoryCorrupt, nil
	}
	d.Logger.Info("Data directory valid.")
	return DataDirectoryValid, nil
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

func isDirEmpty(dataDir string) (bool, error) {
	f, err := os.Open(dataDir)
	if err != nil {
		return false, err
	}
	defer f.Close()

	_, err = f.Readdirnames(1) // Or f.Readdir(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err // Either not empty or error, suits both cases
}

func (d *DataValidator) verifySnapDir() (*raftpb.Snapshot, error) {
	names, err := d.snapNames()
	if err != nil {
		return nil, err
	}

	var snapshot *raftpb.Snapshot
	for _, name := range names {
		if snapshot, err = verifySnap(d.snapDir(), name); err == nil {
			break
		}
	}
	if err != nil {
		return nil, snap.ErrNoSnapshot
	}
	return snapshot, nil
}

func verifySnap(dir, name string) (*raftpb.Snapshot, error) {
	fpath := filepath.Join(dir, name)
	snap, err := read(fpath)
	return snap, err
}

// Read reads the snapshot named by snapname and returns the snapshot.
func read(snapname string) (*raftpb.Snapshot, error) {
	fmt.Printf("Verifying Snapfile %s.\n", snapname)
	b, err := ioutil.ReadFile(snapname)
	if err != nil {
		fmt.Printf("Cannot read file %v: %v.\n", snapname, err)
		return nil, err
	}

	if len(b) == 0 {
		fmt.Printf("Unexpected empty snapshot.\n")
		return nil, snap.ErrEmptySnapshot
	}

	var serializedSnap snappb.Snapshot
	if err = serializedSnap.Unmarshal(b); err != nil {
		fmt.Printf("Corrupted snapshot file %v: %v.\n", snapname, err)
		return nil, err
	}

	if len(serializedSnap.Data) == 0 || serializedSnap.Crc == 0 {
		fmt.Printf("Unexpected empty snapshot.\n")
		return nil, snap.ErrEmptySnapshot
	}

	crc := crc32.Update(0, crcTable, serializedSnap.Data)
	if crc != serializedSnap.Crc {
		fmt.Printf("Corrupted snapshot file %v: crc mismatch.\n", snapname)
		return nil, snap.ErrCRCMismatch
	}

	var snap raftpb.Snapshot
	if err = snap.Unmarshal(serializedSnap.Data); err != nil {
		fmt.Printf("corrupted snapshot file %v: %v", snapname, err)
		return nil, err
	}
	return &snap, nil
}

func (d *DataValidator) snapNames() ([]string, error) {
	dir, err := os.Open(d.snapDir())
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	snaps := checkSuffix(names)
	if len(snaps) == 0 {
		return nil, snap.ErrNoSnapshot
	}
	sort.Sort(sort.Reverse(sort.StringSlice(snaps)))
	return snaps, nil
}

func checkSuffix(names []string) []string {
	snaps := []string{}
	for i := range names {
		if strings.HasSuffix(names[i], snapSuffix) {
			snaps = append(snaps, names[i])
		} else {
			// If we find a file which is not a snapshot then check if it's
			// a vaild file. If not throw out a warning.
			if _, ok := validFiles[names[i]]; !ok {
				fmt.Printf("skipped unexpected non snapshot file %v", names[i])
			}
		}
	}
	return snaps
}

func verifyWALDir(waldir string, snap walpb.Snapshot) error {
	var (
		err error
		w   *wal.WAL
	)

	repaired := false
	for {
		w, err = wal.Open(waldir, snap)
		if err != nil {
			fmt.Printf("open wal error: %v", err)
		}
		defer w.Close()
		if _, _, _, err = w.ReadAll(); err != nil {
			// we can only repair ErrUnexpectedEOF and we never repair twice.
			if repaired || err != io.ErrUnexpectedEOF {
				fmt.Printf("read wal error (%v) and cannot be repaired.\n", err)
				return err
			}
			w.Close()
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
	defer db.Close()
	if err != nil {
		return err
	}

	// Perform consistency check.
	return db.View(func(tx *bolt.Tx) error {
		var count int
		for _ = range tx.Check() {
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
