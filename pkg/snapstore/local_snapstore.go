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

package snapstore

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"
	"syscall"

	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
)

// LocalSnapStore is snapstore with local disk as backend
type LocalSnapStore struct {
	prefix string
}

// NewLocalSnapStore return the new local disk based snapstore
func NewLocalSnapStore(prefix string) (*LocalSnapStore, error) {
	if len(prefix) != 0 {
		err := os.MkdirAll(prefix, 0700)
		if err != nil && !os.IsExist(err) {
			return nil, err
		}
	}
	return &LocalSnapStore{
		prefix: prefix,
	}, nil
}

// Fetch should open reader for the snapshot file from store
func (s *LocalSnapStore) Fetch(snap brtypes.Snapshot) (io.ReadCloser, error) {
	// Fetch the snapshot from V2 storage first as defined in prefix
	// If not found, check V1 storage as well. Return error if still not found
	// TODO: Remove when backward compatibility is not needed
	ir, err := os.Open(path.Join(s.prefix, snap.SnapDir, snap.SnapName))
	if err != nil {
		prefixTokens := strings.Split(s.prefix, "/")
		prefixTokens = prefixTokens[:len(prefixTokens)-1]
		prefix := path.Join(strings.Join(prefixTokens, "/"), backupVersionV1)

		ir, err = os.Open(path.Join(prefix, snap.SnapDir, snap.SnapName))
		if err != nil {
			return nil, err
		}
	}
	return ir, nil
}

// Save will write the snapshot to store
func (s *LocalSnapStore) Save(snap brtypes.Snapshot, rc io.ReadCloser) error {
	defer rc.Close()
	err := os.MkdirAll(path.Join(s.prefix, snap.SnapDir), 0700)
	if err != nil && !os.IsExist(err) {
		return err
	}
	f, err := os.Create(path.Join(s.prefix, snap.SnapDir, snap.SnapName))
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, rc)
	if err != nil {
		return err
	}
	return f.Sync()
}

// List will list the snapshots from store
func (s *LocalSnapStore) List() (brtypes.SnapList, error) {
	var snapList brtypes.SnapList
	// Probing for V1 and V2 is required for backward compatibility
	// TODO: Remove when backward compatibility is not needed
	var v1, v2 bool = false, false
	var err error = nil

	v1 = s.isV1Present()
	v2 = s.isV2Present()

	prefix := s.prefix
	// This if condition is to consider the storage which doesn't yet have V2 directory (Required for backward compatibility)
	if v1 && !v2 {
		prefixTokens := strings.Split(s.prefix, "/")
		prefixTokens = prefixTokens[:len(prefixTokens)-1]
		prefix = path.Join(strings.Join(prefixTokens, "/"), backupVersionV1)
		snapList, err = s.makeSnapListV1(prefix)
	} else {
		snapList, err = s.makeSnapListV2(prefix)
	}

	if err != nil {
		return nil, err
	}

	// This if condition is to ensure garbage collector considers remaining V1 directory as well (Required for backward compatibility)
	if v2 && v1 {
		prefixTokens := strings.Split(s.prefix, "/")
		prefixTokens = prefixTokens[:len(prefixTokens)-1]

		prefix = path.Join(strings.Join(prefixTokens, "/"), backupVersionV1)
		l, err := s.makeSnapListV1(prefix)
		if err != nil {
			return nil, err
		}
		snapList = append(snapList, l...)
	}

	sort.Sort(snapList)
	return snapList, nil
}

func (s *LocalSnapStore) makeSnapListV1(prefix string) (brtypes.SnapList, error) {
	snapList := brtypes.SnapList{}
	directories, err := ioutil.ReadDir(prefix)
	if err != nil {
		return nil, err
	}
	for _, dir := range directories {
		if dir.IsDir() {
			files, err := ioutil.ReadDir(path.Join(prefix, dir.Name()))
			if err != nil {
				return nil, err
			}

			for _, f := range files {
				snap, err := ParseSnapshot(path.Join(dir.Name(), f.Name()))
				if err != nil {
					// Warning
					fmt.Printf("Invalid snapshot %s found:%v\nIgnoring it.", path.Join(dir.Name(), f.Name()), err)
				} else {
					snapList = append(snapList, snap)
				}
			}
		}
	}
	return snapList, nil
}

func (s *LocalSnapStore) makeSnapListV2(prefix string) (brtypes.SnapList, error) {
	snapList := brtypes.SnapList{}
	files, err := ioutil.ReadDir(prefix)
	if err != nil {
		return nil, err
	}

	for _, f := range files {
		snap, err := ParseSnapshot(f.Name())
		if err != nil {
			// Warning
			fmt.Printf("Invalid snapshot %s found:%v\nIgnoring it.", f.Name(), err)
		} else {
			snapList = append(snapList, snap)
		}
	}
	return snapList, nil
}

// Delete should delete the snapshot file from store
func (s *LocalSnapStore) Delete(snap brtypes.Snapshot) error {
	if err := os.Remove(path.Join(s.prefix, snap.SnapDir, snap.SnapName)); err != nil {
		if _, ok := err.(*os.PathError); ok {
			// Try to delete from V1 prefix as well
			prefixTokens := strings.Split(s.prefix, "/")
			prefixTokens = prefixTokens[:len(prefixTokens)-1]
			prefix := path.Join(strings.Join(prefixTokens, "/"), backupVersionV1)
			if err := os.Remove(path.Join(prefix, snap.SnapDir, snap.SnapName)); err != nil {
				return err
			}
		}
	}
	if snap.SnapDir != "" {
		err := os.Remove(path.Join(s.prefix, snap.SnapDir))
		if pathErr, ok := err.(*os.PathError); ok && pathErr.Err != syscall.ENOTEMPTY {
			return err
		}
	}
	return nil
}

// isV1Present checks if V1 is present (Required for backward compatibility)
func (s *LocalSnapStore) isV1Present() bool {
	prefixTokens := strings.Split(s.prefix, "/")
	prefixTokens = prefixTokens[:len(prefixTokens)-1]

	if _, err := os.Stat(path.Join(strings.Join(prefixTokens, "/"), backupVersionV1)); os.IsNotExist(err) {
		return false
	}
	return true
}

// isV2Present checks if V2 prefix is present (Required for backward compatibility)
func (s *LocalSnapStore) isV2Present() bool {
	prefixTokens := strings.Split(s.prefix, "/")
	prefixTokens = prefixTokens[:len(prefixTokens)-1]

	if _, err := os.Stat(path.Join(strings.Join(prefixTokens, "/"), backupVersionV2)); os.IsNotExist(err) {
		return false
	}
	return true
}

// Size should return size of the snapshot file from store
func (s *LocalSnapStore) Size(snap brtypes.Snapshot) (int64, error) {
	fileInfo, err := os.Stat(path.Join(s.prefix, snap.SnapDir, snap.SnapName))
	if err != nil {
		return -1, err
	}
	return fileInfo.Size(), nil
}
