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
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/sirupsen/logrus"
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
	return os.Open(path.Join(snap.Prefix, snap.SnapDir, snap.SnapName))
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

// List will return sorted list with all snapshot files on store.
func (s *LocalSnapStore) List() (brtypes.SnapList, error) {
	prefixTokens := strings.Split(s.prefix, "/")
	// Last element of the tokens is backup version
	// Consider the parent of the backup version level (Required for Backward Compatibility)
	prefix := path.Join(strings.Join(prefixTokens[:len(prefixTokens)-1], "/"))

	snapList := brtypes.SnapList{}
	err := filepath.Walk(prefix, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Printf("prevent panic by handling failure accessing a path %q: %v\n", path, err)
			return err
		}
		if info.IsDir() {
			return nil
		}
		if strings.Contains(path, backupVersionV1) || strings.Contains(path, backupVersionV2) {
			snap, err := ParseSnapshot(path)
			if err != nil {
				// Warning
				logrus.Warnf("Invalid snapshot found. Ignoring it:%s\n", path)
			} else {
				snapList = append(snapList, snap)
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("error walking the path %q: %v", prefix, err)
	}

	sort.Sort(snapList)
	return snapList, nil
}

// Delete should delete the snapshot file from store
func (s *LocalSnapStore) Delete(snap brtypes.Snapshot) error {
	if err := os.Remove(path.Join(snap.Prefix, snap.SnapDir, snap.SnapName)); err != nil {
		return err
	}
	err := os.Remove(path.Join(snap.Prefix, snap.SnapDir))
	if pathErr, ok := err.(*os.PathError); ok && pathErr.Err != syscall.ENOTEMPTY {
		return err
	}
	return nil
}

// Size should return size of the snapshot file from store
func (s *LocalSnapStore) Size(snap brtypes.Snapshot) (int64, error) {
	fileInfo, err := os.Stat(path.Join(s.prefix, snap.SnapDir, snap.SnapName))
	if err != nil {
		return -1, err
	}
	return fileInfo.Size(), nil
}
