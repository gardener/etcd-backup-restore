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

package snapstore

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
)

// LocalSnapStore is snapstore with local disk as backend
type LocalSnapStore struct {
	SnapStore
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
func (s *LocalSnapStore) Fetch(snap Snapshot) (io.ReadCloser, error) {
	return os.Open(path.Join(s.prefix, snap.SnapDir, snap.SnapName))
}

// GetLatest returns the latest snapshot in snapstore
func (s *LocalSnapStore) GetLatest() (*Snapshot, error) {
	snapList, err := s.List()
	if err != nil {
		return nil, err
	}
	if snapList.Len() == 0 {
		return nil, nil
	}
	return snapList[snapList.Len()-1], nil
}

// Save will write the snapshot to store
func (s *LocalSnapStore) Save(snap Snapshot, r io.Reader) error {
	err := os.MkdirAll(path.Join(s.prefix, snap.SnapDir), 0700)
	if err != nil && !os.IsExist(err) {
		return err
	}
	f, err := os.Create(path.Join(s.prefix, snap.SnapDir, snap.SnapName))
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, r)
	if err != nil {
		return err
	}
	return f.Sync()
}

// List will list the snapshots from store
func (s *LocalSnapStore) List() (SnapList, error) {
	snapList := SnapList{}
	directories, err := ioutil.ReadDir(s.prefix)
	if err != nil {
		return nil, err
	}
	for _, dir := range directories {
		if dir.IsDir() {
			files, err := ioutil.ReadDir(path.Join(s.prefix, dir.Name()))
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
	sort.Sort(snapList)
	return snapList, nil
}

// Delete should delete the snapshot file from store
func (s *LocalSnapStore) Delete(snap Snapshot) error {
	err := os.Remove(path.Join(s.prefix, snap.SnapDir, snap.SnapName))
	if err != nil {
		return err
	}
	err = os.Remove(path.Join(s.prefix, snap.SnapDir))
	return err
}
