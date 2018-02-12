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
	return os.Open(path.Join(s.prefix, snap.SnapPath))
}

// Size returns the size of snapshot
func (s *LocalSnapStore) Size(snap Snapshot) (int64, error) {
	fi, err := os.Stat(path.Join(s.prefix, snap.SnapPath))
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

// Save will write the snapshot to store
func (s *LocalSnapStore) Save(snap Snapshot, r io.Reader) error {
	f, err := os.Create(path.Join(s.prefix, snap.SnapPath))
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
	files, err := ioutil.ReadDir(s.prefix)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		if !f.IsDir() {
			s, err := ParseSnapshot(f.Name())
			if err != nil {
				// Warning
				fmt.Printf("Invalid snapshot %s found:%v\nIngonring it.", f.Name(), err)
			} else {
				snapList = append(snapList, s)
			}
		}
	}
	sort.Sort(snapList)
	return snapList, nil
}

// Delete should delete the snapshot file from store
func (s *LocalSnapStore) Delete(snap Snapshot) error {
	err := os.Remove(path.Join(s.prefix, snap.SnapPath))
	return err
}
