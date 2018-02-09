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
	"io"
	"io/ioutil"
	"os"
	"path"
)

// LocalSnapStore is snapstore with local disk as backend
type LocalSnapStore struct {
	SnapStore
	prefix string
}

// NewLocalSnapStore return the new local disk based snapstore
func NewLocalSnapStore(prefix string) (*LocalSnapStore, error) {
	if len(prefix) != 0 {
		err := os.Mkdir(prefix, os.ModeDir)
		if err != nil && !os.IsExist(err) {
			return nil, err
		}
	}
	return &LocalSnapStore{
		prefix: prefix,
	}, nil
}

// Fetch should open reader for the snapshot file from store
func (s *LocalSnapStore) Fetch(snap string) (io.ReadCloser, error) {
	return os.Open(path.Join(s.prefix, snap))
}

// Size returns the size of snapshot
func (s *LocalSnapStore) Size(snap string) (int64, error) {
	fi, err := os.Stat(path.Join(s.prefix, snap))
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

// Save will write the snapshot to store
func (s *LocalSnapStore) Save(snap string, r io.Reader) error {
	f, err := os.Create(path.Join(s.prefix, snap))
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
func (s *LocalSnapStore) List() ([]string, error) {
	var snapList = []string{}
	files, err := ioutil.ReadDir(s.prefix)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		if !f.IsDir() {
			snapList = append(snapList, f.Name())
		}
	}
	return snapList, nil
}
