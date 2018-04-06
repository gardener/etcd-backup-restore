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
	"context"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// GCSSnapStore is snapstore with local disk as backend
type GCSSnapStore struct {
	SnapStore
	prefix string
	client *storage.Client
	bucket string
	ctx    context.Context
}

// NewGCSSnapStore create new S3SnapStore from shared configuration with specified bucket
func NewGCSSnapStore(bucket, prefix string) (*GCSSnapStore, error) {
	ctx := context.Background()
	gcsClient, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	return &GCSSnapStore{
		prefix: prefix,
		client: gcsClient,
		bucket: bucket,
		ctx:    ctx,
	}, nil

}

// Fetch should open reader for the snapshot file from store
func (s *GCSSnapStore) Fetch(snap Snapshot) (io.ReadCloser, error) {
	objectName := path.Join(s.prefix, snap.SnapDir, snap.SnapName)
	return s.client.Bucket(s.bucket).Object(objectName).NewReader(s.ctx)
}

// Size returns the size of snapshot
func (s *GCSSnapStore) Size(snap Snapshot) (int64, error) {
	// recursively list all "files", not directory
	it := s.client.Bucket(s.bucket).Objects(s.ctx, &storage.Query{Prefix: path.Join(s.prefix, snap.SnapDir, snap.SnapName)})

	var attrs []*storage.ObjectAttrs
	var err error
	for {
		var attr *storage.ObjectAttrs
		attr, err = it.Next()
		if err == iterator.Done {
			err = nil
			break
		}
		if err != nil {
			return 0, err
		}
		attrs = append(attrs, attr)
	}

	var size int64
	for _, v := range attrs {
		size += v.Size
	}
	return size, nil
}

// Save will write the snapshot to store
func (s *GCSSnapStore) Save(snap Snapshot, r io.Reader) error {
	bh := s.client.Bucket(s.bucket)
	// Next check if the bucket exists
	if _, err := bh.Attrs(s.ctx); err != nil {
		return err
	}

	name := path.Join(s.prefix, snap.SnapDir, snap.SnapName)
	obj := bh.Object(name)
	w := obj.NewWriter(s.ctx)
	defer w.Close()

	if _, err := io.Copy(w, r); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}

	_, err := obj.Attrs(s.ctx)
	return err
}

// List will list the snapshots from store
func (s *GCSSnapStore) List() (SnapList, error) {
	// recursively list all "files", not directory

	it := s.client.Bucket(s.bucket).Objects(s.ctx, &storage.Query{Prefix: s.prefix})

	var attrs []*storage.ObjectAttrs
	var err error
	for {
		var attr *storage.ObjectAttrs
		attr, err = it.Next()
		if err == iterator.Done {
			err = nil
			break
		}
		if err != nil {
			return nil, err
		}
		attrs = append(attrs, attr)
	}

	var snapList SnapList
	for _, v := range attrs {
		name := strings.Replace(v.Name, s.prefix+"/", "", 1)
		//name := v.Name[len(s.prefix):]
		snap, err := ParseSnapshot(name)
		if err != nil {
			// Warning
			fmt.Printf("Invalid snapshot found. Ignoring it:%s\n", name)
		} else {
			snapList = append(snapList, snap)
		}
	}

	sort.Sort(snapList)
	return snapList, nil
}

// Delete should delete the snapshot file from store
func (s *GCSSnapStore) Delete(snap Snapshot) error {
	objectName := path.Join(s.prefix, snap.SnapDir, snap.SnapName)
	return s.client.Bucket(s.bucket).Object(objectName).Delete(s.ctx)
}

// GetLatest returns the latest snapshot in snapstore
func (s *GCSSnapStore) GetLatest() (*Snapshot, error) {
	snapList, err := s.List()
	if err != nil {
		return nil, err
	}
	if snapList.Len() == 0 {
		return nil, nil
	}
	return snapList[snapList.Len()-1], nil
}
