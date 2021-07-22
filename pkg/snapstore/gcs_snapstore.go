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
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path"
	"sort"
	"strings"
	"sync"

	"cloud.google.com/go/storage"
	stiface "github.com/gardener/etcd-backup-restore/pkg/snapstore/gcs"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
)

const (
	sourceStoreCredentials = "SOURCE_GOOGLE_APPLICATION_CREDENTIALS"
)

// GCSSnapStore is snapstore with GCS object store as backend.
type GCSSnapStore struct {
	client stiface.Client
	prefix string
	bucket string
	// maxParallelChunkUploads hold the maximum number of parallel chunk uploads allowed.
	maxParallelChunkUploads uint
	tempDir                 string
}

const (
	// Total number of chunks to be uploaded must be one less than maximum limit allowed.
	gcsNoOfChunk int64 = 31
)

// NewGCSSnapStore create new GCSSnapStore from shared configuration with specified bucket.
func NewGCSSnapStore(config *brtypes.SnapstoreConfig) (*GCSSnapStore, error) {
	ctx := context.TODO()
	var opts []option.ClientOption
	if config.IsSource {
		filename := os.Getenv(sourceStoreCredentials)
		if filename == "" {
			return nil, fmt.Errorf("Environment variable %s is not set", sourceStoreCredentials)
		}
		opts = append(opts, option.WithCredentialsFile(filename))
	}
	cli, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, err
	}
	gcsClient := stiface.AdaptClient(cli)

	return NewGCSSnapStoreFromClient(config.Container, config.Prefix, config.TempDir, config.MaxParallelChunkUploads, gcsClient), nil
}

// NewGCSSnapStoreFromClient create new GCSSnapStore from shared configuration with specified bucket.
func NewGCSSnapStoreFromClient(bucket, prefix, tempDir string, maxParallelChunkUploads uint, cli stiface.Client) *GCSSnapStore {
	return &GCSSnapStore{
		prefix:                  prefix,
		client:                  cli,
		bucket:                  bucket,
		maxParallelChunkUploads: maxParallelChunkUploads,
		tempDir:                 tempDir,
	}
}

// Fetch should open reader for the snapshot file from store.
func (s *GCSSnapStore) Fetch(snap brtypes.Snapshot) (io.ReadCloser, error) {
	objectName := path.Join(snap.Prefix, snap.SnapDir, snap.SnapName)
	ctx := context.TODO()
	return s.client.Bucket(s.bucket).Object(objectName).NewReader(ctx)
}

// Save will write the snapshot to store.
func (s *GCSSnapStore) Save(snap brtypes.Snapshot, rc io.ReadCloser) error {
	tmpfile, err := ioutil.TempFile(s.tempDir, tmpBackupFilePrefix)
	if err != nil {
		rc.Close()
		return fmt.Errorf("failed to create snapshot tempfile: %v", err)
	}
	defer func() {
		tmpfile.Close()
		os.Remove(tmpfile.Name())
	}()
	size, err := io.Copy(tmpfile, rc)
	rc.Close()
	if err != nil {
		return fmt.Errorf("failed to save snapshot to tmpfile: %v", err)
	}

	var (
		chunkSize  = int64(math.Max(float64(minChunkSize), float64(size/gcsNoOfChunk)))
		noOfChunks = size / chunkSize
	)
	if size%chunkSize != 0 {
		noOfChunks++
	}

	var (
		chunkUploadCh = make(chan chunk, noOfChunks)
		resCh         = make(chan chunkUploadResult, noOfChunks)
		wg            sync.WaitGroup
		cancelCh      = make(chan struct{})
	)

	for i := uint(0); i < s.maxParallelChunkUploads; i++ {
		wg.Add(1)
		go s.componentUploader(&wg, cancelCh, &snap, tmpfile, chunkUploadCh, resCh)
	}

	logrus.Infof("Uploading snapshot of size: %d, chunkSize: %d, noOfChunks: %d", size, chunkSize, noOfChunks)
	for offset, index := int64(0), 1; offset < size; offset += int64(chunkSize) {
		newChunk := chunk{
			id:     index,
			offset: offset,
			size:   chunkSize,
		}
		chunkUploadCh <- newChunk
		index++
	}
	logrus.Infof("Triggered chunk upload for all chunks, total: %d", noOfChunks)

	snapshotErr := collectChunkUploadError(chunkUploadCh, resCh, cancelCh, noOfChunks)
	wg.Wait()

	if snapshotErr != nil {
		return fmt.Errorf("failed uploading chunk, id: %d, offset: %d, error: %v", snapshotErr.chunk.id, snapshotErr.chunk.offset, snapshotErr.err)
	}
	logrus.Info("All chunk uploaded successfully. Uploading composite object.")
	bh := s.client.Bucket(s.bucket)
	var subObjects []stiface.ObjectHandle
	prefix := adaptPrefix(&snap, s.prefix)
	for partNumber := int64(1); partNumber <= noOfChunks; partNumber++ {
		name := path.Join(prefix, snap.SnapDir, snap.SnapName, fmt.Sprintf("%010d", partNumber))
		obj := bh.Object(name)
		subObjects = append(subObjects, obj)
	}
	name := path.Join(prefix, snap.SnapDir, snap.SnapName)
	obj := bh.Object(name)
	c := obj.ComposerFrom(subObjects...)
	ctx, cancel := context.WithTimeout(context.TODO(), chunkUploadTimeout)
	defer cancel()
	if _, err := c.Run(ctx); err != nil {
		return fmt.Errorf("failed uploading composite object for snapshot with error: %v", err)
	}
	logrus.Info("Composite object uploaded successfully.")
	return nil
}

func (s *GCSSnapStore) uploadComponent(snap *brtypes.Snapshot, file *os.File, offset, chunkSize int64) error {
	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}
	size := fileInfo.Size() - offset
	if size > chunkSize {
		size = chunkSize
	}

	sr := io.NewSectionReader(file, offset, size)
	bh := s.client.Bucket(s.bucket)
	partNumber := ((offset / chunkSize) + 1)
	name := path.Join(adaptPrefix(snap, s.prefix), snap.SnapDir, snap.SnapName, fmt.Sprintf("%010d", partNumber))
	obj := bh.Object(name)
	ctx, cancel := context.WithTimeout(context.TODO(), chunkUploadTimeout)
	defer cancel()
	w := obj.NewWriter(ctx)
	if _, err := io.Copy(w, sr); err != nil {
		w.Close()
		return err
	}
	return w.Close()
}

func (s *GCSSnapStore) componentUploader(wg *sync.WaitGroup, stopCh <-chan struct{}, snap *brtypes.Snapshot, file *os.File, chunkUploadCh chan chunk, errCh chan<- chunkUploadResult) {
	defer wg.Done()
	for {
		select {
		case <-stopCh:
			return
		case chunk, ok := <-chunkUploadCh:
			if !ok {
				return
			}
			logrus.Infof("Uploading chunk with offset : %d, attempt: %d", chunk.offset, chunk.attempt)
			err := s.uploadComponent(snap, file, chunk.offset, chunk.size)
			errCh <- chunkUploadResult{
				err:   err,
				chunk: &chunk,
			}
		}
	}
}

// List will return sorted list with all snapshot files on store.
func (s *GCSSnapStore) List() (brtypes.SnapList, error) {
	prefixTokens := strings.Split(s.prefix, "/")
	// Last element of the tokens is backup version
	// Consider the parent of the backup version level (Required for Backward Compatibility)
	prefix := path.Join(strings.Join(prefixTokens[:len(prefixTokens)-1], "/"))

	it := s.client.Bucket(s.bucket).Objects(context.TODO(), &storage.Query{Prefix: prefix})

	var attrs []*storage.ObjectAttrs
	for {
		attr, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		attrs = append(attrs, attr)
	}

	var snapList brtypes.SnapList
	for _, v := range attrs {
		if strings.Contains(v.Name, backupVersionV1) || strings.Contains(v.Name, backupVersionV2) {
			snap, err := ParseSnapshot(v.Name)
			if err != nil {
				// Warning
				logrus.Warnf("Invalid snapshot %s found, ignoring it: %v", v.Name, err)
				continue
			}
			snapList = append(snapList, snap)
		}
	}

	sort.Sort(snapList)
	return snapList, nil
}

// Delete should delete the snapshot file from store.
func (s *GCSSnapStore) Delete(snap brtypes.Snapshot) error {
	objectName := path.Join(snap.Prefix, snap.SnapDir, snap.SnapName)
	return s.client.Bucket(s.bucket).Object(objectName).Delete(context.TODO())
}
