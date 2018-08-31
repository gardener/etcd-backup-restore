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
	"time"

	"cloud.google.com/go/storage"
	"github.com/sirupsen/logrus"
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

const (
	gcsNoOfChunk int64 = 32 //Default configuration in swift installation
)

// NewGCSSnapStore create new S3SnapStore from shared configuration with specified bucket
func NewGCSSnapStore(bucket, prefix string) (*GCSSnapStore, error) {
	ctx := context.TODO()
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

// Save will write the snapshot to store
func (s *GCSSnapStore) Save(snap Snapshot, r io.Reader) error {
	// Save it locally
	tmpfile, err := ioutil.TempFile(tmpDir, tmpBackupFilePrefix)
	if err != nil {
		return fmt.Errorf("failed to create snapshot tempfile: %v", err)
	}
	defer func() {
		tmpfile.Close()
		os.Remove(tmpfile.Name())
	}()
	size, err := io.Copy(tmpfile, r)
	if err != nil {
		return fmt.Errorf("failed to save snapshot to tmpfile: %v", err)
	}

	var (
		errCh      = make(chan chunkUploadError)
		chunkSize  = int64(math.Max(float64(minChunkSize), float64(size/gcsNoOfChunk)))
		noOfChunks = size / chunkSize
	)
	if size%chunkSize != 0 {
		noOfChunks++
	}

	logrus.Infof("Uploading snapshot of size: %d, chunkSize: %d, noOfChunks: %d", size, chunkSize, noOfChunks)
	for offset := int64(0); offset <= size; offset += int64(chunkSize) {
		go uploadComponent(s, &snap, tmpfile, offset, chunkSize, errCh)
	}

	snapshotErr := handleComponentUpload(s, &snap, tmpfile, errCh, noOfChunks, chunkSize)
	if len(snapshotErr) == 0 {
		logrus.Info("All chunk uploaded successfully. Uploading composit object.")
		bh := s.client.Bucket(s.bucket)
		var subObjects []*storage.ObjectHandle
		for partNumber := int64(1); partNumber <= noOfChunks; partNumber++ {
			name := path.Join(s.prefix, snap.SnapDir, snap.SnapName, fmt.Sprintf("%010d", partNumber))
			obj := bh.Object(name)
			subObjects = append(subObjects, obj)
		}
		name := path.Join(s.prefix, snap.SnapDir, snap.SnapName)
		obj := bh.Object(name)
		c := obj.ComposerFrom(subObjects...)
		ctx, cancel := context.WithTimeout(context.TODO(), chunkUploadTimeout)
		defer cancel()
		_, err := c.Run(ctx)
		return err
	}
	var collectedErr []string
	for _, chunkErr := range snapshotErr {
		collectedErr = append(collectedErr, fmt.Sprintf("failed uploading chunk with offset %010d with error %v", chunkErr.offset, chunkErr.err))
	}
	return fmt.Errorf(strings.Join(collectedErr, "\n"))
}

func uploadComponent(s *GCSSnapStore, snap *Snapshot, file *os.File, offset, chunkSize int64, errCh chan<- chunkUploadError) {
	logrus.Infof("Uploading chunk with offset : %010d", offset)
	if _, err := file.Seek(0, os.SEEK_SET); err != nil {
		errCh <- chunkUploadError{
			err:    fmt.Errorf("failed to set offset in temp file: %v", err),
			offset: offset,
		}
		return
	}
	sr := io.NewSectionReader(file, offset, chunkSize)

	bh := s.client.Bucket(s.bucket)
	partNumber := ((offset / chunkSize) + 1)
	name := path.Join(s.prefix, snap.SnapDir, snap.SnapName, fmt.Sprintf("%010d", partNumber))
	obj := bh.Object(name)
	ctx, cancel := context.WithTimeout(context.TODO(), chunkUploadTimeout)
	defer cancel()
	w := obj.NewWriter(ctx)
	if _, err := io.Copy(w, sr); err != nil {
		logrus.Infof("For chunk upload of offset %010d, err %v", offset, err)
		errCh <- chunkUploadError{
			err:    err,
			offset: offset,
		}
		w.Close()
		return
	}

	err := w.Close()
	if err != nil {
		logrus.Infof("For chunk upload of offset %010d, err %v", offset, err)
	}
	errCh <- chunkUploadError{
		err:    err,
		offset: offset,
	}

	return
}

func handleComponentUpload(s *GCSSnapStore, snap *Snapshot, file *os.File, errCh chan chunkUploadError, noOfChunks, chunkSize int64) []chunkUploadError {
	var snapshotErr []chunkUploadError
	manifest := make([]int64, noOfChunks)
	maxAttempts := int64(5)
	remainingChunks := noOfChunks
	logrus.Infof("No of Chunks:= %d", noOfChunks)
	for {
		chunkErr := <-errCh
		if chunkErr.err != nil {
			manifestIndex := chunkErr.offset / chunkSize
			logrus.Warnf("Failed to upload chunk with offset: %019d at attempt %d  with error: %v", chunkErr.offset, manifest[manifestIndex], chunkErr.err)
			if manifest[manifestIndex] < maxAttempts {
				manifest[manifestIndex]++
				delayTime := (1 << uint(manifest[manifestIndex]))
				logrus.Infof("Will try to upload chunk with offset: %010d at attempt %d  after %d seconds", chunkErr.offset, manifest[manifestIndex], delayTime)
				time.Sleep((time.Duration)(delayTime) * time.Second)
				go uploadComponent(s, snap, file, chunkErr.offset, chunkSize, errCh)
				continue
			}
			snapshotErr = append(snapshotErr, chunkErr)
		}
		remainingChunks--
		if remainingChunks == 0 {
			return snapshotErr
		}
	}
}

// List will list the snapshots from store
func (s *GCSSnapStore) List() (SnapList, error) {
	// recursively list all "files", not directory

	it := s.client.Bucket(s.bucket).Objects(s.ctx, &storage.Query{Prefix: s.prefix})

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
