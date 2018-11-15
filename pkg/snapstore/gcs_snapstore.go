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
		go retryComponentUpload(s, &snap, tmpfile, offset, chunkSize, errCh)
	}

	snapshotErr := collectChunkUploadError(errCh, noOfChunks)
	if len(snapshotErr) == 0 {
		logrus.Info("All chunk uploaded successfully. Uploading composite object.")
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
		collectedErr = append(collectedErr, fmt.Sprintf("failed uploading chunk with offset %d with error %v", chunkErr.offset, chunkErr.err))
	}
	return fmt.Errorf(strings.Join(collectedErr, "\n"))
}

func uploadComponent(s *GCSSnapStore, snap *Snapshot, file *os.File, offset, chunkSize int64) error {
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
	name := path.Join(s.prefix, snap.SnapDir, snap.SnapName, fmt.Sprintf("%010d", partNumber))
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

func retryComponentUpload(s *GCSSnapStore, snap *Snapshot, file *os.File, offset, chunkSize int64, errCh chan<- chunkUploadError) {
	var (
		maxAttempts uint = 5
		curAttempt  uint = 1
		err         error
	)
	for {
		logrus.Infof("Uploading chunk with offset : %d, attempt: %d", offset, curAttempt)
		err = uploadComponent(s, snap, file, offset, chunkSize)
		logrus.Infof("For chunk upload of offset %d, err %v", offset, err)
		if err == nil || curAttempt == maxAttempts {
			break
		}
		delayTime := (1 << curAttempt)
		curAttempt++
		logrus.Warnf("Will try to upload chunk with offset: %d at attempt %d  after %d seconds", offset, curAttempt, delayTime)
		time.Sleep((time.Duration)(delayTime) * time.Second)
	}

	errCh <- chunkUploadError{
		err:    err,
		offset: offset,
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
			logrus.Warnf("Invalid snapshot found. Ignoring it:%s\n", name)
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
