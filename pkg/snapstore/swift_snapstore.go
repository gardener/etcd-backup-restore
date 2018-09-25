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
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/gophercloud/gophercloud"
	"github.com/gophercloud/gophercloud/openstack"
	"github.com/gophercloud/gophercloud/openstack/objectstorage/v1/objects"
	"github.com/gophercloud/gophercloud/pagination"
)

// SwiftSnapStore is snapstore with Openstack Swift as backend
type SwiftSnapStore struct {
	SnapStore
	prefix string
	client *gophercloud.ServiceClient
	bucket string
}

const (
	swiftNoOfChunk int64 = 1000 //Default configuration in swift installation
)

// NewSwiftSnapStore create new SwiftSnapStore from shared configuration with specified bucket
func NewSwiftSnapStore(bucket, prefix string) (*SwiftSnapStore, error) {
	authOpts, err := openstack.AuthOptionsFromEnv()
	if err != nil {
		return nil, err
	}
	provider, err := openstack.AuthenticatedClient(authOpts)
	if err != nil {
		return nil, err

	}
	client, err := openstack.NewObjectStorageV1(provider, gophercloud.EndpointOpts{})
	if err != nil {
		return nil, err

	}
	return &SwiftSnapStore{
		prefix: prefix,
		client: client,
		bucket: bucket,
	}, nil

}

// Fetch should open reader for the snapshot file from store
func (s *SwiftSnapStore) Fetch(snap Snapshot) (io.ReadCloser, error) {
	resp := objects.Download(s.client, s.bucket, path.Join(s.prefix, snap.SnapDir, snap.SnapName), nil)
	return resp.Body, resp.Err
}

// Save will write the snapshot to store
func (s *SwiftSnapStore) Save(snap Snapshot, r io.Reader) error {
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
		chunkSize  = int64(math.Max(float64(minChunkSize), float64(size/swiftNoOfChunk)))
		noOfChunks = size / chunkSize
	)
	if size%chunkSize != 0 {
		noOfChunks++
	}

	logrus.Infof("Uploading snapshot of size: %d, chunkSize: %d, noOfChunks: %d", size, chunkSize, noOfChunks)
	for offset := int64(0); offset <= size; offset += int64(chunkSize) {
		go retryChunkUpload(s, &snap, tmpfile, offset, chunkSize, errCh)
	}

	snapshotErr := collectChunkUploadError(errCh, noOfChunks)
	if len(snapshotErr) == 0 {
		logrus.Info("All chunk uploaded successfully. Uploading manifest.")
		b := make([]byte, 0)
		opts := objects.CreateOpts{
			Content:        bytes.NewReader(b),
			ContentLength:  chunkSize,
			ObjectManifest: path.Join(s.bucket, s.prefix, snap.SnapDir, snap.SnapName),
		}
		res := objects.Create(s.client, s.bucket, path.Join(s.prefix, snap.SnapDir, snap.SnapName), opts)
		return res.Err
	}
	var collectedErr []string
	for _, chunkErr := range snapshotErr {
		collectedErr = append(collectedErr, fmt.Sprintf("failed uploading chunk with offset %d with error %v", chunkErr.offset, chunkErr.err))
	}
	return fmt.Errorf(strings.Join(collectedErr, "\n"))
}

func uploadChunk(s *SwiftSnapStore, snap *Snapshot, file *os.File, offset, chunkSize int64) error {
	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}
	size := fileInfo.Size() - offset
	if size > chunkSize {
		size = chunkSize
	}

	sr := io.NewSectionReader(file, offset, size)

	opts := objects.CreateOpts{
		Content:       sr,
		ContentLength: size,
	}
	partNumber := ((offset / chunkSize) + 1)
	res := objects.Create(s.client, s.bucket, path.Join(s.prefix, snap.SnapDir, snap.SnapName, fmt.Sprintf("%010d", partNumber)), opts)
	return res.Err
}

func retryChunkUpload(s *SwiftSnapStore, snap *Snapshot, file *os.File, offset, chunkSize int64, errCh chan<- chunkUploadError) {
	var (
		maxAttempts uint = 5
		curAttempt  uint = 1
		err         error
	)
	for {
		logrus.Infof("Uploading chunk with offset : %d, attempt: %d", offset, curAttempt)
		err = uploadChunk(s, snap, file, offset, chunkSize)
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
	return
}

// List will list the snapshots from store
func (s *SwiftSnapStore) List() (SnapList, error) {

	opts := &objects.ListOpts{
		Full:   false,
		Prefix: s.prefix,
	}
	// Retrieve a pager (i.e. a paginated collection)
	pager := objects.List(s.client, s.bucket, opts)
	var snapList SnapList
	// Define an anonymous function to be executed on each page's iteration
	err := pager.EachPage(func(page pagination.Page) (bool, error) {

		objectList, err := objects.ExtractNames(page)
		if err != nil {
			return false, err
		}
		for _, object := range objectList {
			name := strings.Replace(object, s.prefix+"/", "", 1)
			snap, err := ParseSnapshot(name)
			if err != nil {
				// Warning: the file can be a non snapshot file. Do not return error.
				logrus.Warnf("Invalid snapshot found. Ignoring it:%s, %v", name, err)
			} else {
				snapList = append(snapList, snap)
			}
		}
		return true, nil

	})
	if err != nil {
		return nil, err
	}

	sort.Sort(snapList)
	return snapList, nil

}

// Delete should delete the snapshot file from store
func (s *SwiftSnapStore) Delete(snap Snapshot) error {
	result := objects.Delete(s.client, s.bucket, path.Join(s.prefix, snap.SnapDir, snap.SnapName), nil)
	return result.Err
}
