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
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/sirupsen/logrus"
)

const (
	absStorageAccount = "STORAGE_ACCOUNT"
	absStorageKey     = "STORAGE_KEY"
)

// ABSSnapStore is an ABS backed snapstore.
type ABSSnapStore struct {
	SnapStore
	prefix    string
	container *storage.Container
}

// NewABSSnapStore create new ABSSnapStore from shared configuration with specified bucket
func NewABSSnapStore(container, prefix string) (*ABSSnapStore, error) {
	storageAccount, err := GetEnvVarOrError(absStorageAccount)
	if err != nil {
		return nil, err
	}

	storageKey, err := GetEnvVarOrError(absStorageKey)
	if err != nil {
		return nil, err
	}

	client, err := storage.NewBasicClient(storageAccount, storageKey)
	if err != nil {
		return nil, fmt.Errorf("create ABS client failed: %v", err)
	}

	return GetSnapstoreFromClient(container, prefix, &client)
}

// GetSnapstoreFromClient returns a new ABS object for a given container using the supplied storageClient
func GetSnapstoreFromClient(container, prefix string, storageClient *storage.Client) (*ABSSnapStore, error) {
	client := storageClient.GetBlobService()

	// Check if supplied container exists
	containerRef := client.GetContainerReference(container)
	containerExists, err := containerRef.Exists()
	if err != nil {
		return nil, err
	}

	if !containerExists {
		return nil, fmt.Errorf("container %v does not exist", container)
	}

	return &ABSSnapStore{
		prefix:    prefix,
		container: containerRef,
	}, nil
}

// Fetch should open reader for the snapshot file from store
func (a *ABSSnapStore) Fetch(snap Snapshot) (io.ReadCloser, error) {
	blobName := path.Join(a.prefix, snap.SnapDir, snap.SnapName)
	blob := a.container.GetBlobReference(blobName)
	opts := &storage.GetBlobOptions{}
	return blob.Get(opts)
}

// List will list all snapshot files on store
func (a *ABSSnapStore) List() (SnapList, error) {
	params := storage.ListBlobsParameters{Prefix: path.Join(a.prefix) + "/"}
	resp, err := a.container.ListBlobs(params)
	if err != nil {
		return nil, err
	}

	var snapList SnapList
	for _, blob := range resp.Blobs {
		k := (blob.Name)[len(resp.Prefix):]
		s, err := ParseSnapshot(k)
		if err != nil {
			// Warning
			fmt.Printf("Invalid snapshot found. Ignoring it:%s\n", k)
		} else {
			snapList = append(snapList, s)
		}
	}
	sort.Sort(snapList)
	return snapList, nil
}

// Save will write the snapshot to store
func (a *ABSSnapStore) Save(snap Snapshot, r io.Reader) error {
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
		chunkSize  = minChunkSize
		noOfChunks = size / chunkSize
	)
	if size%chunkSize != 0 {
		noOfChunks++
	}

	logrus.Infof("Uploading snapshot of size: %d, chunkSize: %d, noOfChunks: %d", size, chunkSize, noOfChunks)
	for offset := int64(0); offset <= size; offset += int64(chunkSize) {
		go uploadBlock(a, &snap, tmpfile, offset, chunkSize, errCh)
	}

	snapshotErr := handleBlockUpload(a, &snap, tmpfile, errCh, noOfChunks, chunkSize)
	if len(snapshotErr) == 0 {
		logrus.Info("All chunk uploaded successfully. Uploading blocklist.")
		blobName := path.Join(a.prefix, snap.SnapDir, snap.SnapName)
		blob := a.container.GetBlobReference(blobName)
		var blockList []storage.Block
		for partNumber := int64(1); partNumber <= noOfChunks; partNumber++ {
			block := storage.Block{
				ID:     base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%010d", partNumber))),
				Status: storage.BlockStatusUncommitted,
			}
			blockList = append(blockList, block)
		}
		return blob.PutBlockList(blockList, &storage.PutBlockListOptions{})
	}
	var collectedErr []string
	for _, chunkErr := range snapshotErr {
		collectedErr = append(collectedErr, fmt.Sprintf("failed uploading chunk with offset %010d with error %v", chunkErr.offset, chunkErr.err))
	}
	return fmt.Errorf(strings.Join(collectedErr, "\n"))
}

func uploadBlock(s *ABSSnapStore, snap *Snapshot, file *os.File, offset, chunkSize int64, errCh chan<- chunkUploadError) {
	logrus.Infof("Uploading chunk with offset : %010d", offset)
	if _, err := file.Seek(0, os.SEEK_SET); err != nil {
		errCh <- chunkUploadError{
			err:    fmt.Errorf("failed to set offset in temp file: %v", err),
			offset: offset,
		}
		return
	}

	fileInfo, err := file.Stat()
	if err != nil {
		errCh <- chunkUploadError{
			err:    fmt.Errorf("failed to get size of temp file: %v", err),
			offset: offset,
		}
		return
	}

	size := fileInfo.Size() - offset
	if size > chunkSize {
		size = chunkSize
	}

	sr := io.NewSectionReader(file, offset, chunkSize)
	blobName := path.Join(s.prefix, snap.SnapDir, snap.SnapName)
	blob := s.container.GetBlobReference(blobName)
	partNumber := ((offset / chunkSize) + 1)
	blockID := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%010d", partNumber)))
	err = blob.PutBlockWithLength(blockID, uint64(size), sr, &storage.PutBlockOptions{})
	logrus.Infof("For chunk upload of offset %010d, err %v", offset, err)
	errCh <- chunkUploadError{
		err:    err,
		offset: offset,
	}
	return
}

func handleBlockUpload(s *ABSSnapStore, snap *Snapshot, file *os.File, errCh chan chunkUploadError, noOfChunks, chunkSize int64) []chunkUploadError {
	var (
		snapshotErr     []chunkUploadError
		manifest        = make([]int64, noOfChunks)
		maxAttempts     = int64(5)
		remainingChunks = noOfChunks
	)
	logrus.Infof("No of Chunks:= %d", noOfChunks)
	for {
		chunkErr := <-errCh
		if chunkErr.err != nil {
			manifestIndex := chunkErr.offset / chunkSize
			logrus.Warnf("Failed to upload chunk with offset: %010d at attempt %d  with error: %v", chunkErr.offset, manifest[manifestIndex], chunkErr.err)
			if manifest[manifestIndex] < maxAttempts {
				manifest[manifestIndex]++
				delayTime := (1 << uint(manifest[manifestIndex]))
				logrus.Infof("Will try to upload chunk with offset: %010d at attempt %d  after %d seconds", chunkErr.offset, manifest[manifestIndex], delayTime)
				time.Sleep((time.Duration)(delayTime) * time.Second)
				go uploadBlock(s, snap, file, chunkErr.offset, chunkSize, errCh)
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

// Delete should delete the snapshot file from store
func (a *ABSSnapStore) Delete(snap Snapshot) error {
	blobName := path.Join(a.prefix, snap.SnapDir, snap.SnapName)
	blob := a.container.GetBlobReference(blobName)

	opts := &storage.DeleteBlobOptions{}
	return blob.Delete(opts)
}
