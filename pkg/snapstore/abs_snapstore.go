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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"
	"sync"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/sirupsen/logrus"

	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
)

const (
	absCredentialFile     = "AZURE_APPLICATION_CREDENTIALS"
	absCredentialJSONFile = "AZURE_APPLICATION_CREDENTIALS_JSON"
)

// ABSSnapStore is an ABS backed snapstore.
type ABSSnapStore struct {
	containerURL *azblob.ContainerURL
	prefix       string
	// maxParallelChunkUploads hold the maximum number of parallel chunk uploads allowed.
	maxParallelChunkUploads uint
	minChunkSize            int64
	tempDir                 string
}

type absCredentials struct {
	BucketName     string `json:"bucketName"`
	SecretKey      string `json:"storageKey"`
	StorageAccount string `json:"storageAccount"`
}

// NewABSSnapStore create new ABSSnapStore from shared configuration with specified bucket
func NewABSSnapStore(config *brtypes.SnapstoreConfig) (*ABSSnapStore, error) {
	storageAccount, storageKey, err := getCredentials(getEnvPrefixString(config.IsSource))
	if err != nil {
		return nil, err
	}

	credentials, err := azblob.NewSharedKeyCredential(storageAccount, storageKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create shared key credentials: %v", err)
	}

	p := azblob.NewPipeline(credentials, azblob.PipelineOptions{
		Retry: azblob.RetryOptions{
			TryTimeout: downloadTimeout,
		}})
	u, err := url.Parse(fmt.Sprintf("https://%s.%s", storageAccount, brtypes.AzureBlobStorageHostName))
	if err != nil {
		return nil, fmt.Errorf("failed to parse service url: %v", err)
	}
	serviceURL := azblob.NewServiceURL(*u, p)
	containerURL := serviceURL.NewContainerURL(config.Container)

	return GetABSSnapstoreFromClient(config.Container, config.Prefix, config.TempDir, config.MaxParallelChunkUploads, config.MinChunkSize, &containerURL)
}

func getCredentials(prefixString string) (string, string, error) {

	if filename, isSet := os.LookupEnv(prefixString + absCredentialJSONFile); isSet {
		credentials, err := readABSCredentialsJSON(filename)
		if err != nil {
			return "", "", fmt.Errorf("error getting credentials using %v file", filename)
		}
		return credentials.StorageAccount, credentials.SecretKey, nil
	}

	if dir, isSet := os.LookupEnv(prefixString + absCredentialFile); isSet {
		credentials, err := readABSCredentialFiles(dir)
		if err != nil {
			return "", "", fmt.Errorf("error getting credentials from %v dir", dir)
		}
		return credentials.StorageAccount, credentials.SecretKey, nil
	}

	return "", "", fmt.Errorf("unable to get credentials")
}

func readABSCredentialsJSON(filename string) (*absCredentials, error) {
	jsonData, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	return absCredentialsFromJSON(jsonData)
}

// absCredentialsFromJSON obtains ABS credentials from a JSON value.
func absCredentialsFromJSON(jsonData []byte) (*absCredentials, error) {
	absConfig := &absCredentials{}
	if err := json.Unmarshal(jsonData, absConfig); err != nil {
		return nil, err
	}

	return absConfig, nil
}

func readABSCredentialFiles(dirname string) (*absCredentials, error) {
	absConfig := &absCredentials{}

	files, err := os.ReadDir(dirname)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if file.Name() == "storageAccount" {
			data, err := os.ReadFile(dirname + "/storageAccount")
			if err != nil {
				return nil, err
			}
			absConfig.StorageAccount = string(data)
		} else if file.Name() == "storageKey" {
			data, err := os.ReadFile(dirname + "/storageKey")
			if err != nil {
				return nil, err
			}
			absConfig.SecretKey = string(data)
		}
	}

	if err := isABSConfigEmpty(absConfig); err != nil {
		return nil, err
	}
	return absConfig, nil
}

// GetABSSnapstoreFromClient returns a new ABS object for a given container using the supplied storageClient
func GetABSSnapstoreFromClient(container, prefix, tempDir string, maxParallelChunkUploads uint, minChunkSize int64, containerURL *azblob.ContainerURL) (*ABSSnapStore, error) {
	// Check if supplied container exists
	ctx, cancel := context.WithTimeout(context.TODO(), providerConnectionTimeout)
	defer cancel()
	_, err := containerURL.GetProperties(ctx, azblob.LeaseAccessConditions{})
	if err != nil {
		aer, ok := err.(azblob.StorageError)
		if !ok {
			return nil, fmt.Errorf("failed to get properties of container %v with err, %v", container, err.Error())
		}
		if aer.ServiceCode() != azblob.ServiceCodeContainerNotFound {
			return nil, fmt.Errorf("failed to get properties of container %v with err, %v", container, aer.Error())
		}
		return nil, fmt.Errorf("container %s does not exist", container)
	}
	return &ABSSnapStore{
		prefix:                  prefix,
		containerURL:            containerURL,
		maxParallelChunkUploads: maxParallelChunkUploads,
		minChunkSize:            minChunkSize,
		tempDir:                 tempDir,
	}, nil
}

// Fetch should open reader for the snapshot file from store
func (a *ABSSnapStore) Fetch(snap brtypes.Snapshot) (io.ReadCloser, error) {
	blobName := path.Join(snap.Prefix, snap.SnapDir, snap.SnapName)
	blob := a.containerURL.NewBlobURL(blobName)
	resp, err := blob.Download(context.Background(), io.SeekStart, azblob.CountToEnd, azblob.BlobAccessConditions{}, false)
	if err != nil {
		return nil, fmt.Errorf("failed to download the blob %s with error:%v", blobName, err)
	}
	return resp.Body(azblob.RetryReaderOptions{}), nil
}

// List will return sorted list with all snapshot files on store.
func (a *ABSSnapStore) List() (brtypes.SnapList, error) {
	prefixTokens := strings.Split(a.prefix, "/")
	// Last element of the tokens is backup version
	// Consider the parent of the backup version level (Required for Backward Compatibility)
	prefix := path.Join(strings.Join(prefixTokens[:len(prefixTokens)-1], "/"))
	var snapList brtypes.SnapList
	opts := azblob.ListBlobsSegmentOptions{Prefix: prefix}
	for marker := (azblob.Marker{}); marker.NotDone(); {
		// Get a result segment starting with the blob indicated by the current Marker.
		listBlob, err := a.containerURL.ListBlobsFlatSegment(context.TODO(), marker, opts)
		if err != nil {
			return nil, fmt.Errorf("failed to list the blobs, error: %v", err)
		}
		marker = listBlob.NextMarker

		// Process the blobs returned in this result segment
		for _, blob := range listBlob.Segment.BlobItems {
			if strings.Contains(blob.Name, backupVersionV1) || strings.Contains(blob.Name, backupVersionV2) {
				//the blob may contain the full path in its name including the prefix
				blobName := strings.TrimPrefix(blob.Name, prefix)
				s, err := ParseSnapshot(path.Join(prefix, blobName))
				if err != nil {
					logrus.Warnf("Invalid snapshot found. Ignoring it:%s\n", blob.Name)
				} else {
					snapList = append(snapList, s)
				}
			}
		}
	}
	sort.Sort(snapList)
	return snapList, nil
}

// Save will write the snapshot to store
func (a *ABSSnapStore) Save(snap brtypes.Snapshot, rc io.ReadCloser) error {
	// Save it locally
	tmpfile, err := os.CreateTemp(a.tempDir, tmpBackupFilePrefix)
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
		chunkSize  = a.minChunkSize
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

	for i := uint(0); i < a.maxParallelChunkUploads; i++ {
		wg.Add(1)
		go a.blockUploader(&wg, cancelCh, &snap, tmpfile, chunkUploadCh, resCh)
	}
	logrus.Infof("Uploading snapshot of size: %d, chunkSize: %d, noOfChunks: %d", size, chunkSize, noOfChunks)
	for offset, index := int64(0), 1; offset < size; offset += int64(chunkSize) {
		newChunk := chunk{
			offset: offset,
			size:   chunkSize,
			id:     index,
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
	logrus.Info("All chunk uploaded successfully. Uploading blocklist.")
	blobName := path.Join(adaptPrefix(&snap, a.prefix), snap.SnapDir, snap.SnapName)
	blob := a.containerURL.NewBlockBlobURL(blobName)
	var blockList []string
	for partNumber := int64(1); partNumber <= noOfChunks; partNumber++ {
		blockID := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%010d", partNumber)))
		blockList = append(blockList, blockID)
	}
	ctx, cancel := context.WithTimeout(context.TODO(), chunkUploadTimeout)
	defer cancel()
	if _, err := blob.CommitBlockList(ctx, blockList, azblob.BlobHTTPHeaders{}, azblob.Metadata{}, azblob.BlobAccessConditions{}); err != nil {
		return fmt.Errorf("failed uploading blocklist for snapshot with error: %v", err)
	}
	logrus.Info("Blocklist uploaded successfully.")
	return nil
}

func (a *ABSSnapStore) uploadBlock(snap *brtypes.Snapshot, file *os.File, offset, chunkSize int64) error {
	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}

	size := fileInfo.Size() - offset
	if size > chunkSize {
		size = chunkSize
	}

	sr := io.NewSectionReader(file, offset, size)
	blobName := path.Join(adaptPrefix(snap, a.prefix), snap.SnapDir, snap.SnapName)
	blob := a.containerURL.NewBlockBlobURL(blobName)
	partNumber := ((offset / chunkSize) + 1)
	blockID := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%010d", partNumber)))
	ctx, cancel := context.WithTimeout(context.TODO(), chunkUploadTimeout)
	defer cancel()
	var transactionMD5 []byte
	if _, err := blob.StageBlock(ctx, blockID, sr, azblob.LeaseAccessConditions{}, transactionMD5); err != nil {
		return fmt.Errorf("failed to upload chunk offset: %d, blob: %s, error: %v", offset, blobName, err)
	}
	return nil
}

func (a *ABSSnapStore) blockUploader(wg *sync.WaitGroup, stopCh <-chan struct{}, snap *brtypes.Snapshot, file *os.File, chunkUploadCh chan chunk, errCh chan<- chunkUploadResult) {
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
			err := a.uploadBlock(snap, file, chunk.offset, chunk.size)
			errCh <- chunkUploadResult{
				err:   err,
				chunk: &chunk,
			}
		}
	}
}

// Delete should delete the snapshot file from store
func (a *ABSSnapStore) Delete(snap brtypes.Snapshot) error {
	blobName := path.Join(snap.Prefix, snap.SnapDir, snap.SnapName)
	blob := a.containerURL.NewBlobURL(blobName)
	if _, err := blob.Delete(context.TODO(), azblob.DeleteSnapshotsOptionInclude, azblob.BlobAccessConditions{}); err != nil {
		return fmt.Errorf("failed to delete blob %s with error: %v", blobName, err)
	}
	return nil
}

// ABSSnapStoreHash calculates and returns the hash of azure object storage snapstore secret.
func ABSSnapStoreHash(config *brtypes.SnapstoreConfig) (string, error) {
	if _, isSet := os.LookupEnv(absCredentialFile); isSet {
		if dir := os.Getenv(absCredentialFile); dir != "" {
			absConfig, err := readABSCredentialFiles(dir)
			if err != nil {
				return "", fmt.Errorf("error getting credentials from %v directory", dir)
			}
			return getABSHash(absConfig), nil
		}
	}

	if _, isSet := os.LookupEnv(absCredentialJSONFile); isSet {
		if filename := os.Getenv(absCredentialJSONFile); filename != "" {
			absConfig, err := readABSCredentialsJSON(filename)
			if err != nil {
				return "", fmt.Errorf("error getting credentials using %v file", filename)
			}
			return getABSHash(absConfig), nil
		}
	}

	return "", nil
}

func getABSHash(config *absCredentials) string {
	data := fmt.Sprintf("%s%s", config.SecretKey, config.StorageAccount)
	return getHash(data)
}

func isABSConfigEmpty(config *absCredentials) error {
	if len(config.SecretKey) != 0 && len(config.StorageAccount) != 0 {
		return nil
	}
	return fmt.Errorf("azure object storage credentials: storageKey or storageAccount is missing")
}
