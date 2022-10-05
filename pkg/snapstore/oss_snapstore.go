// Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
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
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"sort"
	"strings"
	"sync"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/sirupsen/logrus"

	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
)

// OSSBucket is an interface for oss.Bucket used in snapstore
type OSSBucket interface {
	GetObject(objectKey string, options ...oss.Option) (io.ReadCloser, error)
	InitiateMultipartUpload(objectKey string, options ...oss.Option) (oss.InitiateMultipartUploadResult, error)
	CompleteMultipartUpload(imur oss.InitiateMultipartUploadResult, parts []oss.UploadPart, options ...oss.Option) (oss.CompleteMultipartUploadResult, error)
	ListObjects(options ...oss.Option) (oss.ListObjectsResult, error)
	DeleteObject(objectKey string, options ...oss.Option) error
	UploadPart(imur oss.InitiateMultipartUploadResult, reader io.Reader, partSize int64, partNumber int, options ...oss.Option) (oss.UploadPart, error)
	AbortMultipartUpload(imur oss.InitiateMultipartUploadResult, options ...oss.Option) error
}

const (
	// Total number of chunks to be uploaded must be one less than maximum limit allowed.
	ossNoOfChunk          int64 = 9999
	aliCredentialFile           = "ALICLOUD_APPLICATION_CREDENTIALS"
	aliCredentialJSONFile       = "ALICLOUD_APPLICATION_CREDENTIALS_JSON"
)

type authOptions struct {
	Endpoint   string `json:"storageEndpoint"`
	AccessID   string `json:"accessKeyID"`
	AccessKey  string `json:"accessKeySecret"`
	BucketName string `json:"bucketName"`
}

// OSSSnapStore is snapstore with Alicloud OSS object store as backend
type OSSSnapStore struct {
	prefix                  string
	bucket                  OSSBucket
	multiPart               sync.Mutex
	maxParallelChunkUploads uint
	minChunkSize            int64
	tempDir                 string
}

// NewOSSSnapStore create new OSSSnapStore from shared configuration with specified bucket
func NewOSSSnapStore(config *brtypes.SnapstoreConfig) (*OSSSnapStore, error) {
	ao, err := getAuthOptions(getEnvPrefixString(config.IsSource))
	if err != nil {
		return nil, err
	}
	return newOSSFromAuthOpt(config.Container, config.Prefix, config.TempDir, config.MaxParallelChunkUploads, config.MinChunkSize, *ao)
}

func newOSSFromAuthOpt(bucket, prefix, tempDir string, maxParallelChunkUploads uint, minChunkSize int64, ao authOptions) (*OSSSnapStore, error) {
	client, err := oss.New(ao.Endpoint, ao.AccessID, ao.AccessKey)
	if err != nil {
		return nil, err
	}

	bucketOSS, err := client.Bucket(bucket)
	if err != nil {
		return nil, err
	}

	return NewOSSFromBucket(prefix, tempDir, maxParallelChunkUploads, minChunkSize, bucketOSS), nil
}

// NewOSSFromBucket will create the new OSS snapstore object from OSS bucket
func NewOSSFromBucket(prefix, tempDir string, maxParallelChunkUploads uint, minChunkSize int64, bucket OSSBucket) *OSSSnapStore {
	return &OSSSnapStore{
		prefix:                  prefix,
		bucket:                  bucket,
		maxParallelChunkUploads: maxParallelChunkUploads,
		minChunkSize:            minChunkSize,
		tempDir:                 tempDir,
	}
}

// Fetch should open reader for the snapshot file from store
func (s *OSSSnapStore) Fetch(snap brtypes.Snapshot) (io.ReadCloser, error) {
	body, err := s.bucket.GetObject(path.Join(snap.Prefix, snap.SnapDir, snap.SnapName))
	if err != nil {
		return nil, err
	}
	return body, nil
}

// Save will write the snapshot to store
func (s *OSSSnapStore) Save(snap brtypes.Snapshot, rc io.ReadCloser) error {
	tmpfile, err := os.CreateTemp(s.tempDir, tmpBackupFilePrefix)
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
	_, err = tmpfile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	var (
		chunkSize  = int64(math.Max(float64(s.minChunkSize), float64(size/ossNoOfChunk)))
		noOfChunks = size / chunkSize
	)
	if size%chunkSize != 0 {
		noOfChunks++
	}

	ossChunks, err := oss.SplitFileByPartNum(tmpfile.Name(), int(noOfChunks))
	if err != nil {
		return err
	}

	imur, err := s.bucket.InitiateMultipartUpload(path.Join(adaptPrefix(&snap, s.prefix), snap.SnapDir, snap.SnapName))
	if err != nil {
		return err
	}

	var (
		completedParts = make([]oss.UploadPart, noOfChunks)
		chunkUploadCh  = make(chan chunk, noOfChunks)
		resCh          = make(chan chunkUploadResult, noOfChunks)
		cancelCh       = make(chan struct{})
		wg             sync.WaitGroup
	)

	for i := uint(0); i < s.maxParallelChunkUploads; i++ {
		wg.Add(1)
		go s.partUploader(&wg, imur, tmpfile, completedParts, chunkUploadCh, cancelCh, resCh)
	}

	for _, ossChunk := range ossChunks {
		chunk := chunk{
			offset: ossChunk.Offset,
			size:   ossChunk.Size,
			id:     ossChunk.Number,
		}
		logrus.Debugf("Triggering chunk upload for offset: %d", chunk.offset)
		chunkUploadCh <- chunk
	}

	logrus.Infof("Triggered chunk upload for all chunks, total: %d", noOfChunks)
	snapshotErr := collectChunkUploadError(chunkUploadCh, resCh, cancelCh, noOfChunks)
	wg.Wait()

	if snapshotErr == nil {
		_, err := s.bucket.CompleteMultipartUpload(imur, completedParts)
		if err != nil {
			return err
		}
		logrus.Infof("Finishing the multipart upload with upload ID : %s", imur.UploadID)
	} else {
		logrus.Infof("Aborting the multipart upload with upload ID : %s", imur.UploadID)
		err := s.bucket.AbortMultipartUpload(imur)
		if err != nil {
			return snapshotErr.err
		}
	}

	return nil
}

func (s *OSSSnapStore) partUploader(wg *sync.WaitGroup, imur oss.InitiateMultipartUploadResult, file *os.File, completedParts []oss.UploadPart, chunkUploadCh <-chan chunk, stopCh <-chan struct{}, errCh chan<- chunkUploadResult) {
	defer wg.Done()
	for {
		select {
		case <-stopCh:
			return
		case chunk, ok := <-chunkUploadCh:
			if !ok {
				return
			}
			logrus.Infof("Uploading chunk with id: %d, offset: %d, size: %d", chunk.id, chunk.offset, chunk.size)
			err := s.uploadPart(imur, file, completedParts, chunk.offset, chunk.size, chunk.id)
			errCh <- chunkUploadResult{
				err:   err,
				chunk: &chunk,
			}
		}
	}
}

func (s *OSSSnapStore) uploadPart(imur oss.InitiateMultipartUploadResult, file *os.File, completedParts []oss.UploadPart, offset, chunkSize int64, number int) error {
	fd := io.NewSectionReader(file, offset, chunkSize)
	part, err := s.bucket.UploadPart(imur, fd, chunkSize, number)

	if err == nil {
		completedParts[number-1] = part
	}
	return err
}

// List will return sorted list with all snapshot files on store.
func (s *OSSSnapStore) List() (brtypes.SnapList, error) {
	prefixTokens := strings.Split(s.prefix, "/")
	// Last element of the tokens is backup version
	// Consider the parent of the backup version level (Required for Backward Compatibility)
	prefix := path.Join(strings.Join(prefixTokens[:len(prefixTokens)-1], "/"))

	var snapList brtypes.SnapList

	marker := ""
	for {
		lsRes, err := s.bucket.ListObjects(oss.Marker(marker), oss.Prefix(prefix))
		if err != nil {
			return nil, err
		}
		for _, object := range lsRes.Objects {
			if strings.Contains(object.Key, backupVersionV1) || strings.Contains(object.Key, backupVersionV2) {
				snap, err := ParseSnapshot(object.Key)
				if err != nil {
					// Warning
					logrus.Warnf("Invalid snapshot found. Ignoring it: %s", object.Key)
				} else {
					snapList = append(snapList, snap)
				}
			}
		}
		if lsRes.IsTruncated {
			marker = lsRes.NextMarker
		} else {
			break
		}
	}
	sort.Sort(snapList)

	return snapList, nil
}

// Delete should delete the snapshot file from store
func (s *OSSSnapStore) Delete(snap brtypes.Snapshot) error {
	return s.bucket.DeleteObject(path.Join(snap.Prefix, snap.SnapDir, snap.SnapName))
}

func getAuthOptions(prefix string) (*authOptions, error) {

	if filename, isSet := os.LookupEnv(prefix + aliCredentialJSONFile); isSet {
		ao, err := readALICredentialsJSON(filename)
		if err != nil {
			return nil, fmt.Errorf("error getting credentials using %v file", filename)
		}
		return ao, nil
	}

	if dir, isSet := os.LookupEnv(prefix + aliCredentialFile); isSet {
		ao, err := readALICredentialFiles(dir)
		if err != nil {
			return nil, fmt.Errorf("error getting credentials from %v directory", dir)
		}
		return ao, nil
	}

	return nil, fmt.Errorf("unable to get credentials")
}

func readALICredentialsJSON(filename string) (*authOptions, error) {
	jsonData, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	return aliCredentialsFromJSON(jsonData)
}

// aliCredentialsFromJSON obtains AliCloud OSS credentials from a JSON value.
func aliCredentialsFromJSON(jsonData []byte) (*authOptions, error) {
	aliConfig := &authOptions{}
	if err := json.Unmarshal(jsonData, aliConfig); err != nil {
		return nil, err
	}

	return aliConfig, nil
}

func readALICredentialFiles(dirname string) (*authOptions, error) {
	aliConfig := &authOptions{}

	files, err := os.ReadDir(dirname)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if file.Name() == "storageEndpoint" {
			data, err := os.ReadFile(dirname + "/storageEndpoint")
			if err != nil {
				return nil, err
			}
			aliConfig.Endpoint = string(data)
		} else if file.Name() == "accessKeySecret" {
			data, err := os.ReadFile(dirname + "/accessKeySecret")
			if err != nil {
				return nil, err
			}
			aliConfig.AccessKey = string(data)
		} else if file.Name() == "accessKeyID" {
			data, err := os.ReadFile(dirname + "/accessKeyID")
			if err != nil {
				return nil, err
			}
			aliConfig.AccessID = string(data)
		}
	}

	if err := isOSSConfigEmpty(aliConfig); err != nil {
		return nil, err
	}
	return aliConfig, nil
}

// OSSSnapStoreHash calculates and returns the hash of aliCloud OSS snapstore secret.
func OSSSnapStoreHash(config *brtypes.SnapstoreConfig) (string, error) {
	if _, isSet := os.LookupEnv(aliCredentialFile); isSet {
		if dir := os.Getenv(aliCredentialFile); dir != "" {
			aliConfig, err := readALICredentialFiles(dir)
			if err != nil {
				return "", fmt.Errorf("error getting credentials from %v directory", dir)
			}
			return getOSSHash(aliConfig), nil
		}
	}

	if _, isSet := os.LookupEnv(aliCredentialJSONFile); isSet {
		if filename := os.Getenv(aliCredentialJSONFile); filename != "" {
			aliConfig, err := readALICredentialsJSON(filename)
			if err != nil {
				return "", fmt.Errorf("error getting credentials using %v file", filename)
			}
			return getOSSHash(aliConfig), nil
		}
	}
	return "", nil
}

func getOSSHash(config *authOptions) string {
	data := fmt.Sprintf("%s%s%s", config.AccessID, config.AccessKey, config.Endpoint)
	return getHash(data)
}

func isOSSConfigEmpty(config *authOptions) error {
	if len(config.AccessID) != 0 && len(config.AccessKey) != 0 && len(config.Endpoint) != 0 {
		return nil
	}
	return fmt.Errorf("aliCloud OSS credentials: accessKeyID, accessKeySecret or storageEndpoint is missing")
}
