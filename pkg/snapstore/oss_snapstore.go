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
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path"
	"sort"
	"strings"
	"sync"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/sirupsen/logrus"
)

// OSSBucket is an interface for oss.Bucket used in snapstore
type OSSBucket interface {
	IsObjectExist(objectKey string, options ...oss.Option) (bool, error)
	GetObject(objectKey string, options ...oss.Option) (io.ReadCloser, error)
	InitiateMultipartUpload(objectKey string, options ...oss.Option) (oss.InitiateMultipartUploadResult, error)
	CompleteMultipartUpload(imur oss.InitiateMultipartUploadResult, parts []oss.UploadPart, options ...oss.Option) (oss.CompleteMultipartUploadResult, error)
	ListObjects(options ...oss.Option) (oss.ListObjectsResult, error)
	DeleteObject(objectKey string, options ...oss.Option) error
	UploadPart(imur oss.InitiateMultipartUploadResult, reader io.Reader, partSize int64, partNumber int, options ...oss.Option) (oss.UploadPart, error)
	AbortMultipartUpload(imur oss.InitiateMultipartUploadResult, options ...oss.Option) error
}

const (
	ossNoOfChunk    int64 = 10000
	ossEndPoint           = "ALICLOUD_ENDPOINT"
	accessKeyID           = "ALICLOUD_ACCESS_KEY_ID"
	accessKeySecret       = "ALICLOUD_ACCESS_KEY_SECRET"
)

type authOptions struct {
	endpoint  string
	accessID  string
	accessKey string
}

// OSSSnapStore is snapstore with Alicloud OSS object store as backend
type OSSSnapStore struct {
	prefix                  string
	bucket                  OSSBucket
	multiPart               sync.Mutex
	maxParallelChunkUploads uint
	tempDir                 string
}

// NewOSSSnapStore create new OSSSnapStore from shared configuration with specified bucket
func NewOSSSnapStore(bucket, prefix, tempDir string, maxParallelChunkUploads uint) (*OSSSnapStore, error) {
	ao, err := authOptionsFromEnv()
	if err != nil {
		return nil, err
	}
	return newOSSFromAuthOpt(bucket, prefix, tempDir, maxParallelChunkUploads, ao)
}

func newOSSFromAuthOpt(bucket, prefix, tempDir string, maxParallelChunkUploads uint, ao authOptions) (*OSSSnapStore, error) {
	client, err := oss.New(ao.endpoint, ao.accessID, ao.accessKey)
	if err != nil {
		return nil, err
	}

	bucketOSS, err := client.Bucket(bucket)
	if err != nil {
		return nil, err
	}

	return NewOSSFromBucket(prefix, tempDir, maxParallelChunkUploads, bucketOSS), nil
}

// NewOSSFromBucket will create the new OSS snapstore object from OSS bucket
func NewOSSFromBucket(prefix, tempDir string, maxParallelChunkUploads uint, bucket OSSBucket) *OSSSnapStore {
	return &OSSSnapStore{
		prefix:                  prefix,
		bucket:                  bucket,
		maxParallelChunkUploads: maxParallelChunkUploads,
		tempDir:                 tempDir,
	}
}

// Fetch should open reader for the snapshot file from store
func (s *OSSSnapStore) Fetch(snap brtypes.Snapshot) (io.ReadCloser, error) {
	prefix := s.prefix

	// If there is no snapdir, fetch from v1
	if snap.SnapDir != "" {
		// Change the prefix to v1 prefix
		prefixTokens := strings.Split(s.prefix, "/")
		prefixTokens = prefixTokens[:len(prefixTokens)-1]
		prefix = path.Join(strings.Join(prefixTokens, "/"), backupVersionV1)
	}
	rc, err := s.bucket.GetObject(path.Join(prefix, snap.SnapDir, snap.SnapName))
	if err != nil {
		return nil, err
	}
	return rc, nil
}

// Save will write the snapshot to store
func (s *OSSSnapStore) Save(snap brtypes.Snapshot, rc io.ReadCloser) error {
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
	_, err = tmpfile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	var (
		chunkSize  = int64(math.Max(float64(minChunkSize), float64(size/ossNoOfChunk)))
		noOfChunks = size / chunkSize
	)
	if size%chunkSize != 0 {
		noOfChunks++
	}

	ossChunks, err := oss.SplitFileByPartNum(tmpfile.Name(), int(noOfChunks))
	if err != nil {
		return err
	}

	imur, err := s.bucket.InitiateMultipartUpload(path.Join(s.prefix, snap.SnapDir, snap.SnapName))
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

// List will list the snapshots from store
func (s *OSSSnapStore) List() (brtypes.SnapList, error) {
	// Probing for v1 and v2 is required for backward compatibility
	// TODO: Remove when backward compatibility is not needed
	var v1, v2 bool = false, false
	var err error
	var ls1, ls2 brtypes.SnapList

	v1, ls1, err = s.isV1Present()
	if err != nil {
		logrus.Warnf("could not decide if v1 prefix is present (Required for backward compatibility): %v", err)
		v1 = false
	}

	v2, ls2, err = s.isV2Present()
	if err != nil {
		logrus.Warnf("could not decide if v2 prefix is present (Required for backward compatibility): %v", err)
		v2 = false
	}

	logrus.Infof("v1 is present : %v, v2 is present : %v", v1, v2)

	snapList := ls2
	// If v1 directory exists (Required for backward compatibility)
	if v1 {
		snapList = append(snapList, ls1...)
	}

	sort.Sort(snapList)
	return snapList, nil
}

func (s *OSSSnapStore) makeSnapList(prefix string) (brtypes.SnapList, error) {
	var snapList brtypes.SnapList

	marker := ""
	for {
		lsRes, err := s.bucket.ListObjects(oss.Marker(marker), oss.Prefix(prefix))
		if err != nil {
			return nil, err
		}
		for _, object := range lsRes.Objects {
			snap, err := ParseSnapshot(object.Key[len(prefix)+1:])
			if err != nil {
				// Warning
				logrus.Warnf("Invalid snapshot found. Ignoring it: %s", object.Key)
			} else {
				snapList = append(snapList, snap)
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
	var err error
	prefix := s.prefix
	if snap.SnapDir != "" {
		// Change the prefix to v1 prefix
		prefixTokens := strings.Split(s.prefix, "/")
		prefixTokens = prefixTokens[:len(prefixTokens)-1]
		prefix = path.Join(strings.Join(prefixTokens, "/"), backupVersionV1)
	}
	err = s.bucket.DeleteObject(path.Join(prefix, snap.SnapDir, snap.SnapName))
	return err
}

// isV1Present checks if v1 is present (Required for backward compatibility)
func (s *OSSSnapStore) isV1Present() (bool, brtypes.SnapList, error) {
	prefixTokens := strings.Split(s.prefix, "/")
	prefixTokens = prefixTokens[:len(prefixTokens)-1]
	prefix := path.Join(strings.Join(prefixTokens, "/"), backupVersionV1)

	ls, err := s.makeSnapList(prefix)
	if err != nil {
		return false, nil, nil
	}

	if len(ls) > 0 {
		return true, ls, nil
	}

	return false, nil, nil
}

// isV2Present checks if v2 prefix is present (Required for backward compatibility)
func (s *OSSSnapStore) isV2Present() (bool, brtypes.SnapList, error) {
	prefixTokens := strings.Split(s.prefix, "/")
	prefixTokens = prefixTokens[:len(prefixTokens)-1]
	prefix := path.Join(strings.Join(prefixTokens, "/"), backupVersionV2)

	ls, err := s.makeSnapList(prefix)
	if err != nil {
		return false, nil, nil
	}

	if len(ls) > 0 {
		return true, ls, nil
	}

	return false, nil, nil
}

func authOptionsFromEnv() (authOptions, error) {
	endpoint, err := GetEnvVarOrError(ossEndPoint)
	if err != nil {
		return authOptions{}, err
	}
	accessID, err := GetEnvVarOrError(accessKeyID)
	if err != nil {
		return authOptions{}, err
	}
	accessKey, err := GetEnvVarOrError(accessKeySecret)
	if err != nil {
		return authOptions{}, err
	}

	ao := authOptions{
		endpoint:  endpoint,
		accessID:  accessID,
		accessKey: accessKey,
	}

	return ao, nil
}
