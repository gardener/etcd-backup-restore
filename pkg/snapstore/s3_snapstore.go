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
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/sirupsen/logrus"
	"k8s.io/utils/pointer"

	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
)

const (
	// Total number of chunks to be uploaded must be one less than maximum limit allowed.
	s3NoOfChunk        int64 = 9999
	awsSecretAccessKey       = "AWS_SECRET_ACCESS_KEY"
	awsAcessKeyID            = "AWS_ACCESS_KEY_ID"
	awsRegion                = "AWS_REGION"
)

// S3SnapStore is snapstore with AWS S3 object store as backend
type S3SnapStore struct {
	prefix    string
	client    s3iface.S3API
	bucket    string
	multiPart sync.Mutex
	// maxParallelChunkUploads hold the maximum number of parallel chunk uploads allowed.
	maxParallelChunkUploads uint
	tempDir                 string
}

// NewS3SnapStore create new S3SnapStore from shared configuration with specified bucket
func NewS3SnapStore(config *brtypes.SnapstoreConfig) (*S3SnapStore, error) {
	return newS3FromSessionOpt(config.Container, config.Prefix, config.TempDir, config.MaxParallelChunkUploads, getSessionOptions(getEnvPrefixString(config.IsSource)))
}

// newS3FromSessionOpt will create the new S3 snapstore object from S3 session options
func newS3FromSessionOpt(bucket, prefix, tempDir string, maxParallelChunkUploads uint, so session.Options) (*S3SnapStore, error) {
	sess, err := session.NewSessionWithOptions(so)
	if err != nil {
		return nil, fmt.Errorf("new AWS session failed: %v", err)
	}
	cli := s3.New(sess)
	return NewS3FromClient(bucket, prefix, tempDir, maxParallelChunkUploads, cli), nil
}

func getSessionOptions(prefixString string) session.Options {
	if _, isSet := os.LookupEnv(prefixString + awsAcessKeyID); isSet {
		return session.Options{
			Config: aws.Config{
				Credentials: credentials.NewStaticCredentials(os.Getenv(prefixString+awsAcessKeyID), os.Getenv(prefixString+awsSecretAccessKey), ""),
				Region:      pointer.StringPtr(os.Getenv(awsRegion)),
			},
		}
	}
	return session.Options{
		// Setting this is equal to the AWS_SDK_LOAD_CONFIG environment variable was set.
		// We want to save the work to set AWS_SDK_LOAD_CONFIG=1 outside.
		SharedConfigState: session.SharedConfigEnable,
	}
}

// NewS3FromClient will create the new S3 snapstore object from S3 client
func NewS3FromClient(bucket, prefix, tempDir string, maxParallelChunkUploads uint, cli s3iface.S3API) *S3SnapStore {
	return &S3SnapStore{
		bucket:                  bucket,
		prefix:                  prefix,
		client:                  cli,
		maxParallelChunkUploads: maxParallelChunkUploads,
		tempDir:                 tempDir,
	}
}

// Fetch should open reader for the snapshot file from store
func (s *S3SnapStore) Fetch(snap brtypes.Snapshot) (io.ReadCloser, error) {
	resp, err := s.client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path.Join(snap.Prefix, snap.SnapDir, snap.SnapName)),
	})
	if err != nil {
		return nil, fmt.Errorf("error while accessing %s: %v", path.Join(snap.Prefix, snap.SnapDir, snap.SnapName), err)
	}
	return resp.Body, nil
}

// Save will write the snapshot to store
func (s *S3SnapStore) Save(snap brtypes.Snapshot, rc io.ReadCloser) error {
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
	// Initiate multi part upload
	ctx := context.TODO()
	ctx, cancel := context.WithTimeout(ctx, chunkUploadTimeout)
	defer cancel()
	prefix := adaptPrefix(&snap, s.prefix)
	uploadOutput, err := s.client.CreateMultipartUploadWithContext(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path.Join(prefix, snap.SnapDir, snap.SnapName)),
	})
	if err != nil {
		return fmt.Errorf("failed to initiate multipart upload %v", err)
	}
	logrus.Infof("Successfully initiated the multipart upload with upload ID : %s", *uploadOutput.UploadId)

	var (
		chunkSize  = int64(math.Max(float64(minChunkSize), float64(size/s3NoOfChunk)))
		noOfChunks = size / chunkSize
	)
	if size%chunkSize != 0 {
		noOfChunks++
	}
	var (
		completedParts = make([]*s3.CompletedPart, noOfChunks)
		chunkUploadCh  = make(chan chunk, noOfChunks)
		resCh          = make(chan chunkUploadResult, noOfChunks)
		wg             sync.WaitGroup
		cancelCh       = make(chan struct{})
	)

	for i := uint(0); i < s.maxParallelChunkUploads; i++ {
		wg.Add(1)
		go s.partUploader(&wg, cancelCh, &snap, tmpfile, uploadOutput.UploadId, completedParts, chunkUploadCh, resCh)
	}
	logrus.Infof("Uploading snapshot of size: %d, chunkSize: %d, noOfChunks: %d", size, chunkSize, noOfChunks)

	for offset, index := int64(0), 1; offset < size; offset += int64(chunkSize) {
		newChunk := chunk{
			id:     index,
			offset: offset,
			size:   chunkSize,
		}
		logrus.Debugf("Triggering chunk upload for offset: %d", offset)
		chunkUploadCh <- newChunk
		index++
	}
	logrus.Infof("Triggered chunk upload for all chunks, total: %d", noOfChunks)
	snapshotErr := collectChunkUploadError(chunkUploadCh, resCh, cancelCh, noOfChunks)
	wg.Wait()

	if snapshotErr != nil {
		ctx := context.TODO()
		ctx, cancel := context.WithTimeout(ctx, chunkUploadTimeout)
		defer cancel()
		logrus.Infof("Aborting the multipart upload with upload ID : %s", *uploadOutput.UploadId)
		_, err = s.client.AbortMultipartUploadWithContext(ctx, &s3.AbortMultipartUploadInput{
			Bucket:   &s.bucket,
			Key:      aws.String(path.Join(prefix, snap.SnapDir, snap.SnapName)),
			UploadId: uploadOutput.UploadId,
		})
	} else {
		ctx = context.TODO()
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		logrus.Infof("Finishing the multipart upload with upload ID : %s", *uploadOutput.UploadId)
		_, err = s.client.CompleteMultipartUploadWithContext(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:   &s.bucket,
			Key:      aws.String(path.Join(prefix, snap.SnapDir, snap.SnapName)),
			UploadId: uploadOutput.UploadId,
			MultipartUpload: &s3.CompletedMultipartUpload{
				Parts: completedParts,
			},
		})
	}

	if err != nil {
		return fmt.Errorf("failed completing snapshot upload with error %v", err)
	}
	if snapshotErr != nil {
		return fmt.Errorf("failed uploading chunk, id: %d, offset: %d, error: %v", snapshotErr.chunk.id, snapshotErr.chunk.offset, snapshotErr.err)
	}
	return nil
}

func (s *S3SnapStore) uploadPart(snap *brtypes.Snapshot, file *os.File, uploadID *string, completedParts []*s3.CompletedPart, offset, chunkSize int64) error {
	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}
	size := fileInfo.Size() - offset
	if size > chunkSize {
		size = chunkSize
	}

	sr := io.NewSectionReader(file, offset, size)
	ctx, cancel := context.WithTimeout(context.TODO(), chunkUploadTimeout)
	defer cancel()
	partNumber := ((offset / chunkSize) + 1)
	in := &s3.UploadPartInput{
		Bucket:     aws.String(s.bucket),
		Key:        aws.String(path.Join(adaptPrefix(snap, s.prefix), snap.SnapDir, snap.SnapName)),
		PartNumber: &partNumber,
		UploadId:   uploadID,
		Body:       sr,
	}

	part, err := s.client.UploadPartWithContext(ctx, in)
	if err == nil {
		completedPart := &s3.CompletedPart{
			ETag:       part.ETag,
			PartNumber: &partNumber,
		}
		completedParts[partNumber-1] = completedPart
	}
	return err
}

func (s *S3SnapStore) partUploader(wg *sync.WaitGroup, stopCh <-chan struct{}, snap *brtypes.Snapshot, file *os.File, uploadID *string, completedParts []*s3.CompletedPart, chunkUploadCh <-chan chunk, errCh chan<- chunkUploadResult) {
	defer wg.Done()
	for {
		select {
		case <-stopCh:
			return
		case chunk, ok := <-chunkUploadCh:
			if !ok {
				return
			}
			logrus.Infof("Uploading chunk with id: %d, offset: %d, attempt: %d", chunk.id, chunk.offset, chunk.attempt)
			err := s.uploadPart(snap, file, uploadID, completedParts, chunk.offset, chunk.size)
			errCh <- chunkUploadResult{
				err:   err,
				chunk: &chunk,
			}
		}
	}
}

// List will return sorted list with all snapshot files on store.
func (s *S3SnapStore) List() (brtypes.SnapList, error) {
	prefixTokens := strings.Split(s.prefix, "/")
	// Last element of the tokens is backup version
	// Consider the parent of the backup version level (Required for Backward Compatibility)
	prefix := path.Join(strings.Join(prefixTokens[:len(prefixTokens)-1], "/"))

	var snapList brtypes.SnapList
	in := &s3.ListObjectsInput{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	}
	err := s.client.ListObjectsPages(in, func(page *s3.ListObjectsOutput, lastPage bool) bool {
		for _, key := range page.Contents {
			k := (*key.Key)[len(*page.Prefix):]
			if strings.Contains(k, backupVersionV1) || strings.Contains(k, backupVersionV2) {
				snap, err := ParseSnapshot(path.Join(prefix, k))
				if err != nil {
					// Warning
					logrus.Warnf("Invalid snapshot found. Ignoring it: %s", k)
				} else {
					snapList = append(snapList, snap)
				}
			}
		}
		return !lastPage
	})
	if err != nil {
		return nil, err
	}

	sort.Sort(snapList)
	return snapList, nil
}

// Delete should delete the snapshot file from store
func (s *S3SnapStore) Delete(snap brtypes.Snapshot) error {
	_, err := s.client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path.Join(snap.Prefix, snap.SnapDir, snap.SnapName)),
	})
	return err
}
