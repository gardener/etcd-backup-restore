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
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/sirupsen/logrus"
)

const (
	tmpDir              = "/tmp"
	tmpBackupFilePrefix = "etcd-backup-"
)

const (
	s3NoOfChunk int64 = 10000 //Default configuration in swift installation
)

// S3SnapStore is snapstore with local disk as backend
type S3SnapStore struct {
	SnapStore
	prefix    string
	client    s3iface.S3API
	bucket    string
	multiPart sync.Mutex
}

// NewS3SnapStore create new S3SnapStore from shared configuration with specified bucket
func NewS3SnapStore(bucket, prefix string) (*S3SnapStore, error) {
	return NewS3FromSessionOpt(bucket, prefix, session.Options{
		// Setting this is equal to the AWS_SDK_LOAD_CONFIG environment variable was set.
		// We want to save the work to set AWS_SDK_LOAD_CONFIG=1 outside.
		SharedConfigState: session.SharedConfigEnable,
	})
}

// NewS3FromSessionOpt will create the new S3 snapstore object from S3 session options
func NewS3FromSessionOpt(bucket, prefix string, so session.Options) (*S3SnapStore, error) {
	sess, err := session.NewSessionWithOptions(so)
	if err != nil {
		return nil, fmt.Errorf("new AWS session failed: %v", err)
	}
	cli := s3.New(sess)
	return NewS3FromClient(bucket, prefix, cli), nil
}

// NewS3FromClient will create the new S3 snapstore object from S3 client
func NewS3FromClient(bucket, prefix string, cli s3iface.S3API) *S3SnapStore {
	return &S3SnapStore{
		bucket: bucket,
		prefix: prefix,
		client: cli,
	}
}

// Fetch should open reader for the snapshot file from store
func (s *S3SnapStore) Fetch(snap Snapshot) (io.ReadCloser, error) {
	resp, err := s.client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path.Join(s.prefix, snap.SnapDir, snap.SnapName)),
	})
	if err != nil {
		return nil, err
	}

	return resp.Body, nil
}

// Save will write the snapshot to store
func (s *S3SnapStore) Save(snap Snapshot, r io.Reader) error {
	// since s3 requires io.ReadSeeker, this is the required hack.
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
	_, err = tmpfile.Seek(0, os.SEEK_SET)
	if err != nil {
		return err
	}
	// Initiate multi part upload
	ctx := context.TODO()
	ctx, cancel := context.WithTimeout(ctx, chunkUploadTimeout)
	defer cancel()
	uploadOutput, err := s.client.CreateMultipartUploadWithContext(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path.Join(s.prefix, snap.SnapDir, snap.SnapName)),
	})
	if err != nil {
		return fmt.Errorf("failed to initiate multipart upload %v", err)
	}
	logrus.Infof("Successfully initiated the multipart upload with uploadid : %s", *uploadOutput.UploadId)
	var (
		errCh      = make(chan chunkUploadError)
		chunkSize  = int64(math.Max(float64(minChunkSize), float64(size/s3NoOfChunk)))
		noOfChunks = size / chunkSize
	)
	if size%chunkSize != 0 {
		noOfChunks++
	}

	completedParts := make([]*s3.CompletedPart, noOfChunks)
	logrus.Infof("Uploading snapshot of size: %d, chunkSize: %d, noOfChunks: %d", size, chunkSize, noOfChunks)
	for offset := int64(0); offset <= size; offset += int64(chunkSize) {
		go uploadPart(s, &snap, tmpfile, uploadOutput.UploadId, completedParts, offset, chunkSize, errCh)
	}
	snapshotErr := handlePartUpload(s, &snap, tmpfile, uploadOutput.UploadId, completedParts, errCh, noOfChunks, chunkSize)

	if len(snapshotErr) != 0 {
		ctx := context.TODO()
		ctx, cancel := context.WithTimeout(ctx, chunkUploadTimeout)
		defer cancel()
		logrus.Infof("Aborting the multipart upload with upload ID : %s", *uploadOutput.UploadId)
		_, err = s.client.AbortMultipartUploadWithContext(ctx, &s3.AbortMultipartUploadInput{
			Bucket:   &s.bucket,
			Key:      aws.String(path.Join(s.prefix, snap.SnapDir, snap.SnapName)),
			UploadId: uploadOutput.UploadId,
		})
	} else {
		ctx = context.TODO()
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		logrus.Infof("Finishing the multipart upload with upload ID : %s", *uploadOutput.UploadId)
		_, err = s.client.CompleteMultipartUploadWithContext(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:   &s.bucket,
			Key:      aws.String(path.Join(s.prefix, snap.SnapDir, snap.SnapName)),
			UploadId: uploadOutput.UploadId,
			MultipartUpload: &s3.CompletedMultipartUpload{
				Parts: completedParts,
			},
		})
	}

	if err != nil {
		return err
	}
	if len(snapshotErr) == 0 {
		return nil
	}

	var collectedErr []string
	for _, chunkErr := range snapshotErr {
		collectedErr = append(collectedErr, fmt.Sprintf("failed uploading chunk with offset %d with error %v", chunkErr.offset, chunkErr.err))
	}
	return fmt.Errorf(strings.Join(collectedErr, "\n"))
}

func uploadPart(s *S3SnapStore, snap *Snapshot, file *os.File, uploadID *string, completedParts []*s3.CompletedPart, offset, chunkSize int64, errCh chan<- chunkUploadError) {
	logrus.Infof("Uploading chunk with offset : %d", offset)
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

	sr := io.NewSectionReader(file, offset, size)

	ctx, cancel := context.WithTimeout(context.TODO(), chunkUploadTimeout)
	defer cancel()
	partNumber := ((offset / chunkSize) + 1)
	in := &s3.UploadPartInput{
		Bucket:     aws.String(s.bucket),
		Key:        aws.String(path.Join(s.prefix, snap.SnapDir, snap.SnapName)),
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
	logrus.Infof("for chunk upload of offset %010d, part number: %d, res.Err %v", offset, partNumber, err)
	errCh <- chunkUploadError{
		err:    err,
		offset: offset,
	}
	return
}

func handlePartUpload(s *S3SnapStore, snap *Snapshot, file *os.File, uploadID *string, completedParts []*s3.CompletedPart, errCh chan chunkUploadError, noOfChunks, chunkSize int64) []chunkUploadError {
	var snapshotErr []chunkUploadError
	manifest := make([]int64, noOfChunks)
	maxAttempts := int64(5)
	remainingChunks := noOfChunks
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
				go uploadPart(s, snap, file, uploadID, completedParts, chunkErr.offset, chunkSize, errCh)
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
func (s *S3SnapStore) List() (SnapList, error) {
	resp, err := s.client.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(fmt.Sprintf("%s/", s.prefix)),
	})
	if err != nil {
		return nil, err
	}

	var snapList SnapList
	for _, key := range resp.Contents {
		k := (*key.Key)[len(*resp.Prefix):]
		snap, err := ParseSnapshot(k)
		if err != nil {
			// Warning
			fmt.Printf("Invalid snapshot found. Ignoring it:%s\n", k)
		} else {
			snapList = append(snapList, snap)
		}
	}
	sort.Sort(snapList)
	return snapList, nil
}

// Delete should delete the snapshot file from store
func (s *S3SnapStore) Delete(snap Snapshot) error {
	_, err := s.client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path.Join(s.prefix, snap.SnapDir, snap.SnapName)),
	})
	return err
}
