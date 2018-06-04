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
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

const (
	tmpDir              = "/tmp"
	tmpBackupFilePrefix = "etcd-backup-"
)

// S3SnapStore is snapstore with local disk as backend
type S3SnapStore struct {
	SnapStore
	prefix string
	client s3iface.S3API
	bucket string
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

	_, err = io.Copy(tmpfile, r)
	if err != nil {
		return fmt.Errorf("failed to save snapshot to tmpfile: %v", err)
	}
	_, err = tmpfile.Seek(0, os.SEEK_SET)
	if err != nil {
		return err
	}
	// S3 put is atomic, so let's go ahead and put the key directly.
	_, err = s.client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path.Join(s.prefix, snap.SnapDir, snap.SnapName)),
		Body:   tmpfile,
	})

	return err
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

// Size should return size of the snapshot file from store
func (s *S3SnapStore) Size(snap Snapshot) (int64, error) {
	resp, err := s.client.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path.Join(s.prefix, snap.SnapDir, snap.SnapName)),
	})
	if err != nil {
		return -1, err
	}

	return *resp.ContentLength, nil
}
