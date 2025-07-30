// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore_test

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	stiface "github.com/gardener/etcd-backup-restore/pkg/snapstore/oss"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

// ensure mockOSSBucket implements the OSSBucket interface
var _ stiface.OSSBucket = (*mockOSSBucket)(nil)

// ensure mockOSSClient implements the Client interface
var _ stiface.Client = (*mockOSSClient)(nil)

type uploadParts []oss.UploadPart

func (slice uploadParts) Len() int {
	return len(slice)
}

func (slice uploadParts) Less(i, j int) bool {
	return slice[i].PartNumber < slice[j].PartNumber
}

func (slice uploadParts) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

type mockOSSBucket struct {
	objects               map[string]*[]byte
	multiPartUploads      map[string]*[][]byte
	prefix                string
	bucketName            string
	multiPartUploadsMutex sync.Mutex
}

type mockOSSClient struct {
	objects          map[string]*[]byte
	multiPartUploads map[string]*[][]byte
	prefix           string
	bucketName       string
}

func getOSSMockBucket(m *mockOSSClient) *mockOSSBucket {
	return &mockOSSBucket{
		objects:          m.objects,
		multiPartUploads: m.multiPartUploads,
		prefix:           m.prefix,
		bucketName:       m.bucketName,
	}
}

// GetObject returns the object from map for mock test
func (m *mockOSSBucket) GetObject(objectKey string, _ ...oss.Option) (io.ReadCloser, error) {
	if m.objects[objectKey] == nil {
		return nil, fmt.Errorf("object not found")
	}
	out := io.NopCloser(bytes.NewReader(*m.objects[objectKey]))
	return out, nil
}

// InitiateMultipartUpload returns the multi-parts needed to upload for mock test
func (m *mockOSSBucket) InitiateMultipartUpload(objectKey string, _ ...oss.Option) (oss.InitiateMultipartUploadResult, error) {
	uploadID := time.Now().String()
	var parts [][]byte
	m.multiPartUploads[uploadID] = &parts
	return oss.InitiateMultipartUploadResult{
		UploadID: uploadID,
		Key:      objectKey,
		Bucket:   m.bucketName,
	}, nil
}

// UploadPart returns part uploaded for mock test
func (m *mockOSSBucket) UploadPart(imur oss.InitiateMultipartUploadResult, reader io.Reader, partSize int64, partNumber int, _ ...oss.Option) (oss.UploadPart, error) {
	if partNumber < 0 {
		return oss.UploadPart{}, fmt.Errorf("part number should be positive integer")
	}

	if reader == nil {
		return oss.UploadPart{}, fmt.Errorf("file should not be null")
	}

	m.multiPartUploadsMutex.Lock()
	if m.multiPartUploads[imur.UploadID] == nil {
		return oss.UploadPart{}, fmt.Errorf("multipart upload not initiated")
	}

	content := make([]byte, partSize)
	if _, err := reader.Read(content); err != nil {
		return oss.UploadPart{}, fmt.Errorf("failed to read complete body %v", err)
	}

	if partNumber > len(*m.multiPartUploads[imur.UploadID]) {
		t := make([][]byte, partNumber)
		copy(t, *m.multiPartUploads[imur.UploadID])
		m.multiPartUploads[imur.UploadID] = &t
	}
	(*m.multiPartUploads[imur.UploadID])[partNumber-1] = content
	m.multiPartUploadsMutex.Unlock()

	eTag := fmt.Sprint(partNumber)
	return oss.UploadPart{
		PartNumber: partNumber,
		ETag:       eTag,
	}, nil
}

// CompleteMultipartUpload returns parts uploaded result for mock test
func (m *mockOSSBucket) CompleteMultipartUpload(imur oss.InitiateMultipartUploadResult, parts []oss.UploadPart, _ ...oss.Option) (oss.CompleteMultipartUploadResult, error) {
	if m.multiPartUploads[imur.UploadID] == nil {
		return oss.CompleteMultipartUploadResult{}, fmt.Errorf("multipart upload not initiated")
	}

	sort.Sort(uploadParts(parts))
	data := *m.multiPartUploads[imur.UploadID]
	var prevPartId int = 0
	var object []byte
	for _, part := range parts {
		if part.PartNumber <= prevPartId {
			return oss.CompleteMultipartUploadResult{}, fmt.Errorf("parts should be sorted in ascending orders")
		}
		object = append(object, data[part.PartNumber-1]...)
		prevPartId = part.PartNumber
	}
	m.objects[imur.Key] = &object
	delete(m.multiPartUploads, imur.UploadID)
	eTag := time.Now().String()

	return oss.CompleteMultipartUploadResult{
		ETag:   eTag,
		Key:    imur.Key,
		Bucket: m.bucketName,
	}, nil
}

// AbortMultipartUpload returns the result of aborting upload.
func (m *mockOSSBucket) AbortMultipartUpload(imur oss.InitiateMultipartUploadResult, _ ...oss.Option) error {
	delete(m.multiPartUploads, imur.UploadID)
	return nil
}

// ListObject returns the objects from map for mock test
func (m *mockOSSBucket) ListObjects(_ ...oss.Option) (oss.ListObjectsResult, error) {
	var contents []oss.ObjectProperties
	for key := range m.objects {
		tempObj := oss.ObjectProperties{
			Key: key,
		}
		contents = append(contents, tempObj)
	}
	out := oss.ListObjectsResult{
		Objects:     contents,
		IsTruncated: false,
	}
	return out, nil
}

// DeleteObject deletes the object from map for mock test
func (m *mockOSSBucket) DeleteObject(objectKey string, _ ...oss.Option) error {
	delete(m.objects, objectKey)
	return nil
}

// GetBucketWorm get bucket worm configuration for given bucket name.
func (m *mockOSSClient) GetBucketWorm(_ string, _ ...oss.Option) (oss.WormConfiguration, error) {
	return oss.WormConfiguration{
		State:                 "InProgress",
		RetentionPeriodInDays: 1,
	}, nil
}
