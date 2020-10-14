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

package snapstore_test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"sort"
	"sync"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
)

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
	snapstore.OSSBucket
	objects               map[string]*[]byte
	prefix                string
	multiPartUploads      map[string]*[][]byte
	multiPartUploadsMutex sync.Mutex
	bucketName            string
}

// GetObject returns the object from map for mock test
func (m *mockOSSBucket) GetObject(objectKey string, options ...oss.Option) (io.ReadCloser, error) {
	if m.objects[objectKey] == nil {
		return nil, fmt.Errorf("object not found")
	}
	out := ioutil.NopCloser(bytes.NewReader(*m.objects[objectKey]))
	return out, nil
}

// InitiateMultipartUpload returns the multi-parts needed to upload for mock test
func (m *mockOSSBucket) InitiateMultipartUpload(objectKey string, options ...oss.Option) (oss.InitiateMultipartUploadResult, error) {
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
func (m *mockOSSBucket) UploadPart(imur oss.InitiateMultipartUploadResult, reader io.Reader, partSize int64, partNumber int, options ...oss.Option) (oss.UploadPart, error) {
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
func (m *mockOSSBucket) CompleteMultipartUpload(imur oss.InitiateMultipartUploadResult, parts []oss.UploadPart, options ...oss.Option) (oss.CompleteMultipartUploadResult, error) {
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
func (m *mockOSSBucket) AbortMultipartUpload(imur oss.InitiateMultipartUploadResult, options ...oss.Option) error {
	delete(m.multiPartUploads, imur.UploadID)
	return nil
}

// ListObject returns the objects from map for mock test
func (m *mockOSSBucket) ListObjects(options ...oss.Option) (oss.ListObjectsResult, error) {
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
func (m *mockOSSBucket) DeleteObject(objectKey string, options ...oss.Option) error {
	delete(m.objects, objectKey)
	return nil
}
