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

package snapstore_test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

// Define a mock struct to be used in your unit tests of myFunc.
type mockS3Client struct {
	s3iface.S3API
	objects               map[string]*[]byte
	prefix                string
	multiPartUploads      map[string]*[][]byte
	multiPartUploadsMutex sync.Mutex
}

// GetObject returns the object from map for mock test
func (m *mockS3Client) GetObject(in *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	if m.objects[*in.Key] == nil {
		return nil, fmt.Errorf("object not found")
	}
	// Only need to return mocked response output
	out := s3.GetObjectOutput{
		Body: ioutil.NopCloser(bytes.NewReader(*m.objects[*in.Key])),
	}
	return &out, nil
}

// PutObject adds the object to the map for mock test
func (m *mockS3Client) PutObject(in *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	size, err := in.Body.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("failed to seek at the end of body %v", err)
	}
	if _, err := in.Body.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek at the start of body %v", err)
	}
	content := make([]byte, size)
	if _, err := in.Body.Read(content); err != nil {
		return nil, fmt.Errorf("failed to read complete body %v", err)
	}
	m.objects[*in.Key] = &content
	out := s3.PutObjectOutput{}
	return &out, nil
}

func (m *mockS3Client) CreateMultipartUploadWithContext(ctx aws.Context, in *s3.CreateMultipartUploadInput, opts ...request.Option) (*s3.CreateMultipartUploadOutput, error) {
	uploadID := time.Now().String()
	var parts [][]byte
	m.multiPartUploads[uploadID] = &parts
	out := &s3.CreateMultipartUploadOutput{
		Bucket:   in.Bucket,
		UploadId: &uploadID,
	}
	return out, nil
}

func (m *mockS3Client) UploadPartWithContext(ctx aws.Context, in *s3.UploadPartInput, opts ...request.Option) (*s3.UploadPartOutput, error) {
	if *in.PartNumber < 0 {
		return nil, fmt.Errorf("part number should be positive integer")
	}
	m.multiPartUploadsMutex.Lock()
	if m.multiPartUploads[*in.UploadId] == nil {
		m.multiPartUploadsMutex.Unlock()
		return nil, fmt.Errorf("multipart upload not initiated")
	}
	if *in.PartNumber > int64(len(*m.multiPartUploads[*in.UploadId])) {
		t := make([][]byte, *in.PartNumber)
		copy(t, *m.multiPartUploads[*in.UploadId])
		delete(m.multiPartUploads, *in.UploadId)
		m.multiPartUploads[*in.UploadId] = &t
	}
	m.multiPartUploadsMutex.Unlock()

	size, err := in.Body.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("failed to seek at the end of body %v", err)
	}
	if _, err := in.Body.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek at the start of body %v", err)
	}
	content := make([]byte, size)
	if _, err := in.Body.Read(content); err != nil {
		return nil, fmt.Errorf("failed to read complete body %v", err)
	}

	m.multiPartUploadsMutex.Lock()
	(*m.multiPartUploads[*in.UploadId])[*in.PartNumber-1] = content
	m.multiPartUploadsMutex.Unlock()

	eTag := string(*in.PartNumber)
	out := &s3.UploadPartOutput{
		ETag: &eTag,
	}
	return out, nil
}

func (m *mockS3Client) CompleteMultipartUploadWithContext(ctx aws.Context, in *s3.CompleteMultipartUploadInput, opts ...request.Option) (*s3.CompleteMultipartUploadOutput, error) {
	if m.multiPartUploads[*in.UploadId] == nil {
		return nil, fmt.Errorf("multipart upload not initiated")
	}
	data := *m.multiPartUploads[*in.UploadId]
	var prevPartId int64 = 0
	var object []byte
	for _, part := range in.MultipartUpload.Parts {
		if *part.PartNumber <= prevPartId {
			return nil, fmt.Errorf("parts should be sorted in ascending orders")
		}
		object = append(object, data[*part.PartNumber-1]...)
		prevPartId = *part.PartNumber
	}
	m.objects[*in.Key] = &object
	delete(m.multiPartUploads, *in.UploadId)
	eTag := time.Now().String()
	out := s3.CompleteMultipartUploadOutput{
		Bucket: in.Bucket,
		ETag:   &eTag,
	}
	return &out, nil
}

func (m *mockS3Client) AbortMultipartUploadWithContext(ctx aws.Context, in *s3.AbortMultipartUploadInput, opts ...request.Option) (*s3.AbortMultipartUploadOutput, error) {
	delete(m.multiPartUploads, *in.UploadId)
	out := &s3.AbortMultipartUploadOutput{}
	return out, nil
}

// ListObject returns the objects from map for mock test
func (m *mockS3Client) ListObjects(in *s3.ListObjectsInput) (*s3.ListObjectsOutput, error) {
	var contents []*s3.Object
	for key := range m.objects {
		if strings.HasPrefix(key, *in.Prefix) {
			keyPtr := new(string)
			*keyPtr = key
			tempObj := &s3.Object{
				Key: keyPtr,
			}
			contents = append(contents, tempObj)
		}
	}
	out := &s3.ListObjectsOutput{
		Prefix:   in.Prefix,
		Contents: contents,
	}
	return out, nil
}

// ListObject returns the objects from map for mock test
func (m *mockS3Client) ListObjectsPages(in *s3.ListObjectsInput, callback func(*s3.ListObjectsOutput, bool) bool) error {
	var (
		count    int64 = 0
		limit    int64 = 1 // aws default is 1000.
		lastPage bool  = false
		keys     []string
		out      = &s3.ListObjectsOutput{
			Prefix:   in.Prefix,
			Contents: make([]*s3.Object, 0),
		}
	)
	if in.MaxKeys != nil {
		limit = *in.MaxKeys
	}
	for key := range m.objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for index, key := range keys {
		if strings.HasPrefix(key, *in.Prefix) {
			keyPtr := new(string)
			*keyPtr = key
			tempObj := &s3.Object{
				Key: keyPtr,
			}
			out.Contents = append(out.Contents, tempObj)
			count++
		}
		if index == len(keys)-1 {
			lastPage = true
		}
		if count == limit || lastPage {
			if !callback(out, lastPage) {
				return nil
			}
			count = 0
			out = &s3.ListObjectsOutput{
				Prefix:     in.Prefix,
				Contents:   make([]*s3.Object, 0),
				NextMarker: &key,
			}
		}
	}
	return nil
}

// DeleteObject deletes the object from map for mock test
func (m *mockS3Client) DeleteObject(in *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	delete(m.objects, *in.Key)
	return &s3.DeleteObjectOutput{}, nil
}
