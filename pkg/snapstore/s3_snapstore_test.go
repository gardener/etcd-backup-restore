// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/snapstore/internal/s3api"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// ensure mockS3Client implements the interface
var _ s3api.Client = (*mockS3Client)(nil)

// Define a mock struct to be used in your unit tests of myFunc.
type mockS3Client struct {
	objects               map[string]*[]byte
	multiPartUploads      map[string]*[][]byte
	prefix                string
	multiPartUploadsMutex sync.Mutex
}

// GetObject returns the object from map for mock test
func (m *mockS3Client) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if m.objects[*in.Key] == nil {
		return nil, fmt.Errorf("object not found")
	}
	// Only need to return mocked response output
	out := s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(*m.objects[*in.Key])),
	}
	return &out, nil
}

func (m *mockS3Client) CreateMultipartUpload(_ context.Context, in *s3.CreateMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	uploadID := time.Now().String()
	var parts [][]byte
	m.multiPartUploads[uploadID] = &parts
	out := &s3.CreateMultipartUploadOutput{
		Bucket:   in.Bucket,
		UploadId: &uploadID,
	}
	return out, nil
}

func (m *mockS3Client) UploadPart(_ context.Context, in *s3.UploadPartInput, _ ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	if *in.PartNumber < 0 {
		return nil, fmt.Errorf("part number should be positive integer")
	}
	m.multiPartUploadsMutex.Lock()
	if m.multiPartUploads[*in.UploadId] == nil {
		m.multiPartUploadsMutex.Unlock()
		return nil, fmt.Errorf("multipart upload not initiated")
	}
	if *in.PartNumber > int32(len(*m.multiPartUploads[*in.UploadId])) {
		t := make([][]byte, *in.PartNumber)
		copy(t, *m.multiPartUploads[*in.UploadId])
		delete(m.multiPartUploads, *in.UploadId)
		m.multiPartUploads[*in.UploadId] = &t
	}
	m.multiPartUploadsMutex.Unlock()

	content, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read complete body %v", err)
	}

	m.multiPartUploadsMutex.Lock()
	(*m.multiPartUploads[*in.UploadId])[*in.PartNumber-1] = content
	m.multiPartUploadsMutex.Unlock()

	eTag := fmt.Sprint(*in.PartNumber)
	out := &s3.UploadPartOutput{
		ETag: &eTag,
	}
	return out, nil
}

func (m *mockS3Client) CompleteMultipartUpload(_ context.Context, in *s3.CompleteMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	if m.multiPartUploads[*in.UploadId] == nil {
		return nil, fmt.Errorf("multipart upload not initiated")
	}
	data := *m.multiPartUploads[*in.UploadId]
	var prevPartId int32 = 0
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

func (m *mockS3Client) AbortMultipartUpload(_ context.Context, in *s3.AbortMultipartUploadInput, _ ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	delete(m.multiPartUploads, *in.UploadId)
	out := &s3.AbortMultipartUploadOutput{}
	return out, nil
}

// ListObjectV2 returns the objects from map for mock test
func (m *mockS3Client) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	var contents []s3types.Object
	for key := range m.objects {
		if strings.HasPrefix(key, *in.Prefix) {
			keyPtr := new(string)
			*keyPtr = key
			tempObj := s3types.Object{
				Key: keyPtr,
			}
			contents = append(contents, tempObj)
		}
	}
	out := &s3.ListObjectsV2Output{
		Prefix:   in.Prefix,
		Contents: contents,
	}
	return out, nil
}

// ListObjectVersions returns the versioned objects from map for mock test.
func (m *mockS3Client) ListObjectVersions(_ context.Context, in *s3.ListObjectVersionsInput, _ ...func(*s3.Options)) (*s3.ListObjectVersionsOutput, error) {
	var (
		count int32
		limit int32 = 1 // aws default is 1000.
		keys  []string
		out   = &s3.ListObjectVersionsOutput{
			Prefix:   in.Prefix,
			Versions: make([]s3types.ObjectVersion, 0),
		}
	)

	if in.MaxKeys != nil {
		limit = *in.MaxKeys
	}
	for key := range m.objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var (
		nextPageIdx = 0
		lastPageIdx = len(keys) - 1
	)

	if in.KeyMarker != nil {
		var err error
		nextPageIdx, err = strconv.Atoi(*in.KeyMarker)
		if err != nil {
			return nil, err
		}
	}

	if nextPageIdx > lastPageIdx {
		return nil, fmt.Errorf("trying to read beyond limits, requested page %d, last page is %d", nextPageIdx, lastPageIdx)
	}

	for idx, key := range keys[nextPageIdx:] {
		pageIdx := nextPageIdx + idx
		if strings.HasPrefix(key, *in.Prefix) {
			tempObj := s3types.ObjectVersion{
				Key:          aws.String(key),
				IsLatest:     aws.Bool(true),
				VersionId:    aws.String(fmt.Sprintf("%s:%d", "mock-versionID", count)),
				LastModified: aws.Time(time.Now()),
			}
			out.Versions = append(out.Versions, tempObj)
			count++
		}

		if count == limit {
			if pageIdx < lastPageIdx {
				out.KeyMarker = aws.String(strconv.Itoa(pageIdx))
				out.NextKeyMarker = aws.String(strconv.Itoa(pageIdx + 1))
				out.IsTruncated = aws.Bool(true)
			}
			return out, nil
		}
	}

	return out, nil
}

// GetBucketVersioning returns the versioning status of S3's mock bucket.
func (m *mockS3Client) GetBucketVersioning(_ context.Context, in *s3.GetBucketVersioningInput, _ ...func(*s3.Options)) (*s3.GetBucketVersioningOutput, error) {
	if in != nil && *in.Bucket == "mock-S3NonObjectLockedBucket" {
		return &s3.GetBucketVersioningOutput{}, nil
	} else if in != nil && *in.Bucket == "mock-S3ObjectLockedBucket" {
		return &s3.GetBucketVersioningOutput{
			Status: s3types.BucketVersioningStatusEnabled,
		}, nil
	}
	return nil, fmt.Errorf("unable to check versioning status for given bucket input")
}

// GetObjectLockConfiguration returns the object lock configuration of S3's mock bucket.
func (m *mockS3Client) GetObjectLockConfiguration(_ context.Context, in *s3.GetObjectLockConfigurationInput, _ ...func(*s3.Options)) (*s3.GetObjectLockConfigurationOutput, error) {
	defaultRetentionPeriod := int32(2)

	if in != nil && *in.Bucket == "mock-S3ObjectLockedBucket" {
		return &s3.GetObjectLockConfigurationOutput{
			ObjectLockConfiguration: &s3types.ObjectLockConfiguration{
				ObjectLockEnabled: s3types.ObjectLockEnabledEnabled,
				Rule: &s3types.ObjectLockRule{
					DefaultRetention: &s3types.DefaultRetention{
						Days: &defaultRetentionPeriod,
					},
				},
			},
		}, nil
	}

	if in != nil && *in.Bucket == "mock-s3ObjectLockBucketButRulesNotDefined" {
		return &s3.GetObjectLockConfigurationOutput{
			ObjectLockConfiguration: &s3types.ObjectLockConfiguration{
				ObjectLockEnabled: s3types.ObjectLockEnabledEnabled,
			},
		}, nil
	}

	return nil, fmt.Errorf("unable to check object lock configuration for given bucket")
}

// GetObjectTagging returns the tag for S3's mock bucket object.
func (m *mockS3Client) GetObjectTagging(_ context.Context, input *s3.GetObjectTaggingInput, _ ...func(*s3.Options)) (*s3.GetObjectTaggingOutput, error) {
	if *input.Bucket == "mock-s3Bucket" {
		return nil, fmt.Errorf("unable to check tag for given object input")
	}

	objectTag := []s3types.Tag{}

	if *input.Key == "mock/v2/Full-000000xx-000000yy-yyxxzz.gz" && *input.VersionId == "mockVersion1" {
		return &s3.GetObjectTaggingOutput{
			TagSet: append(objectTag, s3types.Tag{
				Key:   aws.String("x-etcd-snapshot-exclude"),
				Value: aws.String("true"),
			}),
		}, nil
	} else if *input.Key == "mock/v2/Full-000000xx-000000yy-yyxxzz.gz" && *input.VersionId == "mockVersion2" {
		return &s3.GetObjectTaggingOutput{
			TagSet: append(objectTag, s3types.Tag{
				Key:   aws.String("x-etcd-snapshot-exclude"),
				Value: aws.String("false"),
			}),
		}, nil
	}

	return &s3.GetObjectTaggingOutput{
		TagSet: objectTag,
	}, nil
}

// DeleteObject deletes the object from map for mock test
func (m *mockS3Client) DeleteObject(_ context.Context, in *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	delete(m.objects, *in.Key)
	return &s3.DeleteObjectOutput{}, nil
}
