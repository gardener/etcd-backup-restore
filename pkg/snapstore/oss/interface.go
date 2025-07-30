// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package oss

import (
	"io"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

// OSSBucket is an interface for oss.Bucket used in snapstore
type OSSBucket interface {
	// GetObject downloads the object.
	GetObject(objectKey string, options ...oss.Option) (io.ReadCloser, error)
	// ListObjects lists the objects under the current bucket.
	ListObjects(options ...oss.Option) (oss.ListObjectsResult, error)
	// DeleteObject deletes the object.
	DeleteObject(objectKey string, options ...oss.Option) error
	// InitiateMultipartUpload initializes multipart upload
	InitiateMultipartUpload(objectKey string, options ...oss.Option) (oss.InitiateMultipartUploadResult, error)
	// CompleteMultipartUpload completes the multipart upload.
	CompleteMultipartUpload(imur oss.InitiateMultipartUploadResult, parts []oss.UploadPart, options ...oss.Option) (oss.CompleteMultipartUploadResult, error)
	// UploadPart uploads parts
	UploadPart(imur oss.InitiateMultipartUploadResult, reader io.Reader, partSize int64, partNumber int, options ...oss.Option) (oss.UploadPart, error)
	// AbortMultipartUpload aborts the multipart upload.
	AbortMultipartUpload(imur oss.InitiateMultipartUploadResult, options ...oss.Option) error
}

// Client is an interface for oss.Client used in snapstore
type Client interface {
	// GetBucketWorm get bucket WORM(Write-Once-Read-Many) configuration.
	GetBucketWorm(bucketName string, options ...oss.Option) (oss.WormConfiguration, error)
}
