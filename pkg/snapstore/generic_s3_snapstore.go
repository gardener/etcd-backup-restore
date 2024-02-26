// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore

import (
	"crypto/tls"
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// s3AuthOptions contains all needed options to authenticate against a S3-compatible store.
type s3AuthOptions struct {
	endpoint           string
	region             string
	disableSSL         bool
	insecureSkipVerify bool
	accessKeyID        string
	secretAccessKey    string
}

// newGenericS3FromAuthOpt creates a new S3 snapstore object from the specified authentication options.
func newGenericS3FromAuthOpt(bucket, prefix, tempDir string, maxParallelChunkUploads uint, minChunkSize int64, ao s3AuthOptions) (*S3SnapStore, error) {
	httpClient := http.DefaultClient
	if !ao.disableSSL {
		httpClient.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: ao.insecureSkipVerify},
		}
	}

	sess, err := session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials(ao.accessKeyID, ao.secretAccessKey, ""),
		Endpoint:         aws.String(ao.endpoint),
		Region:           aws.String(ao.region),
		DisableSSL:       aws.Bool(ao.disableSSL),
		S3ForcePathStyle: aws.Bool(true),
		HTTPClient:       httpClient,
	})
	if err != nil {
		return nil, fmt.Errorf("could not create S3 session: %v", err)
	}
	cli := s3.New(sess)
	return NewS3FromClient(bucket, prefix, tempDir, maxParallelChunkUploads, minChunkSize, cli), nil
}
