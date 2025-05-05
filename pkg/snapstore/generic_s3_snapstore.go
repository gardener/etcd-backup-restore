// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// s3AuthOptions contains all needed options to authenticate against a S3-compatible store.
type s3AuthOptions struct {
	endpoint           string
	region             string
	accessKeyID        string
	secretAccessKey    string
	disableSSL         bool
	insecureSkipVerify bool
}

// newGenericS3FromAuthOpt creates a new S3 snapstore object from the specified authentication options.
func newGenericS3FromAuthOpt(bucket, prefix, tempDir string, maxParallelChunkUploads uint, minChunkSize int64, ao s3AuthOptions) (*S3SnapStore, error) {
	httpClient := http.DefaultClient
	if !ao.disableSSL {
		httpClient.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: ao.insecureSkipVerify}, // #nosec G402 -- InsecureSkipVerify is set by user input, and can be allowed to be set to true based on user's requirement.
		}
	}

	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithCredentialsProvider(aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(ao.accessKeyID, ao.secretAccessKey, ""))),
		config.WithBaseEndpoint(ao.endpoint),
		config.WithRegion(ao.region),
		config.WithHTTPClient(httpClient),
	)
	if err != nil {
		return nil, fmt.Errorf("could not create S3 session: %w", err)
	}

	cli := s3.NewFromConfig(cfg,
		func(o *s3.Options) {
			o.EndpointOptions.DisableHTTPS = ao.disableSSL
			o.UsePathStyle = true
		},
	)
	return NewS3FromClient(bucket, prefix, tempDir, maxParallelChunkUploads, minChunkSize, cli, SSECredentials{}), nil
}
