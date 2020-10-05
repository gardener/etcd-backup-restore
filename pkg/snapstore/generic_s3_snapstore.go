// Copyright (c) 2020 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
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
func newGenericS3FromAuthOpt(bucket, prefix, tempDir string, maxParallelChunkUploads uint, ao s3AuthOptions) (*S3SnapStore, error) {
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
	return NewS3FromClient(bucket, prefix, tempDir, maxParallelChunkUploads, cli), nil
}
