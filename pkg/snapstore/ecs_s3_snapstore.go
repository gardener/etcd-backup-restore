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

const (
	// ECS does not support regions and always returns the default region: US-Standard.
	ecsDefaultRegion             string = "US-Standard"
	ecsDefaultDisableSSL         bool   = false
	ecsDefaultInsecureSkipVerify bool   = false

	ecsEndPoint           string = "ECS_ENDPOINT"
	ecsDisableSSL         string = "ECS_DISABLE_SSL"
	ecsInsecureSkipVerify string = "ECS_INSECURE_SKIP_VERIFY"
	ecsAccessKeyID        string = "ECS_ACCESS_KEY_ID"
	ecsSecretAccessKey    string = "ECS_SECRET_ACCESS_KEY"
)

// A ecsAuthOptions groups all the required options to authenticate again ECS
type ecsAuthOptions struct {
	endpoint           string
	region             string
	disableSSL         bool
	insecureSkipVerify bool
	accessKeyID        string
	secretAccessKey    string
}

// NewECSSnapStore create new S3SnapStore from shared configuration with specified bucket
func NewECSSnapStore(bucket, prefix, tempDir string, maxParallelChunkUploads uint) (*S3SnapStore, error) {
	ao, err := ecsAuthOptionsFromEnv()
	if err != nil {
		return nil, err
	}
	return newECSFromAuthOpt(bucket, prefix, tempDir, maxParallelChunkUploads, ao)
}

// newECSFromAuthOpt will create the new S3 snapstore object from ECS S3 authentication options
func newECSFromAuthOpt(bucket, prefix, tempDir string, maxParallelChunkUploads uint, ao ecsAuthOptions) (*S3SnapStore, error) {
	httpClient := http.DefaultClient
	if !ao.disableSSL {
		httpClient.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: ao.insecureSkipVerify},
		}
	}

	sess, err := session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials(ao.accessKeyID, ao.secretAccessKey, ""),
		Endpoint:         aws.String(ao.endpoint),
		Region:           aws.String(ecsDefaultRegion),
		DisableSSL:       aws.Bool(ao.disableSSL),
		S3ForcePathStyle: aws.Bool(true),
		HTTPClient:       httpClient,
	})
	if err != nil {
		return nil, fmt.Errorf("new ECS S3 session failed: %v", err)
	}
	cli := s3.New(sess)
	return NewS3FromClient(bucket, prefix, tempDir, maxParallelChunkUploads, cli), nil
}

// ecsAuthOptionsFromEnv will get provider configuration from environment variables
func ecsAuthOptionsFromEnv() (ecsAuthOptions, error) {
	endpoint, err := GetEnvVarOrError(ecsEndPoint)
	if err != nil {
		return ecsAuthOptions{}, err
	}
	accessKeyID, err := GetEnvVarOrError(ecsAccessKeyID)
	if err != nil {
		return ecsAuthOptions{}, err
	}
	secretAccessKey, err := GetEnvVarOrError(ecsSecretAccessKey)
	if err != nil {
		return ecsAuthOptions{}, err
	}
	disableSSL, err := GetEnvVarToBool(ecsDisableSSL)
	if err != nil {
		disableSSL = ecsDefaultDisableSSL
	}
	insecureSkipVerify, err := GetEnvVarToBool(ecsInsecureSkipVerify)
	if err != nil {
		insecureSkipVerify = ecsDefaultInsecureSkipVerify
	}

	ao := ecsAuthOptions{
		endpoint:           endpoint,
		disableSSL:         disableSSL,
		insecureSkipVerify: insecureSkipVerify,
		accessKeyID:        accessKeyID,
		secretAccessKey:    secretAccessKey,
	}

	return ao, nil
}
