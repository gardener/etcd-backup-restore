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

import brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

const (
	// ECS does not support regions and always uses the default region US-Standard.
	ecsDefaultRegion             string = "US-Standard"
	ecsDefaultDisableSSL         bool   = false
	ecsDefaultInsecureSkipVerify bool   = false

	ecsEndPoint           string = "ECS_ENDPOINT"
	ecsDisableSSL         string = "ECS_DISABLE_SSL"
	ecsInsecureSkipVerify string = "ECS_INSECURE_SKIP_VERIFY"
	ecsAccessKeyID        string = "ECS_ACCESS_KEY_ID"
	ecsSecretAccessKey    string = "ECS_SECRET_ACCESS_KEY"
)

// NewECSSnapStore creates a new S3SnapStore from shared configuration with the specified bucket.
func NewECSSnapStore(config *brtypes.SnapstoreConfig) (*S3SnapStore, error) {
	ao, err := ecsAuthOptionsFromEnv()
	if err != nil {
		return nil, err
	}
	return newGenericS3FromAuthOpt(config.Container, config.Prefix, config.TempDir, config.MaxParallelChunkUploads, ao)
}

// ecsAuthOptionsFromEnv gets ECS provider configuration from environment variables.
func ecsAuthOptionsFromEnv() (s3AuthOptions, error) {
	endpoint, err := GetEnvVarOrError(ecsEndPoint)
	if err != nil {
		return s3AuthOptions{}, err
	}
	accessKeyID, err := GetEnvVarOrError(ecsAccessKeyID)
	if err != nil {
		return s3AuthOptions{}, err
	}
	secretAccessKey, err := GetEnvVarOrError(ecsSecretAccessKey)
	if err != nil {
		return s3AuthOptions{}, err
	}
	disableSSL, err := GetEnvVarToBool(ecsDisableSSL)
	if err != nil {
		disableSSL = ecsDefaultDisableSSL
	}
	insecureSkipVerify, err := GetEnvVarToBool(ecsInsecureSkipVerify)
	if err != nil {
		insecureSkipVerify = ecsDefaultInsecureSkipVerify
	}

	ao := s3AuthOptions{
		endpoint:           endpoint,
		region:             ecsDefaultRegion,
		disableSSL:         disableSSL,
		insecureSkipVerify: insecureSkipVerify,
		accessKeyID:        accessKeyID,
		secretAccessKey:    secretAccessKey,
	}

	return ao, nil
}
