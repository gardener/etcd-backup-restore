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
	ocsDefaultDisableSSL         bool = false
	ocsDefaultInsecureSkipVerify bool = false

	ocsEndpoint           string = "OCS_ENDPOINT"
	ocsRegion             string = "OCS_REGION"
	ocsDisableSSL         string = "OCS_DISABLE_SSL"
	ocsInsecureSkipVerify string = "OCS_INSECURE_SKIP_VERIFY"
	ocsAccessKeyID        string = "OCS_ACCESS_KEY_ID"
	ocsSecretAccessKey    string = "OCS_SECRET_ACCESS_KEY"
)

// NewOCSSnapStore creates a new S3SnapStore from shared configuration with the specified bucket.
func NewOCSSnapStore(config *brtypes.SnapstoreConfig) (*S3SnapStore, error) {
	ao, err := ocsAuthOptionsFromEnv()
	if err != nil {
		return nil, err
	}
	return newGenericS3FromAuthOpt(config.Container, config.Prefix, config.TempDir, config.MaxParallelChunkUploads, ao)
}

// ocsAuthOptionsFromEnv gets OCS provider configuration from environment variables.
func ocsAuthOptionsFromEnv() (s3AuthOptions, error) {
	endpoint, err := GetEnvVarOrError(ocsEndpoint)
	if err != nil {
		return s3AuthOptions{}, err
	}
	accessKeyID, err := GetEnvVarOrError(ocsAccessKeyID)
	if err != nil {
		return s3AuthOptions{}, err
	}
	secretAccessKey, err := GetEnvVarOrError(ocsSecretAccessKey)
	if err != nil {
		return s3AuthOptions{}, err
	}
	region, err := GetEnvVarOrError(ocsRegion)
	if err != nil {
		return s3AuthOptions{}, err
	}
	disableSSL, err := GetEnvVarToBool(ocsDisableSSL)
	if err != nil {
		disableSSL = ocsDefaultDisableSSL
	}
	insecureSkipVerify, err := GetEnvVarToBool(ocsInsecureSkipVerify)
	if err != nil {
		insecureSkipVerify = ocsDefaultInsecureSkipVerify
	}

	ao := s3AuthOptions{
		endpoint:           endpoint,
		region:             region,
		disableSSL:         disableSSL,
		insecureSkipVerify: insecureSkipVerify,
		accessKeyID:        accessKeyID,
		secretAccessKey:    secretAccessKey,
	}

	return ao, nil
}
