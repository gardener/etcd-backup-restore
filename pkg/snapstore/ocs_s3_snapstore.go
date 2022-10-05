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
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
)

const (
	ocsCredentialFile         string = "OPENSHIFT_APPLICATION_CREDENTIALS"
	ocsCredentialFileJSONFile string = "OPENSHIFT_APPLICATION_CREDENTIALS_JSON"
)

type ocsAuthOptions struct {
	Endpoint           string `json:"endpoint"`
	Region             string `json:"region"`
	DisableSSL         bool   `json:"disableSSL"`
	InsecureSkipVerify bool   `json:"insecureSkipVerify"`
	AccessKeyID        string `json:"accessKeyID"`
	SecretAccessKey    string `json:"secretAccessKey"`
}

// NewOCSSnapStore creates a new S3SnapStore from shared configuration with the specified bucket.
func NewOCSSnapStore(config *brtypes.SnapstoreConfig) (*S3SnapStore, error) {
	credentials, err := getOCSAuthOptions(getEnvPrefixString(config.IsSource))
	if err != nil {
		return nil, err
	}

	return newGenericS3FromAuthOpt(config.Container, config.Prefix, config.TempDir, config.MaxParallelChunkUploads, config.MinChunkSize, ocsAuthOptionsToGenericS3(*credentials))
}

func getOCSAuthOptions(prefix string) (*ocsAuthOptions, error) {
	if filename, isSet := os.LookupEnv(prefix + ocsCredentialFileJSONFile); isSet {
		ao, err := readOCSCredentialsJSON(filename)
		if err != nil {
			return nil, fmt.Errorf("error getting credentials using %v file", filename)
		}
		return ao, nil
	}

	if dir, isSet := os.LookupEnv(prefix + ocsCredentialFile); isSet {
		ao, err := readOCSCredentialFromDir(dir)
		if err != nil {
			return nil, fmt.Errorf("error getting credentials from %v directory", dir)
		}
		return ao, nil
	}

	return nil, fmt.Errorf("unable to get credentials")
}

func readOCSCredentialFromDir(dirname string) (*ocsAuthOptions, error) {
	ao := ocsAuthOptions{}

	files, err := os.ReadDir(dirname)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		switch file.Name() {
		case "endpoint":
			{
				data, err := os.ReadFile(dirname + "/endpoint")
				if err != nil {
					return nil, err
				}
				ao.Endpoint = string(data)
			}
		case "accessKeyID":
			{
				data, err := os.ReadFile(dirname + "/accessKeyID")
				if err != nil {
					return nil, err
				}
				ao.AccessKeyID = string(data)
			}
		case "secretAccessKey":
			{
				data, err := os.ReadFile(dirname + "/secretAccessKey")
				if err != nil {
					return nil, err
				}
				ao.SecretAccessKey = string(data)
			}
		case "region":
			{
				data, err := os.ReadFile(dirname + "/region")
				if err != nil {
					return nil, err
				}
				ao.Region = string(data)
			}
		case "disableSSL":
			{
				data, err := os.ReadFile(dirname + "/disableSSL")
				if err != nil {
					return nil, err
				}
				ao.DisableSSL, err = strconv.ParseBool(string(data))
				if err != nil {
					return nil, err
				}
			}
		case "insecureSkipVerify":
			{
				data, err := os.ReadFile(dirname + "/insecureSkipVerify")
				if err != nil {
					return nil, err
				}
				ao.InsecureSkipVerify, err = strconv.ParseBool(string(data))
				if err != nil {
					return nil, err
				}
			}
		}
	}

	if err := isOCSConfigEmpty(ao); err != nil {
		return nil, err
	}
	return &ao, nil
}

func readOCSCredentialsJSON(filename string) (*ocsAuthOptions, error) {
	jsonData, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	return ocsCredentialsFromJSON(jsonData)
}

// ocsCredentialsFromJSON obtains OCS credentials from a JSON value.
func ocsCredentialsFromJSON(jsonData []byte) (*ocsAuthOptions, error) {
	ocsConfig := ocsAuthOptions{}
	if err := json.Unmarshal(jsonData, &ocsConfig); err != nil {
		return nil, err
	}

	return &ocsConfig, nil
}

func ocsAuthOptionsToGenericS3(options ocsAuthOptions) s3AuthOptions {
	return s3AuthOptions{
		endpoint:           options.Endpoint,
		region:             options.Region,
		accessKeyID:        options.AccessKeyID,
		secretAccessKey:    options.SecretAccessKey,
		disableSSL:         options.DisableSSL,
		insecureSkipVerify: options.InsecureSkipVerify,
	}
}

func isOCSConfigEmpty(config ocsAuthOptions) error {
	if len(config.AccessKeyID) != 0 && len(config.Region) != 0 && len(config.SecretAccessKey) != 0 && len(config.Endpoint) != 0 {
		return nil
	}
	return fmt.Errorf("ocs s3 credentials: region, secretAccessKey, endpoint or accessKeyID is missing")
}

// OCSSnapStoreHash calculates and returns the hash of OCS snapstore secret.
func OCSSnapStoreHash(config *brtypes.SnapstoreConfig) (string, error) {
	ao, err := getOCSAuthOptions("")
	if err != nil {
		return "", err
	}

	return getOCSHash(ao), nil
}

func getOCSHash(config *ocsAuthOptions) string {
	data := fmt.Sprintf("%s%s%s%s", config.AccessKeyID, config.Endpoint, config.Region, config.SecretAccessKey)
	return getHash(data)
}
