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

package snapstore

import (
	"fmt"
	"os"
	"path"
)

const (
	envStorageContainer = "STORAGE_CONTAINER"
	defaultLocalStore   = "default.bkp"
)

// GetSnapstore returns the snapstore object for give storageProvider with specified container
func GetSnapstore(config *Config) (SnapStore, error) {
	if config.Container == "" {
		config.Container = os.Getenv(envStorageContainer)
	}
	switch config.Provider {
	case SnapstoreProviderLocal, "":
		if config.Container == "" {
			config.Container = defaultLocalStore
		}
		return NewLocalSnapStore(path.Join(config.Container, config.Prefix))
	case SnapstoreProviderS3:
		if config.Container == "" {
			return nil, fmt.Errorf("storage container name not specified")
		}
		return NewS3SnapStore(config.Container, config.Prefix)
	case SnapstoreProviderABS:
		if config.Container == "" {
			return nil, fmt.Errorf("storage container name not specified")
		}
		return NewABSSnapStore(config.Container, config.Prefix)
	case SnapstoreProviderGCS:
		if config.Container == "" {
			return nil, fmt.Errorf("storage container name not specified")
		}
		return NewGCSSnapStore(config.Container, config.Prefix)
	case SnapstoreProviderSwift:
		if config.Container == "" {
			return nil, fmt.Errorf("storage container name not specified")
		}
		return NewSwiftSnapStore(config.Container, config.Prefix)
	default:
		return nil, fmt.Errorf("unsupported storage provider : %s", config.Provider)

	}
}

// GetEnvVarOrError returns the value of specified environment variable or terminates if it's not defined.
func GetEnvVarOrError(varName string) (string, error) {
	value := os.Getenv(varName)
	if value == "" {
		err := fmt.Errorf("missing environment variable %s", varName)
		return value, err
	}

	return value, nil
}
