// Copyright © 2018 The Gardener Authors.
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
func GetSnapstore(storageProvider, prefix string) (SnapStore, error) {
	switch storageProvider {
	case SnapstoreProviderLocal, "":
		container := os.Getenv(envStorageContainer)
		if container == "" {
			container = defaultLocalStore
		}
		return NewLocalSnapStore(path.Join(container, prefix))
	case SnapstoreProviderS3:
		container := os.Getenv(envStorageContainer)
		if container == "" {
			return nil, fmt.Errorf("storage container name not specified")
		}
		return NewS3SnapStore(container, prefix)
	case SnapstoreProviderGCS:
		container := os.Getenv(envStorageContainer)
		if container == "" {
			return nil, fmt.Errorf("storage container name not specified")
		}
		return NewGCSSnapStore(container, prefix)
	default:
		return nil, fmt.Errorf("unsupported storage provider : %s", storageProvider)

	}
}
