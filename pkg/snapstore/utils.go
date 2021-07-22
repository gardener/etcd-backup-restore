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
	"strconv"
	"strings"
	"time"

	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/sirupsen/logrus"
)

const (
	envStorageContainer       = "STORAGE_CONTAINER"
	sourceEnvStorageContainer = "SOURCE_STORAGE_CONTAINER"
	defaultLocalStore         = "default.bkp"
	backupVersion             = backupVersionV2
	sourcePrefixString        = "SOURCE_"
)

// GetSnapstore returns the snapstore object for give storageProvider with specified container
func GetSnapstore(config *brtypes.SnapstoreConfig) (brtypes.SnapStore, error) {
	if config.Prefix == "" {
		config.Prefix = backupVersion
	}

	if config.Container == "" {
		if config.IsSource {
			config.Container = os.Getenv(sourceEnvStorageContainer)
		} else {
			config.Container = os.Getenv(envStorageContainer)
		}
	}

	if config.Container == "" && config.Provider != "" && config.Provider != brtypes.SnapstoreProviderLocal {
		return nil, fmt.Errorf("storage container name not specified")
	}

	if len(config.TempDir) == 0 {
		config.TempDir = path.Join("/tmp")
	}
	if _, err := os.Stat(config.TempDir); err != nil {
		if os.IsNotExist(err) {
			logrus.Infof("Temporary directory %s does not exist. Creating it...", config.TempDir)
			if err := os.MkdirAll(config.TempDir, 0700); err != nil {
				return nil, fmt.Errorf("failed to create temporary directory %s: %v", config.TempDir, err)
			}
		} else {
			return nil, fmt.Errorf("failed to get file info of temporary directory %s: %v", config.TempDir, err)
		}
	}

	if config.MaxParallelChunkUploads <= 0 {
		config.MaxParallelChunkUploads = 5
	}

	switch config.Provider {
	case brtypes.SnapstoreProviderLocal, "":
		if config.Container == "" {
			config.Container = defaultLocalStore
		}
		return NewLocalSnapStore(path.Join(config.Container, config.Prefix))
	case brtypes.SnapstoreProviderS3:
		return NewS3SnapStore(config)
	case brtypes.SnapstoreProviderABS:
		return NewABSSnapStore(config)
	case brtypes.SnapstoreProviderGCS:
		return NewGCSSnapStore(config)
	case brtypes.SnapstoreProviderSwift:
		return NewSwiftSnapStore(config)
	case brtypes.SnapstoreProviderOSS:
		return NewOSSSnapStore(config)
	case brtypes.SnapstoreProviderECS:
		return NewECSSnapStore(config)
	case brtypes.SnapstoreProviderOCS:
		return NewOCSSnapStore(config)
	case brtypes.SnapstoreProviderFakeFailed:
		return NewFailedSnapStore(), nil
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

// GetEnvVarToBool return corresponding boolen if an environment is set to string true|false
func GetEnvVarToBool(varName string) (bool, error) {
	value, err := GetEnvVarOrError(varName)
	if err != nil {
		return false, err
	}

	return strconv.ParseBool(value)
}

// collectChunkUploadError collects the error from all go routine to upload individual chunks
func collectChunkUploadError(chunkUploadCh chan<- chunk, resCh <-chan chunkUploadResult, stopCh chan struct{}, noOfChunks int64) *chunkUploadResult {
	remainingChunks := noOfChunks
	logrus.Infof("No of Chunks:= %d", noOfChunks)
	for chunkRes := range resCh {
		logrus.Infof("Received chunk result for id: %d, offset: %d", chunkRes.chunk.id, chunkRes.chunk.offset)
		if chunkRes.err != nil {
			logrus.Infof("Chunk upload failed for id: %d, offset: %d with err: %v", chunkRes.chunk.id, chunkRes.chunk.offset, chunkRes.err)
			if chunkRes.chunk.attempt == maxRetryAttempts {
				logrus.Errorf("Received the chunk upload error even after %d attempts from one of the workers. Sending stop signal to all workers.", chunkRes.chunk.attempt)
				close(stopCh)
				return &chunkRes
			}
			chunk := chunkRes.chunk
			delayTime := (1 << chunk.attempt)
			chunk.attempt++
			logrus.Warnf("Will try to upload chunk id: %d, offset: %d at attempt %d  after %d seconds", chunk.id, chunk.offset, chunk.attempt, delayTime)
			time.AfterFunc(time.Duration(delayTime)*time.Second, func() {
				select {
				case <-stopCh:
					return
				default:
					chunkUploadCh <- *chunk
				}
			})
		} else {
			remainingChunks--
			if remainingChunks == 0 {
				logrus.Infof("Received successful chunk result for all chunks. Stopping workers.")
				close(stopCh)
				break
			}
		}
	}
	return nil
}

func getEnvPrefixString(isSource bool) string {
	if isSource {
		return sourcePrefixString
	}
	return ""
}

func adaptPrefix(snap *brtypes.Snapshot, snapstorePrefix string) string {
	if strings.Contains(snap.Prefix, "/"+backupVersionV1) && strings.Contains(snapstorePrefix, "/"+backupVersionV2) {
		return strings.Replace(snapstorePrefix, "/"+backupVersionV2, "/"+backupVersionV1, 1)
	}

	return snapstorePrefix
}
