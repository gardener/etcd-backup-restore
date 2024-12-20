// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
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
		if strings.HasPrefix(config.Container, "../../../test/output") {
			// To be used only by unit tests
			return NewLocalSnapStore(path.Join(config.Container, config.Prefix))
		}
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return nil, err
		}
		return NewLocalSnapStore(path.Join(homeDir, config.Container, config.Prefix))
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

// GetSnapstoreSecretModifiedTime returns the latest modification timestamp of the access credential files.
// Returns an error if fetching the timestamp of the access credential files fails.
func GetSnapstoreSecretModifiedTime(snapstoreProvider string) (time.Time, error) {
	switch snapstoreProvider {
	case brtypes.SnapstoreProviderLocal:
		return time.Time{}, nil
	case brtypes.SnapstoreProviderS3:
		return GetS3CredentialsLastModifiedTime()
	case brtypes.SnapstoreProviderABS:
		return GetABSCredentialsLastModifiedTime()
	case brtypes.SnapstoreProviderGCS:
		return GetGCSCredentialsLastModifiedTime()
	case brtypes.SnapstoreProviderSwift:
		return GetSwiftCredentialsLastModifiedTime()
	case brtypes.SnapstoreProviderOSS:
		return GetOSSCredentialsLastModifiedTime()
	case brtypes.SnapstoreProviderOCS:
		return GetOCSCredentialsLastModifiedTime()
	default:
		return time.Time{}, nil
	}
}

// getLatestCredentialsModifiedTime returns the latest file modification time from a list of files
func getLatestCredentialsModifiedTime(credentialFiles []string) (time.Time, error) {
	var latestModifiedTime time.Time

	for _, filename := range credentialFiles {
		// os.Stat instead of file.Info because the file is symlinked
		fileInfo, err := os.Stat(filename)
		if err != nil {
			return time.Time{}, err
		}
		if fileInfo.IsDir() {
			return time.Time{}, fmt.Errorf("a directory %s found in place of a credential file", fileInfo.Name())
		}
		fileModifiedTime := fileInfo.ModTime()
		if latestModifiedTime.Before(fileModifiedTime) {
			latestModifiedTime = fileModifiedTime
		}
	}
	return latestModifiedTime, nil
}

// findFileWithExtensionInDir checks whether there is any file present in a given directory with given file extension.
func findFileWithExtensionInDir(dir, extension string) (string, error) {
	dirEntries, err := os.ReadDir(dir)
	if err != nil {
		return "", fmt.Errorf("error while listing files to fetch credentials in %v directory with error: %w", dir, err)
	}
	var credentialFile string
	for _, dirEntry := range dirEntries {
		if filepath.Ext(dirEntry.Name()) == extension {
			credentialFile = filepath.Join(dir, dirEntry.Name())
			break
		}
	}
	// return the first file found with the extension
	return credentialFile, nil
}

// getJSONCredentialModifiedTime returns the modification time of a JSON file if it is present in a given directory.
// This function is introduced only to support JSON files being present in the directory which is passed through the
// PROVIDER_APPLICATION_CREDENTIAL environment variable. Will be removed by v0.31.0.
func getJSONCredentialModifiedTime(dir string) (time.Time, error) {
	jsonCredentialFile, err := findFileWithExtensionInDir(dir, ".json")
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to fetch file information of the JSON credential file %v in the directory %v with error: %w", jsonCredentialFile, dir, err)
	}
	if jsonCredentialFile != "" {
		credentialFiles := []string{jsonCredentialFile}
		timestamp, err := getLatestCredentialsModifiedTime(credentialFiles)
		if err != nil {
			return time.Time{}, fmt.Errorf("failed to fetch file modification information of the JSON credential file %v in the directory %v with error: %w", jsonCredentialFile, dir, err)
		}
		return timestamp, nil
	}
	// No JSON credential file was found in a given directory.
	return time.Time{}, nil
}

// writeSnapshotToTempFile writes the snapshot to a temporary file and returns the file handle.
// The caller must ensure that the file is closed and removed after use.
func writeSnapshotToTempFile(tempDir string, rc io.ReadCloser) (tempFile *os.File, written int64, err error) {
	defer func() {
		if err1 := rc.Close(); err1 != nil {
			err = errors.Join(err, fmt.Errorf("failed to close snapshot reader: %v", err1))
		}
	}()

	tempFile, err = os.CreateTemp(tempDir, tmpBackupFilePrefix)
	if err != nil {
		err = fmt.Errorf("failed to create snapshot tempfile: %v", err)
		return
	}

	written, err = io.Copy(tempFile, rc)
	if err != nil {
		err = fmt.Errorf("failed to save snapshot to tempFile: %v", err)
	}

	return
}
