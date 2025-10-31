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

// SnapstoreWithCopier holds a snapstore and optional backup copier
type SnapstoreWithCopier struct {
	Store  brtypes.SnapStore
	Copier *BackupCopier // nil if no secondary endpoint
}

const (
	envStorageContainer          = "STORAGE_CONTAINER"
	sourceEnvStorageContainer    = "SOURCE_STORAGE_CONTAINER"
	secondaryEnvStorageContainer = "SECONDARY_STORAGE_CONTAINER"
	defaultLocalStore            = "default.bkp"
	backupVersion                = backupVersionV2
	sourcePrefixString           = "SOURCE_"
	secondaryPrefixString        = "SECONDARY_"
)

// GetSnapstore returns the snapstore object for give storageProvider with specified container
// Note: For dual endpoints, this returns only the primary store. Use GetSnapstoreWithCopier for dual endpoint management.
func GetSnapstore(config *brtypes.SnapstoreConfig) (brtypes.SnapStore, error) {
	return getSingleSnapstore(config)
}

// GetSnapstoreWithCopier returns snapstore with optional backup copier for dual endpoints
func GetSnapstoreWithCopier(config *brtypes.SnapstoreConfig) (*SnapstoreWithCopier, error) {
	logger := logrus.NewEntry(logrus.New())
	// Check if dual endpoint is configured
	if config.HasSecondaryEndpoint() {
		return createSnapstoreWithCopier(config, logger)
	}

	// Single endpoint
	store, err := getSingleSnapstore(config)
	if err != nil {
		return nil, err
	}
	return &SnapstoreWithCopier{Store: store, Copier: nil}, nil
}

// createSnapstoreWithCopier creates a snapstore with optional backup copier for dual endpoints
func createSnapstoreWithCopier(config *brtypes.SnapstoreConfig, logger *logrus.Entry) (*SnapstoreWithCopier, error) {
	var primaryStore, secondaryStore brtypes.SnapStore
	var primaryErr, secondaryErr error
	logger.Infof("Creating snapstore with backup copier - Primary: %s/%s, Secondary: %s/%s",
		config.Provider, config.Container, config.SecondaryProvider, config.SecondaryContainer)

	// Create primary snapstore
	primaryStore, primaryErr = getSingleSnapstore(config)
	if primaryErr != nil {
		logger.Errorf("Fatal error creating primary snapstore: %v", primaryErr)
	} else {
		logger.Infof("Successfully created primary snapstore")
	}

	// Create secondary snapstore configuration
	secondaryConfig := config.GetSecondaryConfig()
	if secondaryConfig == nil {
		logger.Warnf("No secondary configuration available - using single snapstore")
		if primaryErr != nil {
			return nil, fmt.Errorf("primary snapstore failed and no secondary configured: %v", primaryErr)
		}
		return &SnapstoreWithCopier{Store: primaryStore, Copier: nil}, nil
	}

	logger.Infof("Creating secondary snapstore with config - Provider: %s, Container: %s",
		secondaryConfig.Provider, secondaryConfig.Container)

	// Create secondary snapstore
	secondaryStore, secondaryErr = getSingleSnapstoreWithIdentifier(secondaryConfig, "secondary")
	if secondaryErr != nil {
		logger.Errorf("Fatal error creating secondary snapstore: %v", secondaryErr)
	} else {
		logger.Infof("Successfully created secondary snapstore")
	}

	// Determine what to return based on success/failure of each endpoint
	if primaryErr != nil && secondaryErr != nil {
		return nil, fmt.Errorf("failed to create both primary and secondary snapstores - Primary: %v, Secondary: %v", primaryErr, secondaryErr)
	}

	// Create snapstore with copier based on what we have
	if primaryStore != nil && secondaryStore != nil {
		logger.Infof("Created snapstore with backup copier for dual endpoints")
		copier := NewBackupCopier(primaryStore, secondaryStore, DefaultBackupCopierConfig(), logger)
		return &SnapstoreWithCopier{Store: primaryStore, Copier: copier}, nil
	} else if primaryStore != nil {
		logger.Warnf("Only primary snapstore available, using single endpoint")
		return &SnapstoreWithCopier{Store: primaryStore, Copier: nil}, nil
	} else if secondaryStore != nil {
		logger.Warnf("Only secondary snapstore available, using as primary")
		return &SnapstoreWithCopier{Store: secondaryStore, Copier: nil}, nil
	}

	// This should never happen
	return nil, errors.New("unexpected error creating snapstore")
}

// getSingleSnapstore returns the snapstore object for a single endpoint
func getSingleSnapstore(config *brtypes.SnapstoreConfig) (brtypes.SnapStore, error) {
	return getSingleSnapstoreWithIdentifier(config, "primary")
}

// getSingleSnapstoreWithIdentifier returns a snapstore for a single endpoint with the specified identifier
func getSingleSnapstoreWithIdentifier(config *brtypes.SnapstoreConfig, identifier string) (brtypes.SnapStore, error) {
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
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to get file info of temporary directory %s: %v", config.TempDir, err)
		}

		logrus.Infof("Temporary directory %s does not exist. Creating it...", config.TempDir)
		if err := os.MkdirAll(config.TempDir, 0700); err != nil {
			return nil, fmt.Errorf("failed to create temporary directory %s: %v", config.TempDir, err)
		}
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
		return NewS3SnapStore(config, identifier)
	case brtypes.SnapstoreProviderABS:
		return NewABSSnapStore(config)
	case brtypes.SnapstoreProviderGCS:
		return NewGCSSnapStore(config)
	case brtypes.SnapstoreProviderSwift:
		return NewSwiftSnapStore(config)
	case brtypes.SnapstoreProviderOSS:
		return NewOSSSnapStore(config)
	case brtypes.SnapstoreProviderECS:
		return NewECSSnapStore(config, identifier)
	case brtypes.SnapstoreProviderOCS:
		return NewOCSSnapStore(config, identifier)
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

// getEnvPrefixString returns the environment variable prefix string based on source configuration
func getEnvPrefixString(isSource bool) string {
	if isSource {
		return sourcePrefixString
	}
	return ""
}

// getEnvPrefixStringForConfig returns the environment variable prefix string based on snapstore configuration
func getEnvPrefixStringForConfig(config *brtypes.SnapstoreConfig) string {
	if config.IsSource {
		return sourcePrefixString
	}
	if config.IsSecondary {
		return secondaryPrefixString
	}
	return ""
}

// adaptPrefix adapts the snapstore prefix to match snapshot version compatibility
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
