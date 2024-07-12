// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	azcontainer "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	azblobold "github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/sirupsen/logrus"

	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
)

const (
	absCredentialDirectory = "AZURE_APPLICATION_CREDENTIALS"
	absCredentialJSONFile  = "AZURE_APPLICATION_CREDENTIALS_JSON"
	// AzuriteEndpoint is the environment variable which indicates the endpoint at which the Azurite emulator is hosted
	AzuriteEndpoint = "AZURE_STORAGE_API_ENDPOINT"
)

// ABSSnapStore is an ABS backed snapstore.
type ABSSnapStore struct {
	// TODO: @renormalize replace the client struct with an interface so it can be mocked
	client *azcontainer.Client
	prefix string
	// maxParallelChunkUploads hold the maximum number of parallel chunk uploads allowed.
	maxParallelChunkUploads uint
	minChunkSize            int64
	tempDir                 string
}

type absCredentials struct {
	BucketName     string `json:"bucketName"`
	SecretKey      string `json:"storageKey"`
	StorageAccount string `json:"storageAccount"`
}

// NewABSSnapStore creates a new ABSSnapStore using a shared configuration and a specified bucket
func NewABSSnapStore(config *brtypes.SnapstoreConfig) (*ABSSnapStore, error) {
	accountName, accountKey, err := getCredentials(getEnvPrefixString(config.IsSource))
	if err != nil {
		return nil, fmt.Errorf("failed to get credentials: %w", err)
	}

	absURI, err := ConstructABSURI(accountName)
	if err != nil {
		return nil, err
	}
	containerEndpoint := fmt.Sprintf("%s/%s", absURI, config.Container)

	sharedKeyCredential, err := azcontainer.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create shared key credentials: %w", err)
	}

	client, err := azcontainer.NewClientWithSharedKeyCredential(containerEndpoint, sharedKeyCredential, &azcontainer.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Retry: policy.RetryOptions{
				TryTimeout: downloadTimeout,
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create client with shared key credential with error: %w", err)
	}

	// Check if the ABS container exists (moved over from client constructor function)
	ctx, cancel := context.WithTimeout(context.TODO(), providerConnectionTimeout)
	defer cancel()
	_, err = client.GetProperties(ctx, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.ContainerNotFound) {
			return nil, fmt.Errorf("container %s does not exist", config.Container)
		}
		return nil, fmt.Errorf("failed to get properties of the container %v with error: %w", config.Container, err)
	}

	return NewABSSnapStoreFromClient(config.Container, config.Prefix, config.TempDir, config.MaxParallelChunkUploads, config.MinChunkSize, client), nil
}

// ConstructBlobServiceURL constructs the Blob Service URL based on the activation status of the Azurite Emulator.
// It checks the environment variables for emulator configuration and constructs the URL accordingly.
// The function expects two environment variables:
// - AZURE_EMULATOR_ENABLED: Indicates whether the Azurite Emulator is enabled (expects "true" or "false").
// - AZURE_STORAGE_API_ENDPOINT: Specifies the Azurite Emulator endpoint when the emulator is enabled.
func ConstructBlobServiceURL(credentials *azblobold.SharedKeyCredential) (*url.URL, error) {
	defaultURL, err := url.Parse(fmt.Sprintf("https://%s.%s", credentials.AccountName(), brtypes.AzureBlobStorageHostName))
	if err != nil {
		return nil, fmt.Errorf("failed to parse default service URL: %w", err)
	}
	emulatorEnabled, ok := os.LookupEnv(EnvAzureEmulatorEnabled)
	if !ok {
		return defaultURL, nil
	}
	isEmulator, err := strconv.ParseBool(emulatorEnabled)
	if err != nil {
		return nil, fmt.Errorf("invalid value for %s: %s, error: %w", EnvAzureEmulatorEnabled, emulatorEnabled, err)
	}
	if !isEmulator {
		return defaultURL, nil
	}
	endpoint, ok := os.LookupEnv(AzuriteEndpoint)
	if !ok {
		return nil, fmt.Errorf("%s environment variable not set while %s is true", AzuriteEndpoint, EnvAzureEmulatorEnabled)
	}
	// Application protocol (http or https) is determined by the user of the Azurite, not by this function.
	return url.Parse(fmt.Sprintf("%s/%s", endpoint, credentials.AccountName()))
}

func ConstructABSURI(accountName string) (string, error) {
	defaultURL := fmt.Sprintf("https://%s.%s", accountName, brtypes.AzureBlobStorageHostName)

	emulatorEnabled, ok := os.LookupEnv(EnvAzureEmulatorEnabled)
	if !ok {
		return defaultURL, nil
	}

	isEmulator, err := strconv.ParseBool(emulatorEnabled)
	if err != nil {
		return "", fmt.Errorf("invalid value for %s: %s, error: %w", EnvAzureEmulatorEnabled, emulatorEnabled, err)
	}
	if !isEmulator {
		return defaultURL, nil
	}

	endpoint, ok := os.LookupEnv(AzuriteEndpoint)
	if !ok {
		return "", fmt.Errorf("%s environment variable not set while %s is true", AzuriteEndpoint, EnvAzureEmulatorEnabled)
	}

	// Application protocol (http or https) is determined by the user of the Azurite, not by this function.
	return fmt.Sprintf("%s/%s", endpoint, accountName), nil
}

func getCredentials(prefixString string) (string, string, error) {
	if filename, isSet := os.LookupEnv(prefixString + absCredentialJSONFile); isSet {
		credentials, err := readABSCredentialsJSON(filename)
		if err != nil {
			return "", "", fmt.Errorf("error getting credentials using %v file", filename)
		}
		return credentials.StorageAccount, credentials.SecretKey, nil
	}

	// TODO: @renormalize Remove this extra handling in v0.31.0
	// Check if a JSON file is present in the directory, if it is present -> the JSON file must be used for credentials.
	if dir, isSet := os.LookupEnv(prefixString + absCredentialDirectory); isSet {
		jsonCredentialFile, err := findFileWithExtensionInDir(dir, ".json")
		if err != nil {
			return "", "", fmt.Errorf("error while finding a JSON credential file in %v directory with error: %w", dir, err)
		}
		if jsonCredentialFile != "" {
			credentials, err := readABSCredentialsJSON(jsonCredentialFile)
			if err != nil {
				return "", "", fmt.Errorf("error getting credentials using %v JSON file in a directory with error: %w", jsonCredentialFile, err)
			}
			return credentials.StorageAccount, credentials.SecretKey, nil
		}
		// Non JSON credential files might exist in the credential directory, do not return
	}

	if dir, isSet := os.LookupEnv(prefixString + absCredentialDirectory); isSet {
		credentials, err := readABSCredentialFiles(dir)
		if err != nil {
			return "", "", fmt.Errorf("error getting credentials from %v dir", dir)
		}
		return credentials.StorageAccount, credentials.SecretKey, nil
	}

	return "", "", fmt.Errorf("unable to get credentials")
}

func readABSCredentialsJSON(filename string) (*absCredentials, error) {
	jsonData, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	return absCredentialsFromJSON(jsonData)
}

// absCredentialsFromJSON obtains ABS credentials from a JSON value.
func absCredentialsFromJSON(jsonData []byte) (*absCredentials, error) {
	absConfig := &absCredentials{}
	if err := json.Unmarshal(jsonData, absConfig); err != nil {
		return nil, err
	}

	return absConfig, nil
}

func readABSCredentialFiles(dirname string) (*absCredentials, error) {
	absConfig := &absCredentials{}

	files, err := os.ReadDir(dirname)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if file.Name() == "storageAccount" {
			data, err := os.ReadFile(dirname + "/storageAccount")
			if err != nil {
				return nil, err
			}
			absConfig.StorageAccount = string(data)
		} else if file.Name() == "storageKey" {
			data, err := os.ReadFile(dirname + "/storageKey")
			if err != nil {
				return nil, err
			}
			absConfig.SecretKey = string(data)
		}
	}

	if err := isABSConfigEmpty(absConfig); err != nil {
		return nil, err
	}
	return absConfig, nil
}

// NewABSSnapStoreFromClient returns a new ABS object for a given container using the supplied storageClient
func NewABSSnapStoreFromClient(container, prefix, tempDir string, maxParallelChunkUploads uint, minChunkSize int64, client *azcontainer.Client) *ABSSnapStore {
	return &ABSSnapStore{
		client,
		prefix,
		maxParallelChunkUploads,
		minChunkSize,
		tempDir,
	}
}

// Fetch should open reader for the snapshot file from store
func (a *ABSSnapStore) Fetch(snap brtypes.Snapshot) (io.ReadCloser, error) {
	blobName := path.Join(snap.Prefix, snap.SnapDir, snap.SnapName)

	blobClient := a.client.NewBlobClient(blobName)

	streamResp, err := blobClient.DownloadStream(context.Background(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to download the blob %s with error: %w", blobName, err)
	}

	return streamResp.Body, nil
}

// List will return sorted list with all snapshot files on store.
func (a *ABSSnapStore) List() (brtypes.SnapList, error) {
	prefixTokens := strings.Split(a.prefix, "/")
	// Last element of the tokens is backup version
	// Consider the parent of the backup version level (Required for Backward Compatibility)
	prefix := path.Join(strings.Join(prefixTokens[:len(prefixTokens)-1], "/"))
	var snapList brtypes.SnapList

	// Prefix is compulsory here, since the container could potentially be used by other instances of etcd-backup-restore
	pager := a.client.NewListBlobsFlatPager(&azcontainer.ListBlobsFlatOptions{Prefix: &prefix})
	for pager.More() {
		resp, err := pager.NextPage(context.Background())
		if err != nil {
			return nil, fmt.Errorf("failed to list the blobs, error: %w", err)
		}

		for _, blob := range resp.Segment.BlobItems {
			// process the blobs returned in the result segment
			if strings.Contains(*blob.Name, backupVersionV1) || strings.Contains(*blob.Name, backupVersionV2) {
				//the blob may contain the full path in its name including the prefix
				blobName := strings.TrimPrefix(*blob.Name, prefix)
				s, err := ParseSnapshot(path.Join(prefix, blobName))
				if err != nil {
					logrus.Warnf("Invalid snapshot found. Ignoring it:%s\n", *blob.Name)
				} else {
					snapList = append(snapList, s)
				}
			}
		}
	}

	sort.Sort(snapList)
	return snapList, nil
}

// Save will write the snapshot to store
func (a *ABSSnapStore) Save(snap brtypes.Snapshot, rc io.ReadCloser) error {
	// Save it locally
	tmpfile, err := os.CreateTemp(a.tempDir, tmpBackupFilePrefix)
	if err != nil {
		rc.Close()
		return fmt.Errorf("failed to create snapshot tempfile: %w", err)
	}
	defer func() {
		tmpfile.Close()
		os.Remove(tmpfile.Name())
	}()
	size, err := io.Copy(tmpfile, rc)
	rc.Close()
	if err != nil {
		return fmt.Errorf("failed to save snapshot to tmpfile: %w", err)
	}

	var (
		chunkSize  = a.minChunkSize
		noOfChunks = size / chunkSize
	)
	if size%chunkSize != 0 {
		noOfChunks++
	}

	var (
		chunkUploadCh = make(chan chunk, noOfChunks)
		resCh         = make(chan chunkUploadResult, noOfChunks)
		wg            sync.WaitGroup
		cancelCh      = make(chan struct{})
	)

	for i := uint(0); i < a.maxParallelChunkUploads; i++ {
		wg.Add(1)
		go a.blockUploader(&wg, cancelCh, &snap, tmpfile, chunkUploadCh, resCh)
	}
	logrus.Infof("Uploading snapshot of size: %d, chunkSize: %d, noOfChunks: %d", size, chunkSize, noOfChunks)
	for offset, index := int64(0), 1; offset < size; offset += int64(chunkSize) {
		newChunk := chunk{
			offset: offset,
			size:   chunkSize,
			id:     index,
		}
		chunkUploadCh <- newChunk
		index++
	}
	logrus.Infof("Triggered chunk upload for all chunks, total: %d", noOfChunks)

	snapshotErr := collectChunkUploadError(chunkUploadCh, resCh, cancelCh, noOfChunks)
	wg.Wait()
	if snapshotErr != nil {
		return fmt.Errorf("failed uploading chunk, id: %d, offset: %d, error: %w", snapshotErr.chunk.id, snapshotErr.chunk.offset, snapshotErr.err)
	}
	logrus.Info("All chunk uploaded successfully. Uploading blocklist.")

	blobName := path.Join(adaptPrefix(&snap, a.prefix), snap.SnapDir, snap.SnapName)
	var blockList []string
	for partNumber := int64(1); partNumber <= noOfChunks; partNumber++ {
		blockID := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%010d", partNumber)))
		blockList = append(blockList, blockID)
	}

	blobClient := a.client.NewBlockBlobClient(blobName)
	ctx, cancel := context.WithTimeout(context.TODO(), chunkUploadTimeout)
	defer cancel()
	if _, err := blobClient.CommitBlockList(ctx, blockList, nil); err != nil {
		return fmt.Errorf("failed uploading blocklist for snapshot with error: %w", err)
	}
	logrus.Info("Blocklist uploaded successfully.")
	return nil
}

func (a *ABSSnapStore) uploadBlock(snap *brtypes.Snapshot, file *os.File, offset, chunkSize int64) error {
	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}

	size := fileInfo.Size() - offset
	if size > chunkSize {
		size = chunkSize
	}

	sr := io.NewSectionReader(file, offset, size)
	blobName := path.Join(adaptPrefix(snap, a.prefix), snap.SnapDir, snap.SnapName)
	partNumber := ((offset / chunkSize) + 1)
	blockID := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%010d", partNumber)))

	ctx, cancel := context.WithTimeout(context.TODO(), chunkUploadTimeout)
	defer cancel()
	blobClient := a.client.NewBlockBlobClient(blobName)
	// TODO: @renormalize MD5 validation for the blocks was not done previously, should this be performed now?
	if _, err := blobClient.StageBlock(ctx, blockID, NopCloser(sr), nil); err != nil {
		return fmt.Errorf("failed to upload chunk offset: %d, blob: %s, error: %w", offset, blobName, err)
	}

	return nil
}

func (a *ABSSnapStore) blockUploader(wg *sync.WaitGroup, stopCh <-chan struct{}, snap *brtypes.Snapshot, file *os.File, chunkUploadCh chan chunk, errCh chan<- chunkUploadResult) {
	defer wg.Done()
	for {
		select {
		case <-stopCh:
			return
		case chunk, ok := <-chunkUploadCh:
			if !ok {
				return
			}
			logrus.Infof("Uploading chunk with offset : %d, attempt: %d", chunk.offset, chunk.attempt)
			err := a.uploadBlock(snap, file, chunk.offset, chunk.size)
			errCh <- chunkUploadResult{
				err:   err,
				chunk: &chunk,
			}
		}
	}
}

// Delete should delete the snapshot file from store
func (a *ABSSnapStore) Delete(snap brtypes.Snapshot) error {
	blobName := path.Join(snap.Prefix, snap.SnapDir, snap.SnapName)
	blobClient := a.client.NewBlobClient(blobName)
	// Delete options can be mentioned once support for immutability is added
	if _, err := blobClient.Delete(context.Background(), nil); err != nil {
		return fmt.Errorf("failed to delete blob %s with error: %w", blobName, err)
	}
	return nil
}

// GetABSCredentialsLastModifiedTime returns the latest modification timestamp of the ABS credential file(s)
func GetABSCredentialsLastModifiedTime() (time.Time, error) {
	// TODO: @renormalize Remove this extra handling in v0.31.0
	// Check if a JSON file is present in the directory, if it is present -> the JSON file must be used for credentials.
	if dir, isSet := os.LookupEnv(absCredentialDirectory); isSet {
		modificationTimeStamp, err := getJSONCredentialModifiedTime(dir)
		if err != nil {
			return time.Time{}, fmt.Errorf("failed to fetch credential modification time for ABS with error: %w", err)
		}
		if !modificationTimeStamp.IsZero() {
			return modificationTimeStamp, nil
		}
		// Non JSON credential files might exist in the credential directory, do not return
	}

	if dir, isSet := os.LookupEnv(absCredentialDirectory); isSet {
		// credential files which are essential for creating the snapstore
		credentialFiles := []string{"storageKey", "storageAccount"}
		for i := range credentialFiles {
			credentialFiles[i] = filepath.Join(dir, credentialFiles[i])
		}
		absTimeStamp, err := getLatestCredentialsModifiedTime(credentialFiles)
		if err != nil {
			return time.Time{}, fmt.Errorf("failed to get ABS credential timestamp from the directory %v with error: %w", dir, err)
		}
		return absTimeStamp, nil
	}

	if filename, isSet := os.LookupEnv(absCredentialJSONFile); isSet {
		credentialFiles := []string{filename}
		absTimeStamp, err := getLatestCredentialsModifiedTime(credentialFiles)
		if err != nil {
			return time.Time{}, fmt.Errorf("failed to fetch file information of the ABS JSON credential file %v with error: %w", filename, err)
		}
		return absTimeStamp, nil
	}

	return time.Time{}, fmt.Errorf("no environment variable set for the ABS credential file")
}

func isABSConfigEmpty(config *absCredentials) error {
	if len(config.SecretKey) != 0 && len(config.StorageAccount) != 0 {
		return nil
	}
	return fmt.Errorf("azure object storage credentials: storageKey or storageAccount is missing")
}

// Helper struct that implements io.ReadSeekCloser which is required by (*blob.Client) StageBlock()
type nopCloser struct {
	io.ReadSeeker
}

func (n nopCloser) Close() error {
	return nil
}

func NopCloser(rs io.ReadSeeker) io.ReadSeekCloser {
	return nopCloser{rs}
}
