// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/sirupsen/logrus"
	"k8s.io/utils/ptr"
)

const (
	absCredentialDirectory = "AZURE_APPLICATION_CREDENTIALS"      // #nosec G101 -- This is not a hardcoded password, but only a path to the credentials.
	absCredentialJSONFile  = "AZURE_APPLICATION_CREDENTIALS_JSON" // #nosec G101 -- This is not a hardcoded password, but only a path to the credentials.
	// AzuriteEndpoint is the environment variable which indicates the endpoint at which the Azurite emulator is hosted
	AzuriteEndpoint = "AZURE_STORAGE_API_ENDPOINT"
)

// AzureBlockBlobClientI defines the methods that are invoked from the Azure Block Blob API.
type AzureBlockBlobClientI interface {
	// DownloadStream reads a range of bytes from a blob. The response also includes the blob's properties and metadata.
	DownloadStream(context.Context, *blob.DownloadStreamOptions) (blob.DownloadStreamResponse, error)
	// Delete marks the specified blob or snapshot for deletion. The blob is later deleted during the internal garbage collection of Azure Blob Storage.
	// Note that deleting a blob also deletes all its snapshots.
	Delete(context.Context, *blob.DeleteOptions) (blob.DeleteResponse, error)
	// CommitBlockList writes a blob by specifying the list of block IDs that make up the blob.
	// In order to be written as part of a blob, a block must have been successfully written
	// to the server in a prior PutBlock operation. You can call PutBlockList to update a blob
	// by uploading only those blocks that have changed, then committing the new and existing
	// blocks together. Any blocks not specified in the block list and permanently deleted.
	CommitBlockList(context.Context, []string, *blockblob.CommitBlockListOptions) (blockblob.CommitBlockListResponse, error)
	// StageBlock uploads the specified block to the block blob's "staging area" to be later committed by a call to CommitBlockList.
	StageBlock(context.Context, string, io.ReadSeekCloser, *blockblob.StageBlockOptions) (blockblob.StageBlockResponse, error)
}

// azureContainerClientI defines the methods required for container operations.
type azureContainerClientI interface {
	// NewListBlobsFlatPager returns a pager for blobs starting from the specified Marker. Use an empty
	// Marker to start enumeration from the beginning. Blob names are returned in lexicographic order.
	NewListBlobsFlatPager(*container.ListBlobsFlatOptions) *runtime.Pager[container.ListBlobsFlatResponse]
	// NewBlockBlobClient creates a new blockblob.Client object by concatenating blobName to the end of
	// this Client's URL. The blob name will be URL-encoded.
	// The new blockblob.Client uses the same request policy pipeline as this Client.
	NewBlockBlobClient(string) AzureBlockBlobClientI
}

// AzureContainerClient embeds an azure *container.Client (its methods are overridden in tests).
type AzureContainerClient struct {
	*container.Client
}

// NewBlockBlobClient simply returns the BlockBlobClient that the embeded *azcontainer.Client returns.
func (a *AzureContainerClient) NewBlockBlobClient(blobName string) AzureBlockBlobClientI {
	return a.Client.NewBlockBlobClient(blobName)
}

// ABSSnapStore is an ABS backed snapstore.
type ABSSnapStore struct {
	container string
	client    azureContainerClientI
	prefix    string
	// maxParallelChunkUploads hold the maximum number of parallel chunk uploads allowed.
	maxParallelChunkUploads uint
	minChunkSize            int64
	tempDir                 string
}

type absCredentials struct {
	BucketName      string  `json:"bucketName"`
	StorageAccount  string  `json:"storageAccount"`
	StorageKey      string  `json:"storageKey"`
	Domain          *string `json:"domain,omitempty"`
	EmulatorEnabled bool    `json:"emulatorEnabled,omitempty"`
}

// NewABSSnapStore creates a new ABSSnapStore using a shared configuration and a specified bucket
func NewABSSnapStore(config *brtypes.SnapstoreConfig) (*ABSSnapStore, error) {
	absCreds, err := getCredentials(getEnvPrefixString(config.IsSource))
	if err != nil {
		return nil, fmt.Errorf("failed to get credentials: %w", err)
	}

	sharedKeyCredential, err := container.NewSharedKeyCredential(absCreds.StorageAccount, absCreds.StorageKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create sharedKeyCredential: %w", err)
	}

	emulatorEnabled := config.IsEmulatorEnabled || absCreds.EmulatorEnabled
	domain := brtypes.AzureBlobStorageGlobalDomain
	if absCreds.Domain != nil {
		domain = *absCreds.Domain
	} else {
		// if emulator is enabled, but custom domain for the emulator is not provided, throw error
		if emulatorEnabled {
			return nil, fmt.Errorf("emulator enabled, but `domain` not provided")
		}
	}

	blobServiceURL, err := ConstructBlobServiceURL(absCreds.StorageAccount, domain, emulatorEnabled)
	if err != nil {
		return nil, fmt.Errorf("failed to construct the blob service URL with error: %w", err)
	}
	containerURL := fmt.Sprintf("%s/%s", blobServiceURL, config.Container)

	client, err := container.NewClientWithSharedKeyCredential(containerURL, sharedKeyCredential, &container.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Retry: policy.RetryOptions{
				TryTimeout: downloadTimeout,
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create client with shared key credential with error: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), providerConnectionTimeout)
	defer cancel()

	if _, err := client.GetProperties(ctx, nil); err != nil {
		if bloberror.HasCode(err, bloberror.ContainerNotFound) {
			return nil, fmt.Errorf("container %s does not exist", config.Container)
		}
		return nil, fmt.Errorf("failed to get properties of the container %v with error: %w", config.Container, err)
	}

	return NewABSSnapStoreFromClient(config.Container, config.Prefix, config.TempDir, config.MaxParallelChunkUploads, config.MinChunkSize, &AzureContainerClient{client}), nil
}

// ConstructBlobServiceURL constructs the Blob Service URL based on the activation status of the Azurite Emulator.
// The `domain` must either be the default Azure global blob storage domain, or a specific domain for Azurite (without HTTP scheme).
func ConstructBlobServiceURL(storageAccount, domain string, emulatorEnabled bool) (string, error) {
	if emulatorEnabled {
		// TODO: going forward, use Azurite with HTTPS (TLS) communication
		// by using [production-style URLs](https://github.com/Azure/Azurite?tab=readme-ov-file#production-style-url)
		return fmt.Sprintf("http://%s/%s", domain, storageAccount), nil
	}
	return fmt.Sprintf("https://%s.%s", storageAccount, domain), nil
}

func getCredentials(prefixString string) (*absCredentials, error) {
	if filename, isSet := os.LookupEnv(prefixString + absCredentialJSONFile); isSet {
		credentials, err := readABSCredentialsJSON(filename)
		if err != nil {
			return nil, fmt.Errorf("error getting credentials using %v file with error %w", filename, err)
		}
		return credentials, nil
	}

	// TODO: @renormalize Remove this extra handling in v0.31.0
	// Check if a JSON file is present in the directory, if it is present -> the JSON file must be used for credentials.
	if dir, isSet := os.LookupEnv(prefixString + absCredentialDirectory); isSet {
		jsonCredentialFile, err := findFileWithExtensionInDir(dir, ".json")
		if err != nil {
			return nil, fmt.Errorf("error while finding a JSON credential file in %v directory with error: %w", dir, err)
		}
		if jsonCredentialFile != "" {
			credentials, err := readABSCredentialsJSON(jsonCredentialFile)
			if err != nil {
				return nil, fmt.Errorf("error getting credentials using %v JSON file in a directory with error: %w", jsonCredentialFile, err)
			}
			return credentials, nil
		}
		// Non JSON credential files might exist in the credential directory, do not return
	}

	if dir, isSet := os.LookupEnv(prefixString + absCredentialDirectory); isSet {
		credentials, err := readABSCredentialFiles(dir)
		if err != nil {
			return nil, fmt.Errorf("error getting credentials from %v directory with error %w", dir, err)
		}
		return credentials, nil
	}

	return nil, fmt.Errorf("unable to get credentials")
}

func readABSCredentialsJSON(filename string) (*absCredentials, error) {
	jsonData, err := os.ReadFile(filename) // #nosec G304 -- this is a trusted file, obtained via user input.
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
			data, err := os.ReadFile(path.Join(dirname, file.Name())) // #nosec G304 -- this is a trusted file, obtained via mounted secret.
			if err != nil {
				return nil, err
			}
			absConfig.StorageAccount = string(data)
		} else if file.Name() == "storageKey" {
			data, err := os.ReadFile(path.Join(dirname, file.Name())) // #nosec G304 -- this is a trusted file, obtained via mounted secret.
			if err != nil {
				return nil, err
			}
			absConfig.StorageKey = string(data)
		} else if file.Name() == "domain" {
			data, err := os.ReadFile(path.Join(dirname, file.Name())) // #nosec G304 -- this is a trusted file, obtained via mounted secret.
			if err != nil {
				return nil, err
			}
			absConfig.Domain = ptr.To(string(data))
		} else if file.Name() == "emulatorEnabled" {
			data, err := os.ReadFile(path.Join(dirname, file.Name())) // #nosec G304 -- this is a trusted file, obtained via mounted secret.
			if err != nil {
				return nil, err
			}
			emulatorEnabled, err := strconv.ParseBool(string(data))
			if err != nil {
				return nil, err
			}
			absConfig.EmulatorEnabled = emulatorEnabled
		}
	}

	if err := isABSConfigEmpty(absConfig); err != nil {
		return nil, err
	}
	return absConfig, nil
}

// NewABSSnapStoreFromClient returns a new ABS object for a given container using the supplied storageClient
func NewABSSnapStoreFromClient(container, prefix, tempDir string, maxParallelChunkUploads uint, minChunkSize int64, client azureContainerClientI) *ABSSnapStore {
	return &ABSSnapStore{
		container,
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

	blobClient := a.client.NewBlockBlobClient(blobName)

	streamResp, err := blobClient.DownloadStream(context.Background(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to download the blob %s with error: %w", blobName, err)
	}

	return streamResp.Body, nil
}

// List will return sorted list with all snapshot files on store.
func (a *ABSSnapStore) List(includeAll bool) (brtypes.SnapList, error) {
	prefixTokens := strings.Split(a.prefix, "/")
	// Last element of the tokens is backup version
	// Consider the parent of the backup version level (Required for Backward Compatibility)
	prefix := path.Join(strings.Join(prefixTokens[:len(prefixTokens)-1], "/"))
	var snapList brtypes.SnapList

	// Prefix is compulsory here, since the container could potentially be used by other instances of etcd-backup-restore
	pager := a.client.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{Prefix: &prefix,
		Include: container.ListBlobsInclude{
			Metadata:           true,
			Tags:               true,
			Versions:           true,
			ImmutabilityPolicy: true,
		},
	})
	for pager.More() {
		resp, err := pager.NextPage(context.Background())
		if err != nil {
			return nil, fmt.Errorf("failed to list the blobs, error: %w", err)
		}

	blob:
		for _, blobItem := range resp.Segment.BlobItems {
			// process the blobs returned in the result segment
			if strings.Contains(*blobItem.Name, backupVersionV1) || strings.Contains(*blobItem.Name, backupVersionV2) {
				snapshot, err := ParseSnapshot(*blobItem.Name)
				if err != nil {
					logrus.Warnf("Invalid snapshot found. Ignoring: %s", *blobItem.Name)
				} else {
					// Tagged snapshots are not listed when excluded, e.g. during restoration
					if blobItem.BlobTags != nil {
						for _, tag := range blobItem.BlobTags.BlobTagSet {
							// skip this blob
							if !includeAll && (*tag.Key == brtypes.ExcludeSnapshotMetadataKey && *tag.Value == "true") {
								logrus.Infof("Ignoring snapshot %s due to the exclude tag %q in the snapshot metadata", snapshot.SnapName, *tag.Key)
								continue blob
							}
						}
					}
					// nil check only necessary for Azurite
					if blobItem.Properties.ImmutabilityPolicyExpiresOn != nil {
						snapshot.ImmutabilityExpiryTime = *blobItem.Properties.ImmutabilityPolicyExpiresOn
					}
					snapList = append(snapList, snapshot)
				}
			}
		}
	}

	sort.Sort(snapList)
	return snapList, nil
}

// Save will write the snapshot to store
func (a *ABSSnapStore) Save(snap brtypes.Snapshot, rc io.ReadCloser) (err error) {
	tempFile, size, err := writeSnapshotToTempFile(a.tempDir, rc)
	if err != nil {
		return err
	}
	defer func() {
		err1 := tempFile.Close()
		if err1 != nil {
			err1 = fmt.Errorf("failed to close snapshot tempfile: %v", err1)
		}
		err2 := os.Remove(tempFile.Name())
		if err2 != nil {
			err2 = fmt.Errorf("failed to remove snapshot tempfile: %v", err2)
		}
		err = errors.Join(err, err1, err2)
	}()

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
		go a.blockUploader(&wg, cancelCh, &snap, tempFile, chunkUploadCh, resCh)
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
	if _, err := blobClient.StageBlock(ctx, blockID, streaming.NopCloser(sr), nil); err != nil {
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
	blobClient := a.client.NewBlockBlobClient(blobName)
	if _, err := blobClient.Delete(context.Background(), nil); bloberror.HasCode(err, bloberror.BlobImmutableDueToPolicy) {
		return fmt.Errorf("failed to delete blob %s due to immutability: %w, with provider error: %w", blobName, brtypes.ErrSnapshotDeleteFailDueToImmutability, err)
	} else if err != nil {
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
	if len(config.StorageKey) != 0 && len(config.StorageAccount) != 0 {
		return nil
	}
	return fmt.Errorf("azure object storage credentials: storageKey or storageAccount is missing")
}
