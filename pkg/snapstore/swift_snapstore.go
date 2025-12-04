// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

	"github.com/gophercloud/gophercloud"
	"github.com/gophercloud/gophercloud/openstack"
	"github.com/gophercloud/gophercloud/openstack/objectstorage/v1/objects"
	"github.com/gophercloud/gophercloud/pagination"
	"github.com/gophercloud/utils/openstack/clientconfig"
	"github.com/sirupsen/logrus"
)

const (
	envPrefixSource                 = "SOURCE_OS_"
	authTypePassword                = "password"
	authTypeV3ApplicationCredential = "v3applicationcredential"
	swiftCredentialDirectory        = "OPENSTACK_APPLICATION_CREDENTIALS"      // #nosec G101 -- This is not a hardcoded password, but only a path to the credentials.
	swiftCredentialJSONFile         = "OPENSTACK_APPLICATION_CREDENTIALS_JSON" // #nosec G101 -- This is not a hardcoded password, but only a path to the credentials.
)

// SwiftSnapStore is snapstore with Openstack Swift as backend
type SwiftSnapStore struct {
	client  *gophercloud.ServiceClient
	prefix  string
	bucket  string
	tempDir string
	// maxParallelChunkUploads hold the maximum number of parallel chunk uploads allowed.
	maxParallelChunkUploads uint
	minChunkSize            int64
}

type applicationCredential struct {
	ApplicationCredentialID     string `json:"applicationCredentialID"`
	ApplicationCredentialName   string `json:"applicationCredentialName"`
	ApplicationCredentialSecret string `json:"applicationCredentialSecret"`
}

type passwordCredential struct {
	Password string `json:"password"`
	Username string `json:"username"`
}

type swiftCredentials struct {
	AuthType   string `json:"authType"`
	AuthURL    string `json:"authURL"`
	BucketName string `json:"bucketName"`
	DomainName string `json:"domainName"`
	Region     string `json:"region"`
	TenantName string `json:"tenantName"`
	passwordCredential
	applicationCredential
}

const (
	// Total number of chunks to be uploaded must be one less than maximum limit allowed.
	swiftNoOfChunk int64 = 999 //Default configuration in swift installation
)

// NewSwiftSnapStore create new SwiftSnapStore from shared configuration with specified bucket
func NewSwiftSnapStore(config *brtypes.SnapstoreConfig) (*SwiftSnapStore, error) {
	clientOpts, err := getClientOpts(config)
	if err != nil {
		return nil, err
	}

	authOpts, err := clientconfig.AuthOptions(clientOpts)
	if err != nil {
		return nil, err
	}
	// AllowReauth should be set to true if you grant permission for Gophercloud to
	// cache your credentials in memory, and to allow Gophercloud to attempt to
	// re-authenticate automatically if/when your token expires.
	authOpts.AllowReauth = true
	provider, err := openstack.AuthenticatedClient(*authOpts)
	if err != nil {
		return nil, err

	}
	client, err := openstack.NewObjectStorageV1(provider, gophercloud.EndpointOpts{
		Region: os.Getenv("OS_REGION_NAME"),
	})
	if err != nil {
		return nil, err
	}

	return NewSwiftSnapstoreFromClient(config.Container, config.Prefix, config.TempDir, config.MaxParallelChunkUploads, config.MinChunkSize, client), nil

}

func getClientOpts(config *brtypes.SnapstoreConfig) (*clientconfig.ClientOpts, error) {
	prefix := getEnvPrefixString(config)
	if filename, isSet := os.LookupEnv(prefix + swiftCredentialJSONFile); isSet {
		clientOpts, err := readSwiftCredentialsJSON(filename)
		if err != nil {
			return &clientconfig.ClientOpts{}, fmt.Errorf("error getting credentials using %v file with error: %w", filename, err)
		}
		return clientOpts, nil
	}

	// TODO: @renormalize Remove this extra handling in v0.31.0
	// Check if a JSON file is present in the directory, if it is present -> the JSON file must be used for credentials.
	if dir, isSet := os.LookupEnv(prefix + swiftCredentialDirectory); isSet {
		jsonCredentialFile, err := findFileWithExtensionInDir(dir, ".json")
		if err != nil {
			return &clientconfig.ClientOpts{}, fmt.Errorf("error getting credentials using %v JSON file in a directory with error: %w", jsonCredentialFile, err)
		}
		if jsonCredentialFile != "" {
			clientOpts, err := readSwiftCredentialsJSON(jsonCredentialFile)
			if err != nil {
				return &clientconfig.ClientOpts{}, fmt.Errorf("error getting credentials using %v JSON file in a directory with error: %w", jsonCredentialFile, err)
			}
			return clientOpts, nil
		}
		// Non JSON credential files might exist in the credential directory, do not return
	}

	if dir, isSet := os.LookupEnv(prefix + swiftCredentialDirectory); isSet {
		clientOpts, err := readSwiftCredentialFiles(dir)
		if err != nil {
			return &clientconfig.ClientOpts{}, fmt.Errorf("error getting credentials from %v directory with error: %w", dir, err)
		}
		return clientOpts, nil
	}

	// If a neither a swiftCredentialFile nor a swiftCredentialJSONFile was found, fall back to
	// retrieving credentials from environment variables.
	// If the snapstore is used as source during a copy operation all environment variables have a SOURCE_OS_ prefix.
	if config.IsSource {
		return &clientconfig.ClientOpts{EnvPrefix: envPrefixSource}, nil
	}

	// Otherwise, the environment variable prefix is defined in each function that attempts to get the environment variable.
	// For example: https://github.com/gardener/etcd-backup-restore/blob/9e80a31b8f319b1d29f07781b609a97bfff65916/vendor/github.com/gophercloud/utils/openstack/clientconfig/requests.go#L335
	// That is why it must be left empty.
	return &clientconfig.ClientOpts{}, nil
}

func readSwiftCredentialsJSON(filename string) (*clientconfig.ClientOpts, error) {
	cred, err := swiftCredentialsFromJSON(filename)
	if err != nil {
		return &clientconfig.ClientOpts{}, err
	}

	if err := os.Setenv("OS_TENANT_NAME", cred.TenantName); err != nil {
		return nil, fmt.Errorf("error setting tenant name: %w", err)
	}

	if cred.AuthType == authTypeV3ApplicationCredential {
		return &clientconfig.ClientOpts{
			AuthType: authTypeV3ApplicationCredential,
			AuthInfo: &clientconfig.AuthInfo{
				AuthURL:                     cred.AuthURL,
				DomainName:                  cred.DomainName,
				ApplicationCredentialID:     cred.ApplicationCredentialID,
				ApplicationCredentialName:   cred.ApplicationCredentialName,
				ApplicationCredentialSecret: cred.ApplicationCredentialSecret,
			},
			RegionName: cred.Region,
		}, nil
	}

	// if authType == authTypePassword
	return &clientconfig.ClientOpts{
		AuthInfo: &clientconfig.AuthInfo{
			AuthURL:    cred.AuthURL,
			DomainName: cred.DomainName,
			Password:   cred.Password,
			Username:   cred.Username,
		},
		RegionName: cred.Region,
	}, nil
}

// swiftCredentialsFromJSON obtains Swift credentials from a JSON value.
func swiftCredentialsFromJSON(filename string) (*swiftCredentials, error) {
	cred := &swiftCredentials{}
	jsonData, err := os.ReadFile(filename) // #nosec G304 -- this is a trusted file, obtained via user input.
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(jsonData, cred); err != nil {
		return nil, err
	}

	if cred.AuthType, err = getSwiftCredentialAuthTypeFromJSON(cred.ApplicationCredentialSecret, cred.Username); err != nil {
		return cred, err
	}

	return cred, nil
}

func readSwiftCredentialFiles(dirname string) (*clientconfig.ClientOpts, error) {
	cred, err := readSwiftCredentialDir(dirname)
	if err != nil {
		return nil, err
	}

	if err := os.Setenv("OS_TENANT_NAME", cred.TenantName); err != nil {
		return nil, fmt.Errorf("error setting tenant name: %w", err)
	}

	if cred.AuthType == authTypeV3ApplicationCredential {
		return &clientconfig.ClientOpts{
			AuthType: authTypeV3ApplicationCredential,
			AuthInfo: &clientconfig.AuthInfo{
				AuthURL:                     cred.AuthURL,
				DomainName:                  cred.DomainName,
				ApplicationCredentialID:     cred.ApplicationCredentialID,
				ApplicationCredentialName:   cred.ApplicationCredentialName,
				ApplicationCredentialSecret: cred.ApplicationCredentialSecret,
			},
			RegionName: cred.Region,
		}, nil
	}

	// if authType == authTypePassword
	return &clientconfig.ClientOpts{
		AuthInfo: &clientconfig.AuthInfo{
			AuthURL:    cred.AuthURL,
			DomainName: cred.DomainName,
			Password:   cred.Password,
			Username:   cred.Username,
		},
		RegionName: cred.Region,
	}, nil
}

func readSwiftCredentialDir(dirName string) (*swiftCredentials, error) {
	cred := &swiftCredentials{}
	files, err := os.ReadDir(dirName)
	if err != nil {
		return nil, err
	}

	if cred.AuthType, err = getSwiftCredentialAuthType(files); err != nil {
		return cred, err
	}

	for _, file := range files {
		switch file.Name() {
		case "authURL":
			data, err := os.ReadFile(dirName + "/authURL") // #nosec G304 -- this is a trusted file, obtained via user input.
			if err != nil {
				return nil, err
			}
			cred.AuthURL = string(data)

		case "domainName":
			data, err := os.ReadFile(dirName + "/domainName") // #nosec G304 -- this is a trusted file, obtained via user input.
			if err != nil {
				return nil, err
			}
			cred.DomainName = string(data)
		case "password":
			data, err := os.ReadFile(dirName + "/password") // #nosec G304 -- this is a trusted file, obtained via user input.
			if err != nil {
				return nil, err
			}
			cred.Password = string(data)
		case "region":
			data, err := os.ReadFile(dirName + "/region") // #nosec G304 -- this is a trusted file, obtained via user input.
			if err != nil {
				return nil, err
			}
			cred.Region = string(data)
		case "tenantName":
			data, err := os.ReadFile(dirName + "/tenantName") // #nosec G304 -- this is a trusted file, obtained via user input.
			if err != nil {
				return nil, err
			}
			cred.TenantName = string(data)
		case "username":
			data, err := os.ReadFile(dirName + "/username") // #nosec G304 -- this is a trusted file, obtained via user input.
			if err != nil {
				return nil, err
			}
			cred.Username = string(data)
		case "applicationCredentialID":
			data, err := os.ReadFile(dirName + "/applicationCredentialID") // #nosec G304 -- this is a trusted file, obtained via user input.
			if err != nil {
				return nil, err
			}
			cred.ApplicationCredentialID = string(data)
		case "applicationCredentialName":
			data, err := os.ReadFile(dirName + "/applicationCredentialName") // #nosec G304 -- this is a trusted file, obtained via user input.
			if err != nil {
				return nil, err
			}
			cred.ApplicationCredentialName = string(data)
		case "applicationCredentialSecret":
			data, err := os.ReadFile(dirName + "/applicationCredentialSecret") // #nosec G304 -- this is a trusted file, obtained via user input.
			if err != nil {
				return nil, err
			}
			cred.ApplicationCredentialSecret = string(data)
		}
	}

	if err := isSwiftConfigCorrect(cred); err != nil {
		return nil, err
	}
	return cred, nil
}

// NewSwiftSnapstoreFromClient will create the new Swift snapstore object from Swift client
func NewSwiftSnapstoreFromClient(bucket, prefix, tempDir string, maxParallelChunkUploads uint, minChunkSize int64, cli *gophercloud.ServiceClient) *SwiftSnapStore {
	return &SwiftSnapStore{
		bucket:                  bucket,
		prefix:                  prefix,
		client:                  cli,
		maxParallelChunkUploads: maxParallelChunkUploads,
		minChunkSize:            minChunkSize,
		tempDir:                 tempDir,
	}
}

// Fetch should open reader for the snapshot file from store
func (s *SwiftSnapStore) Fetch(snap brtypes.Snapshot) (io.ReadCloser, error) {
	resp := objects.Download(s.client, s.bucket, path.Join(snap.Prefix, snap.SnapDir, snap.SnapName), nil)
	return resp.Body, resp.Err
}

// Save will write the snapshot to store, as a DLO (dynamic large object), as described
// in https://docs.openstack.org/swift/latest/overview_large_objects.html
func (s *SwiftSnapStore) Save(snap brtypes.Snapshot, rc io.ReadCloser) (err error) {
	tempFile, size, err := writeSnapshotToTempFile(s.tempDir, rc)
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
		chunkSize  = int64(math.Max(float64(s.minChunkSize), float64(size/swiftNoOfChunk)))
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

	for i := uint(0); i < s.maxParallelChunkUploads; i++ {
		wg.Add(1)
		go s.chunkUploader(&wg, cancelCh, &snap, tempFile, chunkUploadCh, resCh)
	}

	logrus.Infof("Uploading snapshot of size: %d, chunkSize: %d, noOfChunks: %d", size, chunkSize, noOfChunks)
	for offset, index := int64(0), 1; offset < size; offset += int64(chunkSize) {
		newChunk := chunk{
			id:     index,
			offset: offset,
			size:   chunkSize,
		}
		chunkUploadCh <- newChunk
		index++
	}
	logrus.Infof("Triggered chunk upload for all chunks, total: %d", noOfChunks)

	snapshotErr := collectChunkUploadError(chunkUploadCh, resCh, cancelCh, noOfChunks)
	wg.Wait()

	if snapshotErr != nil {
		return fmt.Errorf("failed uploading chunk, id: %d, offset: %d, error: %v", snapshotErr.chunk.id, snapshotErr.chunk.offset, snapshotErr.err)
	}
	logrus.Info("All chunk uploaded successfully. Uploading manifest.")
	b := make([]byte, 0)
	prefix := adaptPrefix(&snap, s.prefix)
	opts := objects.CreateOpts{
		Content:        bytes.NewReader(b),
		ContentLength:  chunkSize,
		ObjectManifest: path.Join(s.bucket, prefix, snap.SnapDir, snap.SnapName),
	}
	if res := objects.Create(s.client, s.bucket, path.Join(prefix, snap.SnapDir, snap.SnapName), opts); res.Err != nil {
		return fmt.Errorf("failed uploading manifest for snapshot with error: %v", res.Err)
	}
	logrus.Info("Manifest object uploaded successfully.")
	return nil
}

func (s *SwiftSnapStore) uploadChunk(snap *brtypes.Snapshot, file *os.File, offset, chunkSize int64) error {
	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}
	size := fileInfo.Size() - offset
	if size > chunkSize {
		size = chunkSize
	}

	sr := io.NewSectionReader(file, offset, size)

	opts := objects.CreateOpts{
		Content:       sr,
		ContentLength: size,
	}
	partNumber := ((offset / chunkSize) + 1)
	res := objects.Create(s.client, s.bucket, path.Join(adaptPrefix(snap, s.prefix), snap.SnapDir, snap.SnapName, fmt.Sprintf("%010d", partNumber)), opts)
	return res.Err
}

func (s *SwiftSnapStore) chunkUploader(wg *sync.WaitGroup, stopCh <-chan struct{}, snap *brtypes.Snapshot, file *os.File, chunkUploadCh chan chunk, errCh chan<- chunkUploadResult) {
	defer wg.Done()
	for {
		select {
		case <-stopCh:
			return
		case uploadChunk, ok := <-chunkUploadCh:
			if !ok {
				return
			}
			logrus.Infof("Uploading chunk with offset : %d, attempt: %d", uploadChunk.offset, uploadChunk.attempt)
			err := s.uploadChunk(snap, file, uploadChunk.offset, uploadChunk.size)
			errCh <- chunkUploadResult{
				err:   err,
				chunk: &uploadChunk,
			}
		}
	}
}

// List will return sorted list with all snapshot files on store.
func (s *SwiftSnapStore) List(_ bool) (brtypes.SnapList, error) {
	prefixTokens := strings.Split(s.prefix, "/")
	// Last element of the tokens is backup version
	// Consider the parent of the backup version level (Required for Backward Compatibility)
	prefix := path.Join(strings.Join(prefixTokens[:len(prefixTokens)-1], "/"))

	opts := &objects.ListOpts{
		Full:   false,
		Prefix: prefix,
	}
	// Retrieve a pager (i.e. a paginated collection)
	pager := objects.List(s.client, s.bucket, opts)
	var snapList brtypes.SnapList
	// Define an anonymous function to be executed on each page's iteration
	err := pager.EachPage(func(page pagination.Page) (bool, error) {

		objectList, err := objects.ExtractNames(page)
		if err != nil {
			return false, err
		}
		for _, object := range objectList {
			if strings.Contains(object, backupVersionV1) || strings.Contains(object, backupVersionV2) {
				snap, err := ParseSnapshot(object)
				if err != nil {
					// Warning: the file can be a non snapshot file. Do not return error.
					logrus.Warnf("Invalid snapshot found. Ignoring it:%s, %v", object, err)
				} else {
					snapList = append(snapList, snap)
				}
			}
		}
		return true, nil

	})
	if err != nil {
		return nil, err
	}

	sort.Sort(snapList)
	return snapList, nil
}

func (s *SwiftSnapStore) getSnapshotChunks(snapshot brtypes.Snapshot) (brtypes.SnapList, error) {
	snaps, err := s.List(false)
	if err != nil {
		return nil, err
	}

	var chunkList brtypes.SnapList
	for _, snap := range snaps {
		if snap.IsChunk {
			chunkParentSnapPath, _ := path.Split(path.Join(snap.Prefix, snap.SnapDir, snap.SnapName))
			if strings.TrimSuffix(chunkParentSnapPath, "/") == path.Join(snapshot.Prefix, snapshot.SnapDir, snapshot.SnapName) {
				chunkList = append(chunkList, snap)
			}
		}
	}
	return chunkList, nil
}

// Delete deletes the objects related to the DLO (dynamic large object) from the store.
// This includes the manifest object as well as the segment objects, as
// described in https://docs.openstack.org/swift/latest/overview_large_objects.html
func (s *SwiftSnapStore) Delete(snap brtypes.Snapshot) error {
	chunks, err := s.getSnapshotChunks(snap)
	if err != nil {
		return err
	}

	if len(chunks) > 0 {
		var chunkObjectNames []string
		for _, chunk := range chunks {
			chunkObjectNames = append(chunkObjectNames, path.Join(chunk.Prefix, chunk.SnapDir, chunk.SnapName))
		}

		chunkObjectsDeleteResult := objects.BulkDelete(s.client, s.bucket, chunkObjectNames)
		if chunkObjectsDeleteResult.Err != nil {
			// chunkObjectsDeleteResult.Err is the error that occurs while creating the POST request
			return chunkObjectsDeleteResult.Err
		}

		// See https://docs.openstack.org/swift/latest/api/bulk-delete.html for more information about this error handling
		var bulkDeleteErrors []error
		bulkDeleteResponse, err := chunkObjectsDeleteResult.Extract()
		if err != nil {
			return fmt.Errorf("error while extracting the bulk delete information from the response %w", err)
		}

		if bulkDeleteResponse.NumberNotFound > 0 {
			bulkDeleteErrors = append(bulkDeleteErrors, fmt.Errorf("error while deleting segment objects with error: %d segment objects not found in the store", bulkDeleteResponse.NumberNotFound))
		}
		for _, errorsPerObject := range bulkDeleteResponse.Errors {
			var filteredErrorStrings []string
			for _, errorString := range errorsPerObject {
				if errorString != "" {
					filteredErrorStrings = append(filteredErrorStrings, errorString)
				}
			}
			bulkDeleteErrorString := strings.Join(filteredErrorStrings, ", ")
			if bulkDeleteErrorString != "" {
				bulkDeleteErrors = append(bulkDeleteErrors, fmt.Errorf("error while deleting segment object with error: %s", bulkDeleteErrorString))
			}
		}
		if len(bulkDeleteErrors) > 0 {
			return errors.Join(bulkDeleteErrors...)
		}
	}

	// delete manifest object
	return objects.Delete(s.client, s.bucket, path.Join(snap.Prefix, snap.SnapDir, snap.SnapName), nil).Err
}

// GetSwiftCredentialsLastModifiedTime returns the latest modification timestamp of the Swift credential file(s)
func GetSwiftCredentialsLastModifiedTime() (time.Time, error) {
	// TODO: @renormalize Remove this extra handling in v0.31.0
	// Check if a JSON file is present in the directory, if it is present -> the JSON file must be used for credentials.
	if dir, isSet := os.LookupEnv(swiftCredentialDirectory); isSet {
		modificationTimeStamp, err := getJSONCredentialModifiedTime(dir)
		if err != nil {
			return time.Time{}, fmt.Errorf("failed to fetch credential modification time for Swift with error: %w", err)
		}
		if !modificationTimeStamp.IsZero() {
			return modificationTimeStamp, nil
		}
		// Non JSON credential files might exist in the credential directory, do not return
	}

	if dir, isSet := os.LookupEnv(swiftCredentialDirectory); isSet {
		// credential files which are essential for creating the snapstore
		var credentialFiles []string
		// checking authType
		dirEntries, err := os.ReadDir(dir)
		if err != nil {
			return time.Time{}, err
		}
		authType, err := getSwiftCredentialAuthType(dirEntries)
		if err != nil {
			return time.Time{}, err
		}

		if authType == authTypePassword {
			credentialFiles = []string{"authURL", "tenantName", "domainName", "username", "password"}
		} else {
			credentialFiles = []string{"authURL", "tenantName", "domainName", "applicationCredentialID", "applicationCredentialName", "applicationCredentialSecret"}
		}
		for i := range credentialFiles {
			credentialFiles[i] = filepath.Join(dir, credentialFiles[i])
		}

		swiftTimeStamp, err := getLatestCredentialsModifiedTime(credentialFiles)
		if err != nil {
			return time.Time{}, fmt.Errorf("failed to get Swift credential timestamp from the directory %v with error: %w", dir, err)
		}
		return swiftTimeStamp, nil
	}

	if filename, isSet := os.LookupEnv(swiftCredentialJSONFile); isSet {
		credentialFiles := []string{filename}
		swiftTimeStamp, err := getLatestCredentialsModifiedTime(credentialFiles)
		if err != nil {
			return time.Time{}, fmt.Errorf("failed to fetch file information of the Swift JSON credential file %v with error: %w", filename, err)
		}
		return swiftTimeStamp, nil
	}

	return time.Time{}, fmt.Errorf("no environment variable set for the Swift credential file")
}

func isSwiftConfigCorrect(config *swiftCredentials) error {
	if (len(config.ApplicationCredentialSecret) != 0 || len(config.ApplicationCredentialID) != 0) && (len(config.Password) != 0 || len(config.Username) != 0) {
		return fmt.Errorf("openstack swift credentials are not passed correctly")
	} else if config.AuthType == authTypePassword {
		if len(config.AuthURL) != 0 && len(config.TenantName) != 0 && len(config.Password) != 0 && len(config.Username) != 0 && len(config.DomainName) != 0 {
			return nil
		}
	} else if config.AuthType == authTypeV3ApplicationCredential {
		if len(config.AuthURL) != 0 && len(config.TenantName) != 0 && len(config.DomainName) != 0 && len(config.ApplicationCredentialID) != 0 && len(config.ApplicationCredentialName) != 0 && len(config.ApplicationCredentialSecret) != 0 {
			return nil
		}
	}
	return fmt.Errorf("openstack swift credentials are not passed correctly")
}

func getSwiftCredentialAuthType(files []fs.DirEntry) (string, error) {

	for _, file := range files {
		switch file.Name() {
		case "password", "username":
			return authTypePassword, nil
		case "applicationCredentialSecret", "applicationCredentialID":
			return authTypeV3ApplicationCredential, nil
		}
	}
	return "", fmt.Errorf("unable to decide the authType: openstack swift credentials are not passed correctly")
}

func getSwiftCredentialAuthTypeFromJSON(appCredSecret string, password string) (string, error) {
	if len(appCredSecret) != 0 && len(password) != 0 {
		return "", fmt.Errorf("unable to decide the authType: openstack swift credentials are not passed correctly")
	} else if len(appCredSecret) != 0 {
		return authTypeV3ApplicationCredential, nil
	} else if len(password) != 0 {
		return authTypePassword, nil
	}
	return "", fmt.Errorf("unable to decide the authType: openstack swift credentials are not passed correctly")
}
