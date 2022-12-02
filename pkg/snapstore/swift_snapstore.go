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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"sort"
	"strings"
	"sync"

	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/sirupsen/logrus"

	"github.com/gophercloud/gophercloud"
	"github.com/gophercloud/gophercloud/openstack"
	"github.com/gophercloud/gophercloud/openstack/objectstorage/v1/objects"
	"github.com/gophercloud/gophercloud/pagination"
	"github.com/gophercloud/utils/openstack/clientconfig"
)

const (
	envPrefixSource         = "SOURCE_OS_"
	swiftCredentialFile     = "OPENSTACK_APPLICATION_CREDENTIALS"
	swiftCredentialJSONFile = "OPENSTACK_APPLICATION_CREDENTIALS_JSON"
)

// SwiftSnapStore is snapstore with Openstack Swift as backend
type SwiftSnapStore struct {
	prefix string
	client *gophercloud.ServiceClient
	bucket string
	// maxParallelChunkUploads hold the maximum number of parallel chunk uploads allowed.
	maxParallelChunkUploads uint
	minChunkSize            int64
	tempDir                 string
}

type swiftCredentials struct {
	AuthURL    string `json:"authURL"`
	BucketName string `json:"bucketName"`
	DomainName string `json:"domainName"`
	Password   string `json:"password"`
	Region     string `json:"region"`
	TenantName string `json:"tenantName"`
	Username   string `json:"username"`
}

const (
	// Total number of chunks to be uploaded must be one less than maximum limit allowed.
	swiftNoOfChunk int64 = 999 //Default configuration in swift installation
)

// NewSwiftSnapStore create new SwiftSnapStore from shared configuration with specified bucket
func NewSwiftSnapStore(config *brtypes.SnapstoreConfig) (*SwiftSnapStore, error) {
	clientOpts, err := getClientOpts(config.IsSource)
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

func getClientOpts(isSource bool) (*clientconfig.ClientOpts, error) {
	prefix := getEnvPrefixString(isSource)
	if filename, isSet := os.LookupEnv(prefix + swiftCredentialJSONFile); isSet {
		clientOpts, err := readSwiftCredentialsJSON(filename)
		if err != nil {
			return &clientconfig.ClientOpts{}, fmt.Errorf("error getting credentials using %v file", filename)
		}
		return clientOpts, nil
	}

	if dir, isSet := os.LookupEnv(prefix + swiftCredentialFile); isSet {
		clientOpts, err := readSwiftCredentialFiles(dir)
		if err != nil {
			return &clientconfig.ClientOpts{}, fmt.Errorf("error getting credentials from %v directory", dir)
		}
		return clientOpts, nil
	}

	// If a neither a swiftCredentialFile nor a swiftCredentialJSONFile was found, fall back to
	// retreiving credentials from environment variables.
	// If the snapstore is used as source during a copy operation all environment variables have a SOURCE_OS_ prefix.
	if isSource {
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

	os.Setenv("OS_TENANT_NAME", cred.TenantName)
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
	jsonData, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(jsonData, cred); err != nil {
		return nil, err
	}
	return cred, nil
}

func readSwiftCredentialFiles(dirname string) (*clientconfig.ClientOpts, error) {
	cred, err := readSwiftCredentialDir(dirname)
	if err != nil {
		return nil, err
	}

	os.Setenv("OS_TENANT_NAME", cred.TenantName)
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

func readSwiftCredentialDir(dirname string) (*swiftCredentials, error) {
	cred := &swiftCredentials{}
	files, err := os.ReadDir(dirname)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if file.Name() == "authURL" {
			data, err := os.ReadFile(dirname + "/authURL")
			if err != nil {
				return nil, err
			}
			cred.AuthURL = string(data)
		} else if file.Name() == "domainName" {
			data, err := os.ReadFile(dirname + "/domainName")
			if err != nil {
				return nil, err
			}
			cred.DomainName = string(data)
		} else if file.Name() == "password" {
			data, err := os.ReadFile(dirname + "/password")
			if err != nil {
				return nil, err
			}
			cred.Password = string(data)
		} else if file.Name() == "region" {
			data, err := os.ReadFile(dirname + "/region")
			if err != nil {
				return nil, err
			}
			cred.Region = string(data)
		} else if file.Name() == "tenantName" {
			data, err := os.ReadFile(dirname + "/tenantName")
			if err != nil {
				return nil, err
			}
			cred.TenantName = string(data)
		} else if file.Name() == "username" {
			data, err := os.ReadFile(dirname + "/username")
			if err != nil {
				return nil, err
			}
			cred.Username = string(data)
		}
	}

	if err := isSwiftConfigEmpty(cred); err != nil {
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

// Save will write the snapshot to store
func (s *SwiftSnapStore) Save(snap brtypes.Snapshot, rc io.ReadCloser) error {
	// Save it locally
	tmpfile, err := os.CreateTemp(s.tempDir, tmpBackupFilePrefix)
	if err != nil {
		rc.Close()
		return fmt.Errorf("failed to create snapshot tempfile: %v", err)
	}
	defer func() {
		tmpfile.Close()
		os.Remove(tmpfile.Name())
	}()
	size, err := io.Copy(tmpfile, rc)
	rc.Close()
	if err != nil {
		return fmt.Errorf("failed to save snapshot to tmpfile: %v", err)
	}

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
		go s.chunkUploader(&wg, cancelCh, &snap, tmpfile, chunkUploadCh, resCh)
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
		case chunk, ok := <-chunkUploadCh:
			if !ok {
				return
			}
			logrus.Infof("Uploading chunk with offset : %d, attempt: %d", chunk.offset, chunk.attempt)
			err := s.uploadChunk(snap, file, chunk.offset, chunk.size)
			errCh <- chunkUploadResult{
				err:   err,
				chunk: &chunk,
			}
		}
	}
}

// List will return sorted list with all snapshot files on store.
func (s *SwiftSnapStore) List() (brtypes.SnapList, error) {
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

// Delete should delete the snapshot file from store
func (s *SwiftSnapStore) Delete(snap brtypes.Snapshot) error {
	result := objects.Delete(s.client, s.bucket, path.Join(snap.Prefix, snap.SnapDir, snap.SnapName), nil)
	return result.Err
}

// SwiftSnapStoreHash calculates and returns the hash of openstack swift snapstore secret.
func SwiftSnapStoreHash(config *brtypes.SnapstoreConfig) (string, error) {
	if _, isSet := os.LookupEnv(swiftCredentialFile); isSet {
		if dir := os.Getenv(swiftCredentialFile); dir != "" {
			swiftConfig, err := readSwiftCredentialDir(dir)
			if err != nil {
				return "", fmt.Errorf("error getting credentials from %v directory", dir)
			}
			return getSwiftHash(swiftConfig), nil
		}
	}

	if _, isSet := os.LookupEnv(swiftCredentialJSONFile); isSet {
		if filename := os.Getenv(swiftCredentialJSONFile); filename != "" {
			swiftConfig, err := swiftCredentialsFromJSON(filename)
			if err != nil {
				return "", fmt.Errorf("error getting credentials using %v file", filename)
			}
			return getSwiftHash(swiftConfig), nil
		}
	}

	return "", nil
}

func getSwiftHash(config *swiftCredentials) string {
	data := fmt.Sprintf("%s%s%s%s%s", config.AuthURL, config.TenantName, config.Username, config.DomainName, config.Password)
	return getHash(data)
}

func isSwiftConfigEmpty(config *swiftCredentials) error {
	if len(config.AuthURL) != 0 && len(config.TenantName) != 0 && len(config.Password) != 0 && len(config.Username) != 0 && len(config.DomainName) != 0 {
		return nil
	}
	return fmt.Errorf("openstack swift credentials are not passed correctly")
}
