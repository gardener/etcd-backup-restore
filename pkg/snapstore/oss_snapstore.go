// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/snapstore/internal/ossApi"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/aws/smithy-go/ptr"
	"github.com/sirupsen/logrus"
)

const (
	// Total number of chunks to be uploaded must be one less than maximum limit allowed.
	ossNoOfChunk           int64 = 9999
	aliCredentialDirectory       = "ALICLOUD_APPLICATION_CREDENTIALS"      // #nosec G101 -- This is not a hardcoded password, but only a path to the credentials.
	aliCredentialJSONFile        = "ALICLOUD_APPLICATION_CREDENTIALS_JSON" // #nosec G101 -- This is not a hardcoded password, but only a path to the credentials.
)

type authOptions struct {
	Endpoint   string `json:"storageEndpoint"`
	AccessID   string `json:"accessKeyID"`
	AccessKey  string `json:"accessKeySecret"`
	BucketName string `json:"bucketName"`
}

// OSSSnapStore is snapstore with Alicloud OSS object store as backend
type OSSSnapStore struct {
	bucket                  ossApi.OSSBucket
	client                  ossApi.Client
	bucketName              string
	prefix                  string
	tempDir                 string
	maxParallelChunkUploads uint
	minChunkSize            int64
}

// NewOSSSnapStore create new OSSSnapStore from shared configuration with specified bucket
func NewOSSSnapStore(config *brtypes.SnapstoreConfig) (*OSSSnapStore, error) {
	ao, err := getAuthOptions(getEnvPrefixString(config.IsSource))
	if err != nil {
		return nil, err
	}
	return newOSSFromAuthOpt(config.Container, config.Prefix, config.TempDir, config.MaxParallelChunkUploads, config.MinChunkSize, *ao)
}

func newOSSFromAuthOpt(bucket, prefix, tempDir string, maxParallelChunkUploads uint, minChunkSize int64, ao authOptions) (*OSSSnapStore, error) {
	client, err := oss.New(ao.Endpoint, ao.AccessID, ao.AccessKey)
	if err != nil {
		return nil, err
	}

	bucketOSS, err := client.Bucket(bucket)
	if err != nil {
		return nil, err
	}

	return NewOSSFromBucket(prefix, tempDir, bucket, maxParallelChunkUploads, minChunkSize, client, bucketOSS), nil
}

// NewOSSFromBucket will create the new OSS snapstore object from OSS bucket
func NewOSSFromBucket(prefix, tempDir, bucketName string, maxParallelChunkUploads uint, minChunkSize int64, client ossApi.Client, bucket ossApi.OSSBucket) *OSSSnapStore {
	return &OSSSnapStore{
		prefix:                  prefix,
		bucket:                  bucket,
		client:                  client,
		bucketName:              bucketName,
		maxParallelChunkUploads: maxParallelChunkUploads,
		minChunkSize:            minChunkSize,
		tempDir:                 tempDir,
	}
}

// Fetch should open reader for the snapshot file from store
func (s *OSSSnapStore) Fetch(snap brtypes.Snapshot) (io.ReadCloser, error) {
	body, err := s.bucket.GetObject(path.Join(snap.Prefix, snap.SnapDir, snap.SnapName))
	if err != nil {
		return nil, err
	}
	return body, nil
}

// Save will write the snapshot to store
func (s *OSSSnapStore) Save(snap brtypes.Snapshot, rc io.ReadCloser) (err error) {
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

	_, err = tempFile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	var (
		chunkSize  = int64(math.Max(float64(s.minChunkSize), float64(size/ossNoOfChunk)))
		noOfChunks = size / chunkSize
	)
	if size%chunkSize != 0 {
		noOfChunks++
	}

	ossChunks, err := oss.SplitFileByPartNum(tempFile.Name(), int(noOfChunks))
	if err != nil {
		return err
	}

	imur, err := s.bucket.InitiateMultipartUpload(path.Join(adaptPrefix(&snap, s.prefix), snap.SnapDir, snap.SnapName))
	if err != nil {
		return err
	}

	var (
		completedParts = make([]oss.UploadPart, noOfChunks)
		chunkUploadCh  = make(chan chunk, noOfChunks)
		resCh          = make(chan chunkUploadResult, noOfChunks)
		cancelCh       = make(chan struct{})
		wg             sync.WaitGroup
	)

	for i := uint(0); i < s.maxParallelChunkUploads; i++ {
		wg.Add(1)
		go s.partUploader(&wg, imur, tempFile, completedParts, chunkUploadCh, cancelCh, resCh)
	}

	for _, ossChunk := range ossChunks {
		c := chunk{
			offset: ossChunk.Offset,
			size:   ossChunk.Size,
			id:     ossChunk.Number,
		}
		logrus.Debugf("Triggering chunk upload for offset: %d", c.offset)
		chunkUploadCh <- c
	}

	logrus.Infof("Triggered chunk upload for all chunks, total: %d", noOfChunks)
	snapshotErr := collectChunkUploadError(chunkUploadCh, resCh, cancelCh, noOfChunks)
	wg.Wait()

	if snapshotErr == nil {
		_, err := s.bucket.CompleteMultipartUpload(imur, completedParts)
		if err != nil {
			return err
		}
		logrus.Infof("Finishing the multipart upload with upload ID : %s", imur.UploadID)
	} else {
		logrus.Infof("Aborting the multipart upload with upload ID : %s", imur.UploadID)
		err := s.bucket.AbortMultipartUpload(imur)
		if err != nil {
			return snapshotErr.err
		}
	}

	return nil
}

func (s *OSSSnapStore) partUploader(wg *sync.WaitGroup, imur oss.InitiateMultipartUploadResult, file *os.File, completedParts []oss.UploadPart, chunkUploadCh <-chan chunk, stopCh <-chan struct{}, errCh chan<- chunkUploadResult) {
	defer wg.Done()
	for {
		select {
		case <-stopCh:
			return
		case uploadChunk, ok := <-chunkUploadCh:
			if !ok {
				return
			}
			logrus.Infof("Uploading chunk with id: %d, offset: %d, size: %d", uploadChunk.id, uploadChunk.offset, uploadChunk.size)
			err := s.uploadPart(imur, file, completedParts, uploadChunk.offset, uploadChunk.size, uploadChunk.id)
			errCh <- chunkUploadResult{
				err:   err,
				chunk: &uploadChunk,
			}
		}
	}
}

func (s *OSSSnapStore) uploadPart(imur oss.InitiateMultipartUploadResult, file *os.File, completedParts []oss.UploadPart, offset, chunkSize int64, number int) error {
	fd := io.NewSectionReader(file, offset, chunkSize)
	part, err := s.bucket.UploadPart(imur, fd, chunkSize, number)

	if err == nil {
		completedParts[number-1] = part
	}
	return err
}

// List will return sorted list with all snapshot files on store.
func (s *OSSSnapStore) List(_ bool) (brtypes.SnapList, error) {
	prefixTokens := strings.Split(s.prefix, "/")
	// Last element of the tokens is backup version
	// Consider the parent of the backup version level (Required for Backward Compatibility)
	prefix := path.Join(strings.Join(prefixTokens[:len(prefixTokens)-1], "/"))

	var snapList brtypes.SnapList
	var bucketImmutableExpiryTimeInDays *int

	config, err := s.client.GetBucketWorm(s.bucketName)
	if err != nil {
		logrus.Warnf("unable to get worm configuration for bukcet: %v", err)
	} else if config.State == "InProgress" || config.State == "Locked" {
		bucketImmutableExpiryTimeInDays = ptr.Int(config.RetentionPeriodInDays)
	}

	marker := ""
	for {
		lsRes, err := s.bucket.ListObjects(oss.Marker(marker), oss.Prefix(prefix))
		if err != nil {
			return nil, err
		}
		for _, object := range lsRes.Objects {
			if strings.Contains(object.Key, backupVersionV1) || strings.Contains(object.Key, backupVersionV2) {
				snap, err := ParseSnapshot(object.Key)
				if err != nil {
					// Warning
					logrus.Warnf("Invalid snapshot found. Ignoring it: %s", object.Key)
				} else {
					if bucketImmutableExpiryTimeInDays != nil {
						// To get OSS object's "ImmutabilityExpiryTime", backup-restore is calculating the "ImmutabilityExpiryTime" using bucket retention period.
						// ImmutabilityExpiryTime = SnapshotCreationTime + bucketImmutabilityTimeInDays
						snap.ImmutabilityExpiryTime = snap.CreatedOn.Add(time.Duration(*bucketImmutableExpiryTimeInDays) * 24 * time.Hour)
					}
					snapList = append(snapList, snap)
				}
			}
		}
		if !lsRes.IsTruncated {
			break
		}
		marker = lsRes.NextMarker
	}
	sort.Sort(snapList)

	return snapList, nil
}

// Delete should delete the snapshot file from store
func (s *OSSSnapStore) Delete(snap brtypes.Snapshot) error {
	return s.bucket.DeleteObject(path.Join(snap.Prefix, snap.SnapDir, snap.SnapName))
}

func getAuthOptions(prefix string) (*authOptions, error) {
	if filename, isSet := os.LookupEnv(prefix + aliCredentialJSONFile); isSet {
		ao, err := readALICredentialsJSON(filename)
		if err != nil {
			return nil, fmt.Errorf("error getting credentials using %v file with error: %w", filename, err)
		}
		return ao, nil
	}

	// TODO: @renormalize Remove this extra handling in v0.31.0
	// Check if a JSON file is present in the directory, if it is present -> the JSON file must be used for credentials.
	if dir, isSet := os.LookupEnv(prefix + aliCredentialDirectory); isSet {
		jsonCredentialFile, err := findFileWithExtensionInDir(dir, ".json")
		if err != nil {
			return nil, fmt.Errorf("error while finding a JSON credential file in %v directory with error: %w", dir, err)
		}
		if jsonCredentialFile != "" {
			ao, err := readALICredentialsJSON(jsonCredentialFile)
			if err != nil {
				return nil, fmt.Errorf("error getting credentials using %v JSON file in a directory with error: %w", jsonCredentialFile, err)
			}
			return ao, nil
		}
		// Non JSON credential files might exist in the credential directory, do not return
	}

	if dir, isSet := os.LookupEnv(prefix + aliCredentialDirectory); isSet {
		ao, err := readALICredentialFiles(dir)
		if err != nil {
			return nil, fmt.Errorf("error getting credentials from %v directory with error: %w", dir, err)
		}
		return ao, nil
	}

	return nil, fmt.Errorf("unable to get credentials")
}

func readALICredentialsJSON(filename string) (*authOptions, error) {
	jsonData, err := os.ReadFile(filename) // #nosec G304 -- this is a trusted file, obtained via user input.
	if err != nil {
		return nil, err
	}

	return aliCredentialsFromJSON(jsonData)
}

// aliCredentialsFromJSON obtains AliCloud OSS credentials from a JSON value.
func aliCredentialsFromJSON(jsonData []byte) (*authOptions, error) {
	aliConfig := &authOptions{}
	if err := json.Unmarshal(jsonData, aliConfig); err != nil {
		return nil, err
	}

	return aliConfig, nil
}

func readALICredentialFiles(dirname string) (*authOptions, error) {
	aliConfig := &authOptions{}

	files, err := os.ReadDir(dirname)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if file.Name() == "storageEndpoint" {
			data, err := os.ReadFile(dirname + "/storageEndpoint") // #nosec G304 -- this is a trusted file, obtained via user input.
			if err != nil {
				return nil, err
			}
			aliConfig.Endpoint = string(data)
		} else if file.Name() == "accessKeySecret" {
			data, err := os.ReadFile(dirname + "/accessKeySecret") // #nosec G304 -- this is a trusted file, obtained via user input.
			if err != nil {
				return nil, err
			}
			aliConfig.AccessKey = string(data)
		} else if file.Name() == "accessKeyID" {
			data, err := os.ReadFile(dirname + "/accessKeyID") // #nosec G304 -- this is a trusted file, obtained via user input.
			if err != nil {
				return nil, err
			}
			aliConfig.AccessID = string(data)
		}
	}

	if err := isOSSConfigEmpty(aliConfig); err != nil {
		return nil, err
	}
	return aliConfig, nil
}

// GetOSSCredentialsLastModifiedTime returns the latest modification timestamp of the OSS credential file(s)
func GetOSSCredentialsLastModifiedTime() (time.Time, error) {
	// TODO: @renormalize Remove this extra handling in v0.31.0
	// Check if a JSON file is present in the directory, if it is present -> the JSON file must be used for credentials.
	if dir, isSet := os.LookupEnv(aliCredentialDirectory); isSet {
		modificationTimeStamp, err := getJSONCredentialModifiedTime(dir)
		if err != nil {
			return time.Time{}, fmt.Errorf("failed to fetch credential modification time for OSS with error: %w", err)
		}
		if !modificationTimeStamp.IsZero() {
			return modificationTimeStamp, nil
		}
		// Non JSON credential files might exist in the credential directory, do not return
	}

	if dir, isSet := os.LookupEnv(aliCredentialDirectory); isSet {
		// credential files which are essential for creating the OSS snapstore
		credentialFiles := []string{"accessKeyID", "accessKeySecret", "storageEndpoint"}
		for i := range credentialFiles {
			credentialFiles[i] = filepath.Join(dir, credentialFiles[i])
		}
		aliTimeStamp, err := getLatestCredentialsModifiedTime(credentialFiles)
		if err != nil {
			return time.Time{}, fmt.Errorf("failed to get OSS credential timestamp from the directory %v with error: %v", dir, err)
		}
		return aliTimeStamp, nil
	}

	if filename, isSet := os.LookupEnv(aliCredentialJSONFile); isSet {
		credentialFiles := []string{filename}
		aliTimeStamp, err := getLatestCredentialsModifiedTime(credentialFiles)
		if err != nil {
			return time.Time{}, fmt.Errorf("failed to fetch file information of the OSS JSON credential file %v with error: %v", filename, err)
		}
		return aliTimeStamp, nil
	}

	return time.Time{}, fmt.Errorf("no environment variable set for the OSS credential file")
}

func isOSSConfigEmpty(config *authOptions) error {
	if len(config.AccessID) != 0 && len(config.AccessKey) != 0 && len(config.Endpoint) != 0 {
		return nil
	}
	return fmt.Errorf("aliCloud OSS credentials: accessKeyID, accessKeySecret or storageEndpoint is missing")
}
