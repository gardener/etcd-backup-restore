// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	stiface "github.com/gardener/etcd-backup-restore/pkg/snapstore/gcs"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

	"cloud.google.com/go/storage"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	envStoreCredentials       = "GOOGLE_APPLICATION_CREDENTIALS" // #nosec G101 -- This is not a hardcoded password, but only the environment variable to the credentials.
	envSourceStoreCredentials = "SOURCE_GOOGLE_APPLICATION_CREDENTIALS"

	fileNameEmulatorEnabled    = "emulatorEnabled"
	fileNameStorageAPIEndpoint = "storageAPIEndpoint"
)

// GCSSnapStore is snapstore with GCS object store as backend.
type GCSSnapStore struct {
	client         stiface.Client
	prefix         string
	bucket         string
	tempDir        string
	chunkDirSuffix string
	// maxParallelChunkUploads hold the maximum number of parallel chunk uploads allowed.
	maxParallelChunkUploads uint
	minChunkSize            int64
}

// gcsEmulatorConfig holds the configuration for the fake GCS emulator
type gcsEmulatorConfig struct {
	endpoint string
	enabled  bool
}

const (
	// Total number of chunks to be uploaded must be one less than maximum limit allowed.
	gcsNoOfChunk int64 = 31
)

// NewGCSSnapStore create new GCSSnapStore from shared configuration with specified bucket.
func NewGCSSnapStore(config *brtypes.SnapstoreConfig) (*GCSSnapStore, error) {
	ctx := context.TODO()

	var opts []option.ClientOption // no need to explicitly set store credentials here since the Google SDK picks it up from the standard environment variable
	var emulatorConfig gcsEmulatorConfig
	emulatorConfig.enabled = config.IsEmulatorEnabled || isEmulatorEnabled(config)
	endpoint, err := getGCSStorageAPIEndpoint(config)
	if err != nil {
		return nil, err
	}
	if endpoint != "" {
		opts = append(opts, option.WithEndpoint(endpoint))
		if emulatorConfig.enabled {
			emulatorConfig.endpoint = endpoint
		}
	} else {
		// if emulator is enabled, but custom storage API endpoint for the emulator is not provided, throw error
		if emulatorConfig.enabled {
			return nil, fmt.Errorf("emulator enabled, but `storageAPIEndpoint` not provided")
		}
	}

	var chunkDirSuffix string
	if emulatorConfig.enabled {
		var err error
		opts, err = emulatorConfig.configureClient(opts)
		if err != nil {
			return nil, err
		}
		chunkDirSuffix = brtypes.ChunkDirSuffix
	}
	if config.IsSource && !emulatorConfig.enabled {
		filename := os.Getenv(envSourceStoreCredentials)
		if filename == "" {
			return nil, fmt.Errorf("environment variable %s is not set", envSourceStoreCredentials)
		}
		opts = append(opts, option.WithCredentialsFile(filename))
	}

	cli, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, err
	}
	gcsClient := stiface.AdaptClient(cli)

	return NewGCSSnapStoreFromClient(config.Container, config.Prefix, config.TempDir, config.MaxParallelChunkUploads, config.MinChunkSize, chunkDirSuffix, gcsClient), nil
}

func getGCSStorageAPIEndpoint(config *brtypes.SnapstoreConfig) (string, error) {
	if gcsApplicationCredentialsPath, isSet := os.LookupEnv(getEnvPrefixString(config.IsSource) + envStoreCredentials); isSet {
		storageAPIEndpointFilePath := path.Join(path.Dir(gcsApplicationCredentialsPath), fileNameStorageAPIEndpoint)
		if _, err := os.Stat(storageAPIEndpointFilePath); err != nil {
			// if the file does not exist, then there is no override for the storage API endpoint
			return "", nil
		}
		endpoint, err := os.ReadFile(storageAPIEndpointFilePath) // #nosec G304 -- this is a trusted file, obtained from mounted secret.
		if err != nil {
			return "", fmt.Errorf("error getting storage API endpoint from %v", storageAPIEndpointFilePath)
		}
		return string(endpoint), nil
	}
	return "", nil
}

func isEmulatorEnabled(config *brtypes.SnapstoreConfig) bool {
	if gcsApplicationCredentialsPath, isSet := os.LookupEnv(getEnvPrefixString(config.IsSource) + envStoreCredentials); isSet {
		emulatorEnabledFilePath := path.Join(path.Dir(gcsApplicationCredentialsPath), fileNameEmulatorEnabled)
		emulatorEnabledString, err := os.ReadFile(emulatorEnabledFilePath) // #nosec G304 -- this is a trusted file, obtained from mounted secret.
		if err != nil {
			return false
		}
		emulatorEnabled, err := strconv.ParseBool(string(emulatorEnabledString))
		if err != nil {
			return false
		}
		return emulatorEnabled
	}
	return false
}

// NewGCSSnapStoreFromClient create new GCSSnapStore from shared configuration with specified bucket.
func NewGCSSnapStoreFromClient(bucket, prefix, tempDir string, maxParallelChunkUploads uint, minChunkSize int64, chunkDirSuffix string, cli stiface.Client) *GCSSnapStore {
	return &GCSSnapStore{
		prefix:                  prefix,
		client:                  cli,
		bucket:                  bucket,
		maxParallelChunkUploads: maxParallelChunkUploads,
		minChunkSize:            minChunkSize,
		tempDir:                 tempDir,
		chunkDirSuffix:          chunkDirSuffix,
	}
}

// configureClient configures the fake gcs emulator
func (e *gcsEmulatorConfig) configureClient(opts []option.ClientOption) ([]option.ClientOption, error) {
	err := os.Setenv("STORAGE_EMULATOR_HOST", strings.TrimPrefix(e.endpoint, "http://"))
	if err != nil {
		return nil, fmt.Errorf("failed to set the environment variable for the fake GCS emulator: %v", err)
	}
	return append(opts, option.WithoutAuthentication()), nil
}

// Fetch should open reader for the snapshot file from store.
func (s *GCSSnapStore) Fetch(snap brtypes.Snapshot) (io.ReadCloser, error) {
	objectName := path.Join(snap.Prefix, snap.SnapDir, snap.SnapName)
	ctx := context.TODO()
	return s.client.Bucket(s.bucket).Object(objectName).NewReader(ctx)
}

// Save will write the snapshot to store.
func (s *GCSSnapStore) Save(snap brtypes.Snapshot, rc io.ReadCloser) (err error) {
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
		chunkSize  = int64(math.Max(float64(s.minChunkSize), float64(size/gcsNoOfChunk)))
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
		go s.componentUploader(&wg, cancelCh, &snap, tempFile, chunkUploadCh, resCh)
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
	logrus.Info("All chunk uploaded successfully. Uploading composite object.")
	bh := s.client.Bucket(s.bucket)
	var subObjects []stiface.ObjectHandle
	prefix := adaptPrefix(&snap, s.prefix)
	for partNumber := int64(1); partNumber <= noOfChunks; partNumber++ {
		name := path.Join(prefix, snap.SnapDir, fmt.Sprintf("%s%s", snap.SnapName, s.chunkDirSuffix), fmt.Sprintf("%010d", partNumber))
		obj := bh.Object(name)
		subObjects = append(subObjects, obj)
	}
	name := path.Join(prefix, snap.SnapDir, snap.SnapName)
	obj := bh.Object(name)
	c := obj.ComposerFrom(subObjects...)
	ctx, cancel := context.WithTimeout(context.TODO(), chunkUploadTimeout)
	defer cancel()
	if _, err := c.Run(ctx); err != nil {
		return fmt.Errorf("failed uploading composite object for snapshot with error: %v", err)
	}
	logrus.Info("Composite object uploaded successfully.")
	return nil
}

func (s *GCSSnapStore) uploadComponent(snap *brtypes.Snapshot, file *os.File, offset, chunkSize int64) error {
	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}
	size := fileInfo.Size() - offset
	if size > chunkSize {
		size = chunkSize
	}

	sr := io.NewSectionReader(file, offset, size)
	bh := s.client.Bucket(s.bucket)
	partNumber := ((offset / chunkSize) + 1)
	name := path.Join(adaptPrefix(snap, s.prefix), snap.SnapDir, fmt.Sprintf("%s%s", snap.SnapName, s.chunkDirSuffix), fmt.Sprintf("%010d", partNumber))
	obj := bh.Object(name)
	ctx, cancel := context.WithTimeout(context.TODO(), chunkUploadTimeout)
	defer cancel()
	w := obj.NewWriter(ctx)
	if _, err := io.Copy(w, sr); err != nil {
		if err1 := w.Close(); err1 != nil {
			return errors.Join(err, err1)
		}
		return err
	}
	return w.Close()
}

func (s *GCSSnapStore) componentUploader(wg *sync.WaitGroup, stopCh <-chan struct{}, snap *brtypes.Snapshot, file *os.File, chunkUploadCh chan chunk, errCh chan<- chunkUploadResult) {
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
			err := s.uploadComponent(snap, file, uploadChunk.offset, uploadChunk.size)
			errCh <- chunkUploadResult{
				err:   err,
				chunk: &uploadChunk,
			}
		}
	}
}

// List returns a sorted list of all snapshot files in the object store, excluding those snapshots tagged with `x-ignore-etcd-snapshot-exclude` in their object metadata/tags. To include these tagged snapshots in the List output, pass `true` as the argument.
func (s *GCSSnapStore) List(includeAll bool) (brtypes.SnapList, error) {
	prefixTokens := strings.Split(s.prefix, "/")
	// Consider the parent of the last element for backward compatibility.
	prefix := path.Join(strings.Join(prefixTokens[:len(prefixTokens)-1], "/"))

	it := s.client.Bucket(s.bucket).Objects(context.TODO(), &storage.Query{
		Prefix: prefix,
	})

	var attrs []*storage.ObjectAttrs
	for {
		attr, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		// Check if the snapshot should be ignored
		if !includeAll && attr.Metadata[brtypes.ExcludeSnapshotMetadataKey] == "true" {
			logrus.Infof("Ignoring snapshot %s due to the exclude tag %q in the snapshot metadata", brtypes.ExcludeSnapshotMetadataKey, attr.Name)
			continue
		}
		attrs = append(attrs, attr)
	}

	var snapList brtypes.SnapList
	for _, v := range attrs {
		if strings.Contains(v.Name, backupVersionV1) || strings.Contains(v.Name, backupVersionV2) {
			snap, err := ParseSnapshot(v.Name)
			if err != nil {
				logrus.Warnf("Invalid snapshot %s found, ignoring it: %v", v.Name, err)
				continue
			}
			snap.ImmutabilityExpiryTime = v.RetentionExpirationTime
			snapList = append(snapList, snap)
		}
	}

	sort.Sort(snapList)
	return snapList, nil
}

// Delete should delete the snapshot file from store.
func (s *GCSSnapStore) Delete(snap brtypes.Snapshot) error {
	objectName := path.Join(snap.Prefix, snap.SnapDir, snap.SnapName)
	return s.client.Bucket(s.bucket).Object(objectName).Delete(context.TODO())
}

// GetGCSCredentialsLastModifiedTime returns the latest modification timestamp of the GCS credential file
func GetGCSCredentialsLastModifiedTime() (time.Time, error) {
	if credentialsFilePath, isSet := os.LookupEnv(envStoreCredentials); isSet {
		credentialFiles := []string{credentialsFilePath}
		gcsTimeStamp, err := getLatestCredentialsModifiedTime(credentialFiles)
		if err != nil {
			return time.Time{}, fmt.Errorf("failed to fetch file information of the GCS JSON credential dir %v with error: %v", path.Dir(credentialsFilePath), err)
		}
		return gcsTimeStamp, nil
	}

	return time.Time{}, fmt.Errorf("no environment variable set for the GCS credential file")
}
