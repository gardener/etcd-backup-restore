// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore

import (
	"context"
	"crypto/md5" // #nosec G501 -- S3 API supports only MD5 hash for SSE headers
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/sirupsen/logrus"
	"k8s.io/utils/ptr"

	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
)

const (
	// Total number of chunks to be uploaded must be one less than maximum limit allowed.
	s3NoOfChunk            int64 = 9999
	awsCredentialDirectory       = "AWS_APPLICATION_CREDENTIALS"
	awsCredentialJSONFile        = "AWS_APPLICATION_CREDENTIALS_JSON"
)

type awsCredentials struct {
	AccessKeyID          string  `json:"accessKeyID"`
	Region               string  `json:"region"`
	SecretAccessKey      string  `json:"secretAccessKey"`
	SSECustomerAlgorithm *string `json:"sseCustomerAlgorithm,omitempty"`
	SSECustomerKey       *string `json:"sseCustomerKey,omitempty"`
	BucketName           string  `json:"bucketName"`
	Endpoint             *string `json:"endpoint,omitempty"`
	S3ForcePathStyle     *bool   `json:"s3ForcePathStyle,omitempty"`
	InsecureSkipVerify   *bool   `json:"insecureSkipVerify,omitempty"`
	TrustedCaCert        *string `json:"trustedCaCert,omitempty"`
}

// SSECredentials to hold fields for server-side encryption in I/O operations
type SSECredentials struct {
	sseCustomerAlgorithm string
	sseCustomerKey       string
	sseCustomerKeyMD5    string
}

// S3SnapStore is snapstore with AWS S3 object store as backend
type S3SnapStore struct {
	prefix string
	client s3iface.S3API
	bucket string
	// maxParallelChunkUploads hold the maximum number of parallel chunk uploads allowed.
	maxParallelChunkUploads uint
	minChunkSize            int64
	tempDir                 string
	SSECredentials
}

// NewS3SnapStore create new S3SnapStore from shared configuration with specified bucket
func NewS3SnapStore(config *brtypes.SnapstoreConfig) (*S3SnapStore, error) {
	sessionOpts, sseCreds, err := getSessionOptions(getEnvPrefixString(config.IsSource))
	if err != nil {
		return nil, err
	}
	sess, err := session.NewSessionWithOptions(sessionOpts)
	if err != nil {
		return nil, fmt.Errorf("new AWS session failed: %v", err)
	}
	cli := s3.New(sess)
	return NewS3FromClient(config.Container, config.Prefix, config.TempDir, config.MaxParallelChunkUploads, config.MinChunkSize, cli, sseCreds), nil
}

func getSessionOptions(prefixString string) (session.Options, SSECredentials, error) {
	if filename, isSet := os.LookupEnv(prefixString + awsCredentialJSONFile); isSet {
		creds, sseCreds, err := readAWSCredentialsJSONFile(filename)
		if err != nil {
			return session.Options{}, SSECredentials{}, fmt.Errorf("error getting credentials using %v file with error %w", filename, err)
		}
		return creds, sseCreds, nil
	}

	// TODO: @renormalize Remove this extra handling in v0.31.0
	// Check if a JSON file is present in the directory, if it is present -> the JSON file must be used for credentials.
	if dir, isSet := os.LookupEnv(prefixString + awsCredentialDirectory); isSet {
		jsonCredentialFile, err := findFileWithExtensionInDir(dir, ".json")
		if err != nil {
			return session.Options{}, SSECredentials{}, fmt.Errorf("error while finding a JSON credential file in %v directory with error: %w", dir, err)
		}
		if jsonCredentialFile != "" {
			creds, sseCreds, err := readAWSCredentialsJSONFile(jsonCredentialFile)
			if err != nil {
				return session.Options{}, SSECredentials{}, fmt.Errorf("error getting credentials using %v JSON file in a directory with error: %w", jsonCredentialFile, err)
			}
			return creds, sseCreds, nil
		}
		// Non JSON credential files might exist in the credential directory, do not return
	}

	if dir, isSet := os.LookupEnv(prefixString + awsCredentialDirectory); isSet {
		creds, sseCreds, err := readAWSCredentialFiles(dir)
		if err != nil {
			return session.Options{}, SSECredentials{}, fmt.Errorf("error getting credentials from %v directory with error %w", dir, err)
		}
		return creds, sseCreds, nil
	}

	return session.Options{
		// Setting this is equal to the AWS_SDK_LOAD_CONFIG environment variable was set.
		// We want to save the work to set AWS_SDK_LOAD_CONFIG=1 outside.
		SharedConfigState: session.SharedConfigEnable,
	}, SSECredentials{}, nil
}

func readAWSCredentialsJSONFile(filename string) (session.Options, SSECredentials, error) {
	awsConfig, err := credentialsFromJSON(filename)
	if err != nil {
		return session.Options{}, SSECredentials{}, err
	}

	httpClient := http.DefaultClient
	if awsConfig.InsecureSkipVerify != nil && *awsConfig.InsecureSkipVerify {
		httpClient.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: *awsConfig.InsecureSkipVerify}, // #nosec G402 -- InsecureSkipVerify is set by user input, and can be allowed to be set to true based on user's requirement.
		}
	}

	if awsConfig.TrustedCaCert != nil {
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM([]byte(*awsConfig.TrustedCaCert))
		httpClient.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:            caCertPool,
				InsecureSkipVerify: false,
				MinVersion:         tls.VersionTLS13,
			},
		}
	}

	sseCreds, err := getSSECreds(awsConfig.SSECustomerKey, awsConfig.SSECustomerAlgorithm)
	if err != nil {
		return session.Options{}, SSECredentials{}, err
	}

	return session.Options{
		Config: aws.Config{
			Credentials:      credentials.NewStaticCredentials(awsConfig.AccessKeyID, awsConfig.SecretAccessKey, ""),
			Region:           ptr.To(awsConfig.Region),
			Endpoint:         awsConfig.Endpoint,
			S3ForcePathStyle: awsConfig.S3ForcePathStyle,
			HTTPClient:       httpClient,
		},
	}, sseCreds, nil
}

// credentialsFromJSON obtains AWS credentials from a JSON value.
func credentialsFromJSON(filename string) (*awsCredentials, error) {
	jsonData, err := os.ReadFile(filename) // #nosec G304 -- this is a trusted file, obtained via user input.
	if err != nil {
		return nil, err
	}
	awsConfig := &awsCredentials{}
	if err := json.Unmarshal(jsonData, awsConfig); err != nil {
		return nil, err
	}
	return awsConfig, nil
}

func readAWSCredentialFiles(dirname string) (session.Options, SSECredentials, error) {
	awsConfig, err := readAWSCredentialFromDir(dirname)
	if err != nil {
		return session.Options{}, SSECredentials{}, err
	}

	httpClient := http.DefaultClient
	if awsConfig.InsecureSkipVerify != nil && *awsConfig.InsecureSkipVerify {
		httpClient.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: *awsConfig.InsecureSkipVerify}, // #nosec G402 -- InsecureSkipVerify is set by user input, and can be allowed to be set to true based on user's requirement.
		}
	}

	if awsConfig.TrustedCaCert != nil {
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM([]byte(*awsConfig.TrustedCaCert))
		httpClient.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:            caCertPool,
				InsecureSkipVerify: false,
				MinVersion:         tls.VersionTLS13,
			},
		}
	}
	sseCreds, err := getSSECreds(awsConfig.SSECustomerKey, awsConfig.SSECustomerAlgorithm)
	if err != nil {
		return session.Options{}, SSECredentials{}, err
	}

	return session.Options{
		Config: aws.Config{
			Credentials:      credentials.NewStaticCredentials(awsConfig.AccessKeyID, awsConfig.SecretAccessKey, ""),
			Region:           ptr.To(awsConfig.Region),
			Endpoint:         awsConfig.Endpoint,
			S3ForcePathStyle: awsConfig.S3ForcePathStyle,
			HTTPClient:       httpClient,
		},
	}, sseCreds, nil
}

func readAWSCredentialFromDir(dirname string) (*awsCredentials, error) {
	awsConfig := &awsCredentials{}

	files, err := os.ReadDir(dirname)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		switch file.Name() {
		case "accessKeyID":
			data, err := os.ReadFile(dirname + "/accessKeyID") // #nosec G304 -- this is a trusted file, obtained via user input.
			if err != nil {
				return nil, err
			}
			awsConfig.AccessKeyID = string(data)
		case "region":
			data, err := os.ReadFile(dirname + "/region") // #nosec G304 -- this is a trusted file, obtained via user input.
			if err != nil {
				return nil, err
			}
			awsConfig.Region = string(data)
		case "secretAccessKey":
			data, err := os.ReadFile(dirname + "/secretAccessKey") // #nosec G304 -- this is a trusted file, obtained via user input.
			if err != nil {
				return nil, err
			}
			awsConfig.SecretAccessKey = string(data)
		case "endpoint":
			data, err := os.ReadFile(dirname + "/endpoint") // #nosec G304 -- this is a trusted file, obtained via user input.
			if err != nil {
				return nil, err
			}
			awsConfig.Endpoint = ptr.To(string(data))
		case "s3ForcePathStyle":
			data, err := os.ReadFile(dirname + "/s3ForcePathStyle") // #nosec G304 -- this is a trusted file, obtained via user input.
			if err != nil {
				return nil, err
			}
			val, err := strconv.ParseBool(string(data))
			if err != nil {
				return nil, err
			}
			awsConfig.S3ForcePathStyle = &val
		case "insecureSkipVerify":
			data, err := os.ReadFile(dirname + "/insecureSkipVerify") // #nosec G304 -- this is a trusted file, obtained via user input.
			if err != nil {
				return nil, err
			}
			val, err := strconv.ParseBool(string(data))
			if err != nil {
				return nil, err
			}
			awsConfig.InsecureSkipVerify = &val
		case "trustedCaCert":
			data, err := os.ReadFile(dirname + "/trustedCaCert") // #nosec G304 -- this is a trusted file, obtained via user input.
			if err != nil {
				return nil, err
			}
			awsConfig.TrustedCaCert = ptr.To(string(data))
		case "sseCustomerKey":
			data, err := os.ReadFile(dirname + "/sseCustomerKey") // #nosec G304 -- this is a trusted file, obtained via user input.
			if err != nil {
				return nil, err
			}
			awsConfig.SSECustomerKey = ptr.To(string(data))
		case "sseCustomerAlgorithm":
			data, err := os.ReadFile(dirname + "/sseCustomerAlgorithm") // #nosec G304 -- this is a trusted file, obtained via user input.
			if err != nil {
				return nil, err
			}
			awsConfig.SSECustomerAlgorithm = ptr.To(string(data))
		}
	}

	if err := isAWSConfigEmpty(awsConfig); err != nil {
		return nil, err
	}
	return awsConfig, nil
}

// NewS3FromClient will create the new S3 snapstore object from S3 client
func NewS3FromClient(bucket, prefix, tempDir string, maxParallelChunkUploads uint, minChunkSize int64, cli s3iface.S3API, sseCreds SSECredentials) *S3SnapStore {
	return &S3SnapStore{
		bucket:                  bucket,
		prefix:                  prefix,
		client:                  cli,
		maxParallelChunkUploads: maxParallelChunkUploads,
		minChunkSize:            minChunkSize,
		tempDir:                 tempDir,
		SSECredentials:          sseCreds,
	}
}

// Fetch should open reader for the snapshot file from store
func (s *S3SnapStore) Fetch(snap brtypes.Snapshot) (io.ReadCloser, error) {
	getObjectInput := &s3.GetObjectInput{}
	if len(snap.VersionID) > 0 {
		getObjectInput = &s3.GetObjectInput{
			Bucket:    aws.String(s.bucket),
			Key:       aws.String(path.Join(snap.Prefix, snap.SnapDir, snap.SnapName)),
			VersionId: aws.String(snap.VersionID),
		}
	} else {
		getObjectInput = &s3.GetObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(path.Join(snap.Prefix, snap.SnapDir, snap.SnapName)),
		}
	}
	if s.sseCustomerKey != "" {
		// Customer managed Server Side Encryption
		getObjectInput.SSECustomerAlgorithm = aws.String(s.sseCustomerAlgorithm)
		getObjectInput.SSECustomerKey = aws.String(s.sseCustomerKey)
		getObjectInput.SSECustomerKeyMD5 = aws.String(s.sseCustomerKeyMD5)
	}
	getObjecOutput, err := s.client.GetObject(getObjectInput)
	if err != nil {
		return nil, fmt.Errorf("error while accessing %s: %v", path.Join(snap.Prefix, snap.SnapDir, snap.SnapName), err)
	}
	return getObjecOutput.Body, nil
}

// Save will write the snapshot to store
func (s *S3SnapStore) Save(snap brtypes.Snapshot, rc io.ReadCloser) (err error) {
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
	// Initiate multi part upload
	ctx := context.TODO()
	ctx, cancel := context.WithTimeout(ctx, chunkUploadTimeout)
	defer cancel()
	prefix := adaptPrefix(&snap, s.prefix)

	createMultipartUploadInput := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path.Join(prefix, snap.SnapDir, snap.SnapName)),
	}
	if s.sseCustomerKey != "" {
		// Customer managed Server Side Encryption
		createMultipartUploadInput.SSECustomerAlgorithm = aws.String(s.sseCustomerAlgorithm)
		createMultipartUploadInput.SSECustomerKey = aws.String(s.sseCustomerKey)
		createMultipartUploadInput.SSECustomerKeyMD5 = aws.String(s.sseCustomerKeyMD5)
	}
	uploadOutput, err := s.client.CreateMultipartUploadWithContext(ctx, createMultipartUploadInput)
	if err != nil {
		return fmt.Errorf("failed to initiate multipart upload %v", err)
	}
	logrus.Infof("Successfully initiated the multipart upload with upload ID : %s", *uploadOutput.UploadId)

	var (
		chunkSize  = int64(math.Max(float64(s.minChunkSize), float64(size/s3NoOfChunk)))
		noOfChunks = size / chunkSize
	)
	if size%chunkSize != 0 {
		noOfChunks++
	}
	var (
		completedParts = make([]*s3.CompletedPart, noOfChunks)
		chunkUploadCh  = make(chan chunk, noOfChunks)
		resCh          = make(chan chunkUploadResult, noOfChunks)
		wg             sync.WaitGroup
		cancelCh       = make(chan struct{})
	)

	for i := uint(0); i < s.maxParallelChunkUploads; i++ {
		wg.Add(1)
		go s.partUploader(&wg, cancelCh, &snap, tempFile, uploadOutput.UploadId, completedParts, chunkUploadCh, resCh)
	}
	logrus.Infof("Uploading snapshot of size: %d, chunkSize: %d, noOfChunks: %d", size, chunkSize, noOfChunks)

	for offset, index := int64(0), 1; offset < size; offset += int64(chunkSize) {
		newChunk := chunk{
			id:     index,
			offset: offset,
			size:   chunkSize,
		}
		logrus.Debugf("Triggering chunk upload for offset: %d", offset)
		chunkUploadCh <- newChunk
		index++
	}
	logrus.Infof("Triggered chunk upload for all chunks, total: %d", noOfChunks)
	snapshotErr := collectChunkUploadError(chunkUploadCh, resCh, cancelCh, noOfChunks)
	wg.Wait()

	if snapshotErr != nil {
		ctx := context.TODO()
		ctx, cancel := context.WithTimeout(ctx, chunkUploadTimeout)
		defer cancel()
		logrus.Infof("Aborting the multipart upload with upload ID : %s", *uploadOutput.UploadId)
		_, err = s.client.AbortMultipartUploadWithContext(ctx, &s3.AbortMultipartUploadInput{
			Bucket:   &s.bucket,
			Key:      aws.String(path.Join(prefix, snap.SnapDir, snap.SnapName)),
			UploadId: uploadOutput.UploadId,
		})
	} else {
		ctx = context.TODO()
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		logrus.Infof("Finishing the multipart upload with upload ID : %s", *uploadOutput.UploadId)
		_, err = s.client.CompleteMultipartUploadWithContext(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:   &s.bucket,
			Key:      aws.String(path.Join(prefix, snap.SnapDir, snap.SnapName)),
			UploadId: uploadOutput.UploadId,
			MultipartUpload: &s3.CompletedMultipartUpload{
				Parts: completedParts,
			},
		})
	}

	if err != nil {
		return fmt.Errorf("failed completing snapshot upload with error %v", err)
	}
	if snapshotErr != nil {
		return fmt.Errorf("failed uploading chunk, id: %d, offset: %d, error: %v", snapshotErr.chunk.id, snapshotErr.chunk.offset, snapshotErr.err)
	}
	return nil
}

func (s *S3SnapStore) uploadPart(snap *brtypes.Snapshot, file *os.File, uploadID *string, completedParts []*s3.CompletedPart, offset, chunkSize int64) error {
	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}
	size := fileInfo.Size() - offset
	if size > chunkSize {
		size = chunkSize
	}

	sr := io.NewSectionReader(file, offset, size)
	ctx, cancel := context.WithTimeout(context.TODO(), chunkUploadTimeout)
	defer cancel()
	partNumber := ((offset / chunkSize) + 1)

	uploadPartInput := &s3.UploadPartInput{
		Bucket:     aws.String(s.bucket),
		Key:        aws.String(path.Join(adaptPrefix(snap, s.prefix), snap.SnapDir, snap.SnapName)),
		PartNumber: &partNumber,
		UploadId:   uploadID,
		Body:       sr,
	}

	if s.sseCustomerKey != "" {
		// Customer managed Server Side Encryption
		uploadPartInput.SSECustomerAlgorithm = aws.String(s.sseCustomerAlgorithm)
		uploadPartInput.SSECustomerKey = aws.String(s.sseCustomerKey)
		uploadPartInput.SSECustomerKeyMD5 = aws.String(s.sseCustomerKeyMD5)
	}

	uploadPartOutput, err := s.client.UploadPartWithContext(ctx, uploadPartInput)
	if err == nil {
		completedPart := &s3.CompletedPart{
			ETag:       uploadPartOutput.ETag,
			PartNumber: &partNumber,
		}
		completedParts[partNumber-1] = completedPart
	}
	return err
}

func (s *S3SnapStore) partUploader(wg *sync.WaitGroup, stopCh <-chan struct{}, snap *brtypes.Snapshot, file *os.File, uploadID *string, completedParts []*s3.CompletedPart, chunkUploadCh <-chan chunk, errCh chan<- chunkUploadResult) {
	defer wg.Done()
	for {
		select {
		case <-stopCh:
			return
		case chunk, ok := <-chunkUploadCh:
			if !ok {
				return
			}
			logrus.Infof("Uploading chunk with id: %d, offset: %d, attempt: %d", chunk.id, chunk.offset, chunk.attempt)
			err := s.uploadPart(snap, file, uploadID, completedParts, chunk.offset, chunk.size)
			errCh <- chunkUploadResult{
				err:   err,
				chunk: &chunk,
			}
		}
	}
}

// List returns a sorted list of snapshot files present in the object store.
// If Bucket versioning is not enabled for S3 bucket:
//   - It returns a sorted list of all snapshot files present in the object store.
//
// If Bucket versioning is enabled for S3 bucket:
//   - It returns a sorted list of all latest snapshot files present in the object store.
//   - It also captures the "ImmutabilityExpiryTime" and "VersionID" of corresponding versioned snapshot.
func (s *S3SnapStore) List(_ bool) (brtypes.SnapList, error) {
	var snapList brtypes.SnapList
	prefixTokens := strings.Split(s.prefix, "/")
	// Last element of the tokens is backup version
	// Consider the parent of the backup version level (Required for Backward Compatibility)
	prefix := path.Join(strings.Join(prefixTokens[:len(prefixTokens)-1], "/"))

	// Get the status of bucket versioning.
	// Note: Bucket versioning will always be enabled for object lock.
	versioningStatus, err := s.client.GetBucketVersioning(&s3.GetBucketVersioningInput{
		Bucket: &s.bucket,
	})
	if err != nil {
		return nil, err
	}

	if versioningStatus.Status != nil && *versioningStatus.Status == "Enabled" {
		// versioning is found to be enabled on given bucket.
		logrus.Info("Bucket versioning is found to be enabled.")

		isObjectLockEnabled, bucketImmutableExpiryTimeInDays, err := GetBucketImmutabilityTime(s)
		if err != nil {
			logrus.Warnf("unable to check object lock configuration for the bucket: %v", err)
		}
		if !isObjectLockEnabled {
			logrus.Warnf("Bucket versioning is found to be enabled but object lock is not found to be enabled.")
			logrus.Warnf("Please enable the object lock as well for the given bucket for immutability of snapshots.")
		}

		in := &s3.ListObjectVersionsInput{
			Bucket: aws.String(s.bucket),
			Prefix: aws.String(prefix),
		}

		if err := s.client.ListObjectVersionsPages(in, func(page *s3.ListObjectVersionsOutput, lastPage bool) bool {
			for _, version := range page.Versions {
				if *version.IsLatest {
					k := (*version.Key)[len(*page.Prefix):]
					if strings.Contains(k, backupVersionV1) || strings.Contains(k, backupVersionV2) {
						snap, err := ParseSnapshot(path.Join(prefix, k))
						if err != nil {
							// Warning
							logrus.Warnf("Invalid snapshot found. Ignoring it: %s", k)
						} else {
							// capture the versionID of snapshot and immutability expiry time of snapshot.
							snap.VersionID = *version.VersionId
							if bucketImmutableExpiryTimeInDays != nil {
								// To get S3 object's "RetainUntilDate" or "ImmutabilityExpiryTime", backup-restore need to make an API call for each snapshots.
								// To avoid API calls for each snapshots, backup-restore is calculating the "ImmutabilityExpiryTime" using bucket retention period.
								// ImmutabilityExpiryTime = SnapshotCreationTime + ObjectRetentionTimeInDays
								snap.ImmutabilityExpiryTime = snap.CreatedOn.Add(time.Duration(*bucketImmutableExpiryTimeInDays) * 24 * time.Hour)
							} else {
								// retry to get bucketImmutableExpiryTimeInDays
								_, bucketImmutableExpiryTimeInDays, err = GetBucketImmutabilityTime(s)
								if err != nil {
									logrus.Warnf("unable to get bucket immutability expiry time: %v", err)
								}
							}
							snapList = append(snapList, snap)
						}
					}
				} else {
					// Warning
					logrus.Warnf("Snapshot: %s with versionID: %s found to be not latest, it was last modified: %s. Ignoring it.", *version.Key, *version.VersionId, version.LastModified)
				}
			}
			return !lastPage
		}); err != nil {
			return nil, err
		}
	} else {
		// versioning is not found to be enabled on given bucket.
		logrus.Info("Bucket versioning is not found to be enabled.")
		in := &s3.ListObjectsInput{
			Bucket: aws.String(s.bucket),
			Prefix: aws.String(prefix),
		}

		if err := s.client.ListObjectsPages(in, func(page *s3.ListObjectsOutput, lastPage bool) bool {
			for _, key := range page.Contents {
				k := (*key.Key)[len(*page.Prefix):]
				if strings.Contains(k, backupVersionV1) || strings.Contains(k, backupVersionV2) {
					snap, err := ParseSnapshot(path.Join(prefix, k))
					if err != nil {
						// Warning
						logrus.Warnf("Invalid snapshot found. Ignoring it: %s", k)
					} else {
						snapList = append(snapList, snap)
					}
				}
			}
			return !lastPage
		}); err != nil {
			return nil, err
		}
	}

	sort.Sort(snapList)
	return snapList, nil
}

// Delete should delete the snapshot file from store
func (s *S3SnapStore) Delete(snap brtypes.Snapshot) error {
	if len(snap.VersionID) > 0 {
		// to delete versioned snapshots present in bucket.
		if _, err := s.client.DeleteObject(&s3.DeleteObjectInput{
			Bucket:    aws.String(s.bucket),
			Key:       aws.String(path.Join(snap.Prefix, snap.SnapDir, snap.SnapName)),
			VersionId: &snap.VersionID,
		}); err != nil {
			return err
		}
	} else {
		// to delete non-versioned snapshots present in bucket.
		if _, err := s.client.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(path.Join(snap.Prefix, snap.SnapDir, snap.SnapName)),
		}); err != nil {
			return err
		}
	}
	return nil
}

// GetS3CredentialsLastModifiedTime returns the latest modification timestamp of the AWS credential file(s)
func GetS3CredentialsLastModifiedTime() (time.Time, error) {
	// TODO: @renormalize Remove this extra handling in v0.31.0
	// Check if a JSON file is present in the directory, if it is present -> the JSON file must be used for credentials.
	if dir, isSet := os.LookupEnv(awsCredentialDirectory); isSet {
		modificationTimeStamp, err := getJSONCredentialModifiedTime(dir)
		if err != nil {
			return time.Time{}, fmt.Errorf("failed to fetch credential modification time for AWS with error: %w", err)
		}
		if !modificationTimeStamp.IsZero() {
			return modificationTimeStamp, nil
		}
		// Non JSON credential files might exist in the credential directory, do not return
	}

	if dir, isSet := os.LookupEnv(awsCredentialDirectory); isSet {
		// credential files which are essential for creating the S3 snapstore
		credentialFiles := []string{"accessKeyID", "region", "secretAccessKey"}
		for i := range credentialFiles {
			credentialFiles[i] = filepath.Join(dir, credentialFiles[i])
		}
		awsTimeStamp, err := getLatestCredentialsModifiedTime(credentialFiles)
		if err != nil {
			return time.Time{}, fmt.Errorf("failed to get AWS credential timestamp from the directory %v with error: %w", dir, err)
		}
		return awsTimeStamp, nil
	}

	if filename, isSet := os.LookupEnv(awsCredentialJSONFile); isSet {
		credentialFiles := []string{filename}
		awsTimeStamp, err := getLatestCredentialsModifiedTime(credentialFiles)
		if err != nil {
			return time.Time{}, fmt.Errorf("failed to fetch file information of the AWS JSON credential file %v with error: %w", filename, err)
		}
		return awsTimeStamp, nil
	}

	return time.Time{}, fmt.Errorf("no environment variable set for the AWS credential file")
}

func isAWSConfigEmpty(config *awsCredentials) error {
	if len(config.AccessKeyID) != 0 && len(config.Region) != 0 && len(config.SecretAccessKey) != 0 {
		return nil
	}
	return fmt.Errorf("aws s3 credentials: region, secretAccessKey or accessKeyID is missing")
}

// Creates SSE Credentials that are included in the S3 API calls for customer managed SSE
// Key rotation is not handled by etcd-backup-restore, use SSE through customer managed keys at your discretion
// See: https://github.com/gardener/etcd-backup-restore/pull/719#issuecomment-2016366462
func getSSECreds(sseCustomerKey, sseCustomerAlgorithm *string) (SSECredentials, error) {
	// SSE-C not utilized
	if sseCustomerKey == nil && sseCustomerAlgorithm == nil {
		return SSECredentials{}, nil
	}

	if sseCustomerKey == nil || sseCustomerAlgorithm == nil {
		return SSECredentials{}, fmt.Errorf("both sseCustomerKey and sseCustomerAlgorithm are to be provided if customer managed SSE keys are to be used")
	}

	SSECustomerKeyMD5Bytes := md5.Sum([]byte(*sseCustomerKey)) // #nosec G401 -- S3 API supports only MD5 hash for SSE headers, as per https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerSideEncryptionCustomerKeys.html
	sseCustomerKeyMD5 := base64.StdEncoding.EncodeToString(SSECustomerKeyMD5Bytes[:])
	return SSECredentials{
		sseCustomerKey:       *sseCustomerKey,
		sseCustomerKeyMD5:    sseCustomerKeyMD5,
		sseCustomerAlgorithm: *sseCustomerAlgorithm,
	}, nil
}

// GetBucketImmutabilityTime check objectlock is enabled for given bucket, if it does returns the retention period.
func GetBucketImmutabilityTime(s *S3SnapStore) (bool, *int64, error) {
	objectConfig, err := s.client.GetObjectLockConfiguration(&s3.GetObjectLockConfigurationInput{
		Bucket: aws.String(s.bucket),
	})
	if err != nil {
		return false, nil, err
	}

	if *objectConfig.ObjectLockConfiguration.ObjectLockEnabled == "Enabled" {
		// assumption: retention period of bucket will always be in days, not years.
		return true, objectConfig.ObjectLockConfiguration.Rule.DefaultRetention.Days, nil
	}

	return false, nil, nil
}
