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

	"github.com/gardener/etcd-backup-restore/pkg/snapstore/internal/s3api"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/sirupsen/logrus"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
)

const (
	// Total number of chunks to be uploaded must be one less than maximum limit allowed.
	s3NoOfChunk            int64 = 9999
	awsCredentialDirectory       = "AWS_APPLICATION_CREDENTIALS"
	awsCredentialJSONFile        = "AWS_APPLICATION_CREDENTIALS_JSON"
)

type awsCredentials struct {
	SSECustomerAlgorithm       *string `json:"sseCustomerAlgorithm,omitempty"`
	SSECustomerKey             *string `json:"sseCustomerKey,omitempty"`
	Endpoint                   *string `json:"endpoint,omitempty"`
	S3ForcePathStyle           *bool   `json:"s3ForcePathStyle,omitempty"`
	InsecureSkipVerify         *bool   `json:"insecureSkipVerify,omitempty"`
	TrustedCaCert              *string `json:"trustedCaCert,omitempty"`
	RequestChecksumCalculation *string `json:"requestChecksumCalculation,omitempty"`
	ResponseChecksumValidation *string `json:"responseChecksumValidation,omitempty"`
	AccessKeyID                string  `json:"accessKeyID"`
	Region                     string  `json:"region"`
	SecretAccessKey            string  `json:"secretAccessKey"`
	BucketName                 string  `json:"bucketName"`
	RoleARN                    string  `json:"roleARN"`
	TokenPath                  string  `json:"tokenPath"`
}

// SSECredentials to hold fields for server-side encryption in I/O operations
type SSECredentials struct {
	sseCustomerAlgorithm string
	sseCustomerKey       string
	sseCustomerKeyMD5    string
}

// S3SnapStore is snapstore with AWS S3 object store as backend
type S3SnapStore struct {
	client s3api.Client
	SSECredentials
	prefix  string
	bucket  string
	tempDir string
	// maxParallelChunkUploads hold the maximum number of parallel chunk uploads allowed.
	maxParallelChunkUploads uint
	minChunkSize            int64
}

// NewS3SnapStore create new S3SnapStore from shared configuration with specified bucket
func NewS3SnapStore(config *brtypes.SnapstoreConfig) (*S3SnapStore, error) {
	cfgOpts, cliOpts, sseCreds, err := getConfigOpts(getEnvPrefixString(config))
	if err != nil {
		return nil, err
	}

	if config.EndpointOverride != "" {
		cfgOpts = append(cfgOpts, awsconfig.WithBaseEndpoint(config.EndpointOverride))
	}

	cfg, err := awsconfig.LoadDefaultConfig(context.TODO(), cfgOpts...)
	if err != nil {
		return nil, fmt.Errorf("new AWS config failed: %w", err)
	}

	cli := s3.NewFromConfig(cfg, cliOpts...)
	return NewS3FromClient(config.Container, config.Prefix, config.TempDir, config.MaxParallelChunkUploads, config.MinChunkSize, cli, sseCreds), nil
}

func getConfigOpts(prefixString string) ([]func(*awsconfig.LoadOptions) error, []func(*s3.Options), SSECredentials, error) {
	if filename, isSet := os.LookupEnv(prefixString + awsCredentialJSONFile); isSet {
		creds, cliOpts, sseCreds, err := readAWSCredentialsJSONFile(filename)
		if err != nil {
			return nil, nil, SSECredentials{}, fmt.Errorf("error getting credentials using %v file with error %w", filename, err)
		}
		return creds, cliOpts, sseCreds, nil
	}

	// TODO: @renormalize Remove this extra handling in v0.31.0
	// Check if a JSON file is present in the directory, if it is present -> the JSON file must be used for credentials.
	if dir, isSet := os.LookupEnv(prefixString + awsCredentialDirectory); isSet {
		jsonCredentialFile, err := findFileWithExtensionInDir(dir, ".json")
		if err != nil {
			return nil, nil, SSECredentials{}, fmt.Errorf("error while finding a JSON credential file in %v directory with error: %w", dir, err)
		}
		if jsonCredentialFile != "" {
			creds, cliOpts, sseCreds, err := readAWSCredentialsJSONFile(jsonCredentialFile)
			if err != nil {
				return nil, nil, SSECredentials{}, fmt.Errorf("error getting credentials using %v JSON file in a directory with error: %w", jsonCredentialFile, err)
			}
			return creds, cliOpts, sseCreds, nil
		}
		// Non JSON credential files might exist in the credential directory, do not return
	}

	if dir, isSet := os.LookupEnv(prefixString + awsCredentialDirectory); isSet {
		creds, cliOpts, sseCreds, err := readAWSCredentialFiles(dir)
		if err != nil {
			return nil, nil, SSECredentials{}, fmt.Errorf("error getting credentials from %v directory with error %w", dir, err)
		}
		return creds, cliOpts, sseCreds, nil
	}

	return nil, nil, SSECredentials{}, nil
}

func readAWSCredentialsJSONFile(filename string) ([]func(*awsconfig.LoadOptions) error, []func(*s3.Options), SSECredentials, error) {
	awsConfig, err := credentialsFromJSON(filename)
	if err != nil {
		return nil, nil, SSECredentials{}, err
	}
	return awsCredentialsFromConfig(awsConfig)
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

func readAWSCredentialFiles(dirname string) ([]func(*awsconfig.LoadOptions) error, []func(*s3.Options), SSECredentials, error) {
	awsConfig, err := readAWSCredentialFromDir(dirname)
	if err != nil {
		return nil, nil, SSECredentials{}, err
	}
	return awsCredentialsFromConfig(awsConfig)
}

func awsCredentialsFromConfig(awsConfig *awsCredentials) ([]func(*awsconfig.LoadOptions) error, []func(*s3.Options), SSECredentials, error) {
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
		return nil, nil, SSECredentials{}, err
	}

	var (
		cfgOpts = []func(*awsconfig.LoadOptions) error{
			awsconfig.WithRegion(awsConfig.Region),
			awsconfig.WithHTTPClient(httpClient),
		}
		cliOpts             = []func(*s3.Options){}
		credentialsProvider aws.CredentialsProvider
	)

	if awsConfig.AccessKeyID != "" {
		credentialsProvider = credentials.NewStaticCredentialsProvider(awsConfig.AccessKeyID, awsConfig.SecretAccessKey, "")
	} else {
		credentialsProvider = stscreds.NewWebIdentityRoleProvider(sts.NewFromConfig(aws.Config{Region: awsConfig.Region}), awsConfig.RoleARN, stscreds.IdentityTokenFile(awsConfig.TokenPath))
	}
	cfgOpts = append(cfgOpts, awsconfig.WithCredentialsProvider(aws.NewCredentialsCache(credentialsProvider)))

	// TODO: @renormalize support for passing Endpoint through the credential file must be removed in v0.42.0.
	if awsConfig.Endpoint != nil {
		logrus.Warnf("Passing endpoint override through the credential file is now deprecated. Please use the --store-endpoint-override flag instead.")
		cfgOpts = append(cfgOpts, awsconfig.WithBaseEndpoint(*awsConfig.Endpoint))
	}

	if awsConfig.S3ForcePathStyle != nil {
		cliOpts = append(cliOpts, func(o *s3.Options) {
			o.UsePathStyle = *awsConfig.S3ForcePathStyle
		})
	}
	if awsConfig.RequestChecksumCalculation != nil {
		v, err := requestChecksumCalculation(*awsConfig.RequestChecksumCalculation)
		if err != nil {
			return nil, nil, SSECredentials{}, err
		}
		cliOpts = append(cliOpts, func(o *s3.Options) {
			o.RequestChecksumCalculation = v
		})
	}
	if awsConfig.ResponseChecksumValidation != nil {
		v, err := responseChecksumValidation(*awsConfig.ResponseChecksumValidation)
		if err != nil {
			return nil, nil, SSECredentials{}, err
		}
		cliOpts = append(cliOpts, func(o *s3.Options) {
			o.ResponseChecksumValidation = v
		})
	}

	return cfgOpts, cliOpts, sseCreds, nil
}

const (
	checksumWhenSupported = "when_supported"
	checksumWhenRequired  = "when_required"
)

func requestChecksumCalculation(value string) (aws.RequestChecksumCalculation, error) {
	switch strings.ToLower(value) {
	case checksumWhenSupported:
		return aws.RequestChecksumCalculationWhenSupported, nil
	case checksumWhenRequired:
		return aws.RequestChecksumCalculationWhenRequired, nil
	default:
		return 0, fmt.Errorf("invalid value %q for requestChecksumCalculation, must be when_supported/when_required", value)
	}
}

func responseChecksumValidation(value string) (aws.ResponseChecksumValidation, error) {
	switch strings.ToLower(value) {
	case checksumWhenSupported:
		return aws.ResponseChecksumValidationWhenSupported, nil
	case checksumWhenRequired:
		return aws.ResponseChecksumValidationWhenRequired, nil
	default:
		return 0, fmt.Errorf("invalid value %q for responseChecksumValidation, must be when_supported/when_required", value)
	}
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
			data, err := os.ReadFile(filepath.Join(dirname, "accessKeyID")) // #nosec G304 -- this is a trusted file, obtained via user input.
			if err != nil {
				return nil, err
			}
			awsConfig.AccessKeyID = string(data)
		case "region":
			data, err := os.ReadFile(filepath.Join(dirname, "region")) // #nosec G304 -- this is a trusted file, obtained via user input.
			if err != nil {
				return nil, err
			}
			awsConfig.Region = string(data)
		case "secretAccessKey":
			data, err := os.ReadFile(filepath.Join(dirname, "secretAccessKey")) // #nosec G304 -- this is a trusted file, obtained via user input.
			if err != nil {
				return nil, err
			}
			awsConfig.SecretAccessKey = string(data)
		case "endpoint":
			data, err := os.ReadFile(filepath.Join(dirname, "endpoint")) // #nosec G304 -- this is a trusted file, obtained via user input.
			if err != nil {
				return nil, err
			}
			awsConfig.Endpoint = ptr.To(string(data))
		case "s3ForcePathStyle":
			data, err := os.ReadFile(filepath.Join(dirname, "s3ForcePathStyle")) // #nosec G304 -- this is a trusted file, obtained via user input.
			if err != nil {
				return nil, err
			}
			val, err := strconv.ParseBool(string(data))
			if err != nil {
				return nil, err
			}
			awsConfig.S3ForcePathStyle = &val
		case "insecureSkipVerify":
			data, err := os.ReadFile(filepath.Join(dirname, "insecureSkipVerify")) // #nosec G304 -- this is a trusted file, obtained via user input.
			if err != nil {
				return nil, err
			}
			val, err := strconv.ParseBool(string(data))
			if err != nil {
				return nil, err
			}
			awsConfig.InsecureSkipVerify = &val
		case "trustedCaCert":
			data, err := os.ReadFile(filepath.Join(dirname, "trustedCaCert")) // #nosec G304 -- this is a trusted file, obtained via user input.
			if err != nil {
				return nil, err
			}
			awsConfig.TrustedCaCert = ptr.To(string(data))
		case "sseCustomerKey":
			data, err := os.ReadFile(filepath.Join(dirname, "sseCustomerKey")) // #nosec G304 -- this is a trusted file, obtained via user input.
			if err != nil {
				return nil, err
			}
			awsConfig.SSECustomerKey = ptr.To(string(data))
		case "sseCustomerAlgorithm":
			data, err := os.ReadFile(filepath.Join(dirname, "sseCustomerAlgorithm")) // #nosec G304 -- this is a trusted file, obtained via user input.
			if err != nil {
				return nil, err
			}
			awsConfig.SSECustomerAlgorithm = ptr.To(string(data))
		case "roleARN":
			roleARN, err := os.ReadFile(filepath.Join(dirname, "roleARN")) // #nosec G304 -- this is a trusted file, obtained via user input.
			if err != nil {
				return nil, err
			}
			awsConfig.RoleARN = string(roleARN)
		case "token":
			awsConfig.TokenPath = filepath.Join(dirname, "token")
		case "requestChecksumCalculation":
			data, err := os.ReadFile(filepath.Join(dirname, "requestChecksumCalculation")) // #nosec G304 -- this is a trusted file, obtained via user input.
			if err != nil {
				return nil, err
			}
			awsConfig.RequestChecksumCalculation = ptr.To(string(data))
		case "responseChecksumValidation":
			data, err := os.ReadFile(filepath.Join(dirname, "responseChecksumValidation")) // #nosec G304 -- this is a trusted file, obtained via user input.
			if err != nil {
				return nil, err
			}
			awsConfig.ResponseChecksumValidation = ptr.To(string(data))
		}
	}

	if err := isAWSConfigEmpty(awsConfig); err != nil {
		return nil, err
	}
	return awsConfig, nil
}

// NewS3FromClient will create the new S3 snapstore object from S3 client
func NewS3FromClient(bucket, prefix, tempDir string, maxParallelChunkUploads uint, minChunkSize int64, cli s3api.Client, sseCreds SSECredentials) *S3SnapStore {
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
	getObjectInput := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path.Join(snap.Prefix, snap.SnapDir, snap.SnapName)),
	}

	if snap.VersionID != nil {
		// To fetch the versioned snapshot incase object lock is enabled for bucket.
		getObjectInput.VersionId = snap.VersionID
	}
	if s.sseCustomerKey != "" {
		// Customer managed Server Side Encryption
		getObjectInput.SSECustomerAlgorithm = aws.String(s.sseCustomerAlgorithm)
		getObjectInput.SSECustomerKey = aws.String(s.sseCustomerKey)
		getObjectInput.SSECustomerKeyMD5 = aws.String(s.sseCustomerKeyMD5)
	}
	getObjecOutput, err := s.client.GetObject(context.TODO(), getObjectInput)
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
	ctx, cancel := context.WithTimeout(context.TODO(), chunkUploadTimeout)
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
	uploadOutput, err := s.client.CreateMultipartUpload(ctx, createMultipartUploadInput)
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
		completedParts = make([]s3types.CompletedPart, noOfChunks)
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
		ctx, cancel = context.WithTimeout(context.TODO(), chunkUploadTimeout)
		defer cancel()
		logrus.Infof("Aborting the multipart upload with upload ID : %s", *uploadOutput.UploadId)
		_, err = s.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket:   &s.bucket,
			Key:      aws.String(path.Join(prefix, snap.SnapDir, snap.SnapName)),
			UploadId: uploadOutput.UploadId,
		})
	} else {
		ctx = context.TODO()
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		logrus.Infof("Finishing the multipart upload with upload ID : %s", *uploadOutput.UploadId)
		_, err = s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:   &s.bucket,
			Key:      aws.String(path.Join(prefix, snap.SnapDir, snap.SnapName)),
			UploadId: uploadOutput.UploadId,
			MultipartUpload: &s3types.CompletedMultipartUpload{
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

func (s *S3SnapStore) uploadPart(snap *brtypes.Snapshot, file *os.File, uploadID *string, completedParts []s3types.CompletedPart, offset, chunkSize int64) error {
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
	partNumber := int32((offset / chunkSize) + 1) // #nosec G115 -- partNumber is positive integer between 1 and 10000.

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

	uploadPartOutput, err := s.client.UploadPart(ctx, uploadPartInput)
	if err == nil {
		completedPart := s3types.CompletedPart{
			ETag:       uploadPartOutput.ETag,
			PartNumber: &partNumber,
		}
		completedParts[partNumber-1] = completedPart
	}
	return err
}

func (s *S3SnapStore) partUploader(wg *sync.WaitGroup, stopCh <-chan struct{}, snap *brtypes.Snapshot, file *os.File, uploadID *string, completedParts []s3types.CompletedPart, chunkUploadCh <-chan chunk, errCh chan<- chunkUploadResult) {
	defer wg.Done()
	for {
		select {
		case <-stopCh:
			return
		case uploadChunk, ok := <-chunkUploadCh:
			if !ok {
				return
			}
			logrus.Infof("Uploading chunk with id: %d, offset: %d, attempt: %d", uploadChunk.id, uploadChunk.offset, uploadChunk.attempt)
			err := s.uploadPart(snap, file, uploadID, completedParts, uploadChunk.offset, uploadChunk.size)
			errCh <- chunkUploadResult{
				err:   err,
				chunk: &uploadChunk,
			}
		}
	}
}

// List returns a sorted list of snapshot files present in the object store.
// If Bucket versioning is not enabled for S3 bucket:
//   - It returns a sorted list of all snapshot files present in the object store.
//
// If Bucket versioning or object lock is enabled for S3 bucket:
//   - It returns a sorted list of all firstCreated/oldest snapshot files present in the object store.
//   - It also captures the "ImmutabilityExpiryTime" and "VersionID" of corresponding versioned snapshot.
func (s *S3SnapStore) List(includeAll bool) (brtypes.SnapList, error) {
	var snapList brtypes.SnapList
	prefixTokens := strings.Split(s.prefix, "/")
	// Last element of the tokens is backup version
	// Consider the parent of the backup version level (Required for Backward Compatibility)
	prefix := path.Join(strings.Join(prefixTokens[:len(prefixTokens)-1], "/"))

	// Get the status of bucket versioning.
	// Note: Bucket versioning will always be enabled for object lock.
	versioningStatus, err := s.client.GetBucketVersioning(context.TODO(), &s3.GetBucketVersioningInput{
		Bucket: &s.bucket,
	})
	if err != nil {
		return nil, err
	}

	if versioningStatus != nil && versioningStatus.Status == s3types.BucketVersioningStatusEnabled {
		// versioning is found to be enabled on given bucket.
		logrus.Info("Bucket versioning is found to be enabled.")

		var bucketImmutableExpiryTimeInDays *int32
		var isObjectLockEnabled bool

		if err := retry.OnError(retry.DefaultBackoff, func(err error) bool {
			return err != nil
		}, func() error {
			if isObjectLockEnabled, bucketImmutableExpiryTimeInDays, err = GetBucketImmutabilityTime(s); err != nil {
				logrus.Errorf("unable to check bucket immutability retention period: %v", err)
				return err
			} else if !isObjectLockEnabled {
				logrus.Warnf("Bucket versioning is found to be enabled but object lock is not found to be enabled.")
				logrus.Warnf("Please enable the object lock for the given bucket to have immutable snapshots.")
			} else if bucketImmutableExpiryTimeInDays == nil {
				logrus.Warnf("Object lock is found to be enabled but object lock rules or default retention period are not found to be set.")
			}
			return nil
		}); err != nil {
			logrus.Errorf("unable to check object lock configuration for the bucket: %v", err)
		}

		in := &s3.ListObjectVersionsInput{
			Bucket: aws.String(s.bucket),
			Prefix: aws.String(prefix),
		}

		type snapshotMetaInfo struct {
			creationTime time.Time
			versionID    string
		}

		// allSnapKeyMapToSnapshotInfo contains oldest snapshots keys mapped to their versionID and creation timestamp.
		allSnapKeyMapToSnapshotInfo := make(map[string]*snapshotMetaInfo)

		// allDeleteMarkersInfo contains key of all delete markers present(if any) in the S3 bucket.
		allDeleteMarkersInfo := make(map[string]struct{})

		paginator := s3.NewListObjectVersionsPaginator(s.client, in)
		for paginator.HasMorePages() {
			page, err := paginator.NextPage(context.TODO())
			if err != nil {
				return nil, err
			}

			for _, version := range page.Versions {
				snapKey := (*version.Key)[len(*page.Prefix):]
				if strings.Contains(snapKey, backupVersionV1) || strings.Contains(snapKey, backupVersionV2) {
					// Add snapshot key to map
					//   - if snapshot key not found to be already present in map
					//   - or if the incoming version of snapshot key is older
					//     than already seen version of snapshot key.
					if value, isKeyPresent := allSnapKeyMapToSnapshotInfo[*version.Key]; !isKeyPresent || (*version.LastModified).Before(value.creationTime) {

						if isKeyPresent && (*version.LastModified).Before(value.creationTime) {
							logrus.Infof("Key: [%s] already present in map with versionID: [%s]\n", *version.Key, value.versionID)
							logrus.Infof("Updating the map with key: [%s] in map with versionID: [%s]\n", *version.Key, *version.VersionId)
						}

						// update the map.
						allSnapKeyMapToSnapshotInfo[*version.Key] = &snapshotMetaInfo{
							creationTime: *version.LastModified,
							versionID:    *version.VersionId,
						}
					}
				}
			}

			// traverse all the deletion markers present(if any) in the bucket
			for _, deletionMarker := range page.DeleteMarkers {
				deletionKey := (*deletionMarker.Key)[len(*page.Prefix):]
				if strings.Contains(deletionKey, backupVersionV1) || strings.Contains(deletionKey, backupVersionV2) {
					allDeleteMarkersInfo[*deletionMarker.Key] = struct{}{}
				}
			}
		}

		for key, val := range allSnapKeyMapToSnapshotInfo {
			// If a snapshot key has a delete marker present in the bucket,
			// check whether that snapshot object is marked to be ignored.
			if _, isDeleteMarkerPresent := allDeleteMarkersInfo[key]; isDeleteMarkerPresent && !includeAll {
				if IsSnapshotMarkedToBeIgnored(s.client, s.bucket, key, val.versionID) {
					logrus.Infof("Snapshot: %s with versionID: %s is marked to be ignored", key, val.versionID)
					continue
				}
			}

			snap, err := ParseSnapshot(key)
			if err != nil {
				// Warning
				logrus.Warnf("Invalid snapshot found. Ignoring it: %s", key)
			} else {
				// capture the versionID of snapshot and immutability expiry time of snapshot.
				snap.VersionID = aws.String(val.versionID)
				if bucketImmutableExpiryTimeInDays != nil {
					// To get S3 object's "RetainUntilDate" or "ImmutabilityExpiryTime", backup-restore need to make an API call for each snapshot.
					// To avoid API calls for each snapshot, backup-restore is calculating the "ImmutabilityExpiryTime" using bucket retention period.
					// ImmutabilityExpiryTime = SnapshotCreationTime + ObjectRetentionTimeInDays
					snap.ImmutabilityExpiryTime = snap.CreatedOn.Add(time.Duration(*bucketImmutableExpiryTimeInDays) * 24 * time.Hour)
				}
				snapList = append(snapList, snap)
			}
		}
	} else {
		// versioning is not found to be enabled on given bucket.
		logrus.Info("Bucket versioning is not found to be enabled.")
		in := &s3.ListObjectsV2Input{
			Bucket: aws.String(s.bucket),
			Prefix: aws.String(prefix),
		}

		paginator := s3.NewListObjectsV2Paginator(s.client, in)
		for paginator.HasMorePages() {
			page, err := paginator.NextPage(context.TODO())
			if err != nil {
				return nil, err
			}

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
		}
	}

	sort.Sort(snapList)
	return snapList, nil
}

// Delete should delete the snapshot file from store
func (s *S3SnapStore) Delete(snap brtypes.Snapshot) error {
	deleteObjectInput := &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path.Join(snap.Prefix, snap.SnapDir, snap.SnapName)),
	}

	if snap.VersionID != nil {
		// to delete versioned snapshot present in bucket
		// update deleteObject input with versionID of snapshot.
		deleteObjectInput.VersionId = snap.VersionID
	}

	// delete snapshot present in bucket.
	_, err := s.client.DeleteObject(context.TODO(), deleteObjectInput)
	return err
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
		// either static or web identity credential files should be set
		var (
			staticCredentialsFiles      = []string{filepath.Join(dir, "region"), filepath.Join(dir, "accessKeyID"), filepath.Join(dir, "secretAccessKey")}
			webIdentityCredentialsFiles = []string{filepath.Join(dir, "region"), filepath.Join(dir, "roleARN"), filepath.Join(dir, "token")}
		)

		awsTimestamp, err1 := getLatestCredentialsModifiedTime(staticCredentialsFiles)
		if err1 == nil {
			return awsTimestamp, nil
		}

		awsTimestamp, err2 := getLatestCredentialsModifiedTime(webIdentityCredentialsFiles)
		if err2 == nil {
			return awsTimestamp, nil
		}

		return time.Time{}, fmt.Errorf("failed to get AWS credential timestamp from the directory %v with error: %w", dir, errors.Join(err1, err2))
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
	if len(config.Region) == 0 {
		return fmt.Errorf("aws s3 credentials: region is missing")
	}

	if len(config.AccessKeyID) != 0 && len(config.SecretAccessKey) != 0 {
		return nil
	}

	if len(config.RoleARN) != 0 && len(config.TokenPath) != 0 {
		return nil
	}

	return fmt.Errorf("aws s3 credentials: either set secretAccessKey and accessKeyID or roleARN and token")
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
func GetBucketImmutabilityTime(s *S3SnapStore) (bool, *int32, error) {
	objectConfig, err := s.client.GetObjectLockConfiguration(context.TODO(), &s3.GetObjectLockConfigurationInput{
		Bucket: aws.String(s.bucket),
	})
	if err != nil {
		return false, nil, err
	}

	if objectConfig != nil && objectConfig.ObjectLockConfiguration != nil {
		// check if object lock is enabled for bucket
		if objectConfig.ObjectLockConfiguration.ObjectLockEnabled == s3types.ObjectLockEnabledEnabled {
			// check if rules for object lock is defined for bucket.
			if objectConfig.ObjectLockConfiguration.Rule != nil && objectConfig.ObjectLockConfiguration.Rule.DefaultRetention != nil {
				//assumption: retention period of bucket should always be in days, not years.
				return true, objectConfig.ObjectLockConfiguration.Rule.DefaultRetention.Days, nil
			}
			// object lock is enabled but rules are not defined for bucket
			return true, nil, nil
		}
		// object lock is not enabled for bucket
		return false, nil, nil
	}

	return false, nil, fmt.Errorf("got nil object lock configuration")
}

// IsSnapshotMarkedToBeIgnored checks whether snapshot object key with given versionID is tagged to be ignored or not.
func IsSnapshotMarkedToBeIgnored(client s3api.Client, bucketName, key, versionID string) bool {
	out, err := client.GetObjectTagging(context.TODO(), &s3.GetObjectTaggingInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(key),
		VersionId: aws.String(versionID),
	})
	if err != nil {
		return false
	}

	for _, tagOnSnap := range out.TagSet {
		if tagOnSnap.Key != nil && *tagOnSnap.Key == brtypes.ExcludeSnapshotMetadataKey && tagOnSnap.Value != nil && *tagOnSnap.Value == "true" {
			return true
		}
	}

	return false
}
