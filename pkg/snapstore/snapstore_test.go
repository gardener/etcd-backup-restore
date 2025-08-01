// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	fake "github.com/gophercloud/gophercloud/testhelper/client"
	"github.com/sirupsen/logrus"

	. "github.com/gardener/etcd-backup-restore/pkg/snapstore"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	prefixV1 string = "v1"
	prefixV2 string = "v2"
)

var (
	bucket                  string = "mock-bucket"
	s3ObjectLockedBucket    string = "mock-S3ObjectLockedBucket"
	s3NonObjectLockedBucket string = "mock-S3NonObjectLockedBucket"
	objectMap                      = map[string]*[]byte{}
)

// testSnapStore embedds brtypes.Snapstore and contains the number of
// objects that are stored per snapshot for a given storage provider
type testSnapStore struct {
	brtypes.SnapStore
	objectCountPerSnapshot int
}

// tagger is the interface that is to be implemented by mock snapstores to set tags on snapshots
type tagger interface {
	// Sets all of the tags for a mocked snapshot
	setTags(string, map[string]string)
	// Deletes all of the tags of a mocked snapshot
	deleteTags(string)
}

var _ = Describe("Save, List, Fetch, Delete from mock snapstore", func() {
	var (
		snap1        brtypes.Snapshot
		snap2        brtypes.Snapshot
		snap3        brtypes.Snapshot
		snap4        brtypes.Snapshot
		snap5        brtypes.Snapshot
		snapstores   map[string]testSnapStore
		awsS3Client  *mockS3Client
		gcsClient    *mockGCSClient
		absClient    *fakeABSContainerClient
		aliOSSClient *mockOSSClient
	)

	BeforeEach(func() {
		now := time.Now().Unix()
		snap1 = brtypes.Snapshot{
			CreatedOn:     time.Unix(now, 0).UTC(),
			StartRevision: 0,
			LastRevision:  2088,
			Kind:          brtypes.SnapshotKindFull,
			Prefix:        prefixV1,
		}
		snap2 = brtypes.Snapshot{
			CreatedOn:     time.Unix(now+100, 0).UTC(),
			StartRevision: 0,
			LastRevision:  1988,
			Kind:          brtypes.SnapshotKindFull,
			Prefix:        prefixV1,
		}
		snap3 = brtypes.Snapshot{
			CreatedOn:     time.Unix(now+200, 0).UTC(),
			StartRevision: 0,
			LastRevision:  1958,
			Kind:          brtypes.SnapshotKindFull,
			Prefix:        prefixV1,
		}
		snap4 = brtypes.Snapshot{
			CreatedOn:     time.Unix(now+300, 0).UTC(),
			StartRevision: 0,
			LastRevision:  3058,
			Kind:          brtypes.SnapshotKindFull,
			Prefix:        prefixV2,
		}
		snap5 = brtypes.Snapshot{
			CreatedOn:     time.Unix(now+400, 0).UTC(),
			StartRevision: 3058,
			LastRevision:  3088,
			Kind:          brtypes.SnapshotKindDelta,
			Prefix:        prefixV2,
		}

		// prefixv1
		snap1.GenerateSnapshotName()
		snap1.GenerateSnapshotDirectory()
		snap2.GenerateSnapshotName()
		snap2.GenerateSnapshotDirectory()
		snap3.GenerateSnapshotName()
		snap3.GenerateSnapshotDirectory()

		// prefixv2
		snap4.GenerateSnapshotName()
		snap5.GenerateSnapshotName()

		gcsClient = &mockGCSClient{
			objects:    objectMap,
			prefix:     prefixV2,
			objectTags: make(map[string]map[string]string),
		}

		absClient = &fakeABSContainerClient{
			objects:     objectMap,
			prefix:      prefixV2,
			blobClients: make(map[string]*fakeBlockBlobClient),
			objectTags:  make(map[string]map[string]string),
		}

		awsS3Client = &mockS3Client{
			objects:          objectMap,
			prefix:           prefixV2,
			multiPartUploads: map[string]*[][]byte{},
		}

		aliOSSClient = &mockOSSClient{
			objects:          objectMap,
			prefix:           prefixV2,
			multiPartUploads: map[string]*[][]byte{},
			bucketName:       bucket,
		}

		snapstores = map[string]testSnapStore{
			brtypes.SnapstoreProviderSwift: {
				SnapStore:              NewSwiftSnapstoreFromClient(bucket, prefixV2, "/tmp", 5, brtypes.MinChunkSize, fake.ServiceClient()),
				objectCountPerSnapshot: 3,
			},
			brtypes.SnapstoreProviderABS: {
				SnapStore:              NewABSSnapStoreFromClient(bucket, prefixV2, "/tmp", 5, brtypes.MinChunkSize, absClient),
				objectCountPerSnapshot: 1,
			},
			brtypes.SnapstoreProviderGCS: {
				SnapStore:              NewGCSSnapStoreFromClient(bucket, prefixV2, "/tmp", 5, brtypes.MinChunkSize, "", gcsClient),
				objectCountPerSnapshot: 1,
			},
			brtypes.SnapstoreProviderOSS: {
				SnapStore:              NewOSSFromBucket(prefixV2, "/tmp", bucket, 5, brtypes.MinChunkSize, aliOSSClient, getOSSMockBucket(aliOSSClient)),
				objectCountPerSnapshot: 1,
			},
			// Storage Provider S3 bucket with object lock enabled.
			brtypes.SnapstoreProviderS3: {
				SnapStore:              NewS3FromClient(s3ObjectLockedBucket, prefixV2, "/tmp", 5, brtypes.MinChunkSize, awsS3Client, SSECredentials{}),
				objectCountPerSnapshot: 1,
			},
			// Storage Provider S3 bucket with object lock not enabled
			// Note: Don't remove this test as it's require to test S3 functionality when S3 bucket versioning or object lock is not enabled.
			brtypes.SnapstoreProviderECS: {
				SnapStore: NewS3FromClient(s3NonObjectLockedBucket, prefixV2, "/tmp", 5, brtypes.MinChunkSize, &mockS3Client{
					objects:          objectMap,
					prefix:           prefixV2,
					multiPartUploads: map[string]*[][]byte{},
				}, SSECredentials{}),
				objectCountPerSnapshot: 1,
			},
			// TODO: To be removed as storage provider OCS is using S3 compatible APIs,
			// hence this test case is not adding much values.
			brtypes.SnapstoreProviderOCS: {
				SnapStore: NewS3FromClient(s3NonObjectLockedBucket, prefixV2, "/tmp", 5, brtypes.MinChunkSize, &mockS3Client{
					objects:          objectMap,
					prefix:           prefixV2,
					multiPartUploads: map[string]*[][]byte{},
				}, SSECredentials{}),
				objectCountPerSnapshot: 1,
			},
		}
	})
	AfterEach(func() {
		resetObjectMap()
	})

	Describe("When Only v1 is present", func() {
		It("When Only v1 is present", func() {
			for provider, snapStore := range snapstores {
				// Create store for mock tests
				resetObjectMap()

				var objectMapSnapshots brtypes.SnapList
				objectMapSnapshots = append(objectMapSnapshots, &snap1, &snap2)

				// number of snapshots that are added to the objectMap
				numberSnapshotsInObjectMap := setObjectMap(provider, objectMapSnapshots)
				secondSnapshotIndex := 1 * snapStore.objectCountPerSnapshot

				logrus.Infof("Running mock tests for %s when only v1 is present", provider)

				// List snap1 and snap2
				snapList, err := snapStore.List(false)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(snapList.Len()).To(Equal(numberSnapshotsInObjectMap * snapStore.objectCountPerSnapshot))
				Expect(snapList[0].SnapName).To(Equal(snap2.SnapName))
				Expect(snapList[secondSnapshotIndex].SnapName).To(Equal(snap1.SnapName))

				// Fetch snap1 - 2nd in sorted order
				rc, err := snapStore.Fetch(*snapList[secondSnapshotIndex])
				Expect(err).ShouldNot(HaveOccurred())
				defer rc.Close()
				buf := new(bytes.Buffer)
				_, err = io.Copy(buf, rc)
				Expect(err).ShouldNot(HaveOccurred())
				expectedBytes := []byte(generateContentsForSnapshot(&snap1))
				Expect(buf.Bytes()).To(Equal(expectedBytes))

				// Delete snap1
				prevLen := len(objectMap)
				err = snapStore.Delete(*snapList[secondSnapshotIndex])
				Expect(err).ShouldNot(HaveOccurred())
				snapList, err = snapStore.List(false)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(snapList.Len()).To(Equal(prevLen - 1*snapStore.objectCountPerSnapshot))

				// reset the objectMap
				resetObjectMap()
				dummyData := make([]byte, 6*1024*1024)
				// Save a new snapshot 'snap3'
				err = snapStore.Save(snap3, io.NopCloser(bytes.NewReader(dummyData)))
				Expect(err).ShouldNot(HaveOccurred())
				Expect(len(objectMap)).Should(BeNumerically(">=", 1*snapStore.objectCountPerSnapshot))
			}
		})
	})

	Describe("When both v1 and v2 are present", func() {
		It("When both v1 and v2 are present", func() {
			for provider, snapStore := range snapstores {
				// Create store for mock tests
				resetObjectMap()

				var objectMapSnapshots brtypes.SnapList
				objectMapSnapshots = append(objectMapSnapshots, &snap1, &snap4, &snap5)

				// number of snapshots that are added to the objectMap
				numberSnapshotsInObjectMap := setObjectMap(provider, objectMapSnapshots)
				secondSnapshotIndex := 1 * snapStore.objectCountPerSnapshot
				thirdSnapshotIndex := 2 * snapStore.objectCountPerSnapshot

				logrus.Infof("Running mock tests for %s when both v1 and v2 are present", provider)

				// List snap1, snap4, snap5
				snapList, err := snapStore.List(false)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(snapList.Len()).To(Equal(numberSnapshotsInObjectMap * snapStore.objectCountPerSnapshot))
				Expect(snapList[0].SnapName).To(Equal(snap1.SnapName))
				Expect(snapList[secondSnapshotIndex].SnapName).To(Equal(snap4.SnapName))
				Expect(snapList[thirdSnapshotIndex].SnapName).To(Equal(snap5.SnapName))

				// Fetch snap1 and snap4
				rc, err := snapStore.Fetch(*snapList[0])
				Expect(err).ShouldNot(HaveOccurred())
				defer rc.Close()
				buf := new(bytes.Buffer)
				_, err = io.Copy(buf, rc)
				expectedBytes := []byte(generateContentsForSnapshot(&snap1))
				Expect(err).ShouldNot(HaveOccurred())
				Expect(buf.Bytes()).To(Equal(expectedBytes))
				rc, err = snapStore.Fetch(*snapList[secondSnapshotIndex])
				Expect(err).ShouldNot(HaveOccurred())
				defer rc.Close()
				buf = new(bytes.Buffer)
				_, err = io.Copy(buf, rc)
				expectedBytes = []byte(generateContentsForSnapshot(&snap4))
				Expect(err).ShouldNot(HaveOccurred())
				Expect(buf.Bytes()).To(Equal(expectedBytes))

				// Delete snap1 and snap5
				prevLen := len(objectMap)
				err = snapStore.Delete(*snapList[0])
				Expect(err).ShouldNot(HaveOccurred())
				Expect(len(objectMap)).To(Equal(prevLen - snapStore.objectCountPerSnapshot))
				prevLen = len(objectMap)
				err = snapStore.Delete(*snapList[thirdSnapshotIndex])
				Expect(err).ShouldNot(HaveOccurred())
				Expect(len(objectMap)).To(Equal(prevLen - snapStore.objectCountPerSnapshot))

				// reset the objectMap
				resetObjectMap()
				// Save a new snapshot 'snap1'
				dummyData := make([]byte, 6*1024*1024)
				err = snapStore.Save(snap1, io.NopCloser(bytes.NewReader(dummyData)))
				Expect(err).ShouldNot(HaveOccurred())
				Expect(len(objectMap)).Should(BeNumerically(">=", snapStore.objectCountPerSnapshot))

				// Save another new snapshot 'snap4'
				prevLen = len(objectMap)
				dummyData = make([]byte, 6*1024*1024)
				err = snapStore.Save(snap4, io.NopCloser(bytes.NewReader(dummyData)))
				Expect(err).ShouldNot(HaveOccurred())
				Expect(len(objectMap)).Should(BeNumerically(">=", prevLen+snapStore.objectCountPerSnapshot))
			}
		})
	})

	Describe("When Only v2 is present", func() {
		It("When Only v2 is present", func() {
			for provider, snapStore := range snapstores {
				// Create store for mock tests
				resetObjectMap()

				var objectMapSnapshots brtypes.SnapList
				objectMapSnapshots = append(objectMapSnapshots, &snap4, &snap5)

				// number of snapshots that are added to the objectMap
				numberSnapshotsInObjectMap := setObjectMap(provider, objectMapSnapshots)
				secondSnapshotIndex := 1 * snapStore.objectCountPerSnapshot

				logrus.Infof("Running mock tests for %s when only v2 is present", provider)

				// List snap4 and snap5
				snapList, err := snapStore.List(false)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(snapList.Len()).To(Equal(numberSnapshotsInObjectMap * snapStore.objectCountPerSnapshot))
				Expect(snapList[0].SnapName).To(Equal(snap4.SnapName))
				Expect(snapList[secondSnapshotIndex].SnapName).To(Equal(snap5.SnapName))

				// The List method implemented for SnapStores which support immutable objects is tested for tagged and untagged snapshots.
				var mockClient tagger
				switch provider {
				case brtypes.SnapstoreProviderGCS:
					mockClient = gcsClient
				case brtypes.SnapstoreProviderABS:
					mockClient = absClient
				}
				if provider == brtypes.SnapstoreProviderGCS || provider == brtypes.SnapstoreProviderABS {
					// the tagged snapshot should not be returned by the List() call
					taggedSnapshot := snapList[0]
					taggedSnapshotName := path.Join(taggedSnapshot.Prefix, taggedSnapshot.SnapDir, taggedSnapshot.SnapName)
					mockClient.setTags(taggedSnapshotName, map[string]string{brtypes.ExcludeSnapshotMetadataKey: "true"})
					snapList, err = snapStore.List(false)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(snapList.Len()).Should(Equal((numberSnapshotsInObjectMap - 1) * snapStore.objectCountPerSnapshot))
					Expect(snapList[0].SnapName).ShouldNot(Equal(taggedSnapshot.SnapName))

					// listing both tagged and untagged snapshots
					snapList, err = snapStore.List(true)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(snapList.Len()).Should(Equal(numberSnapshotsInObjectMap * snapStore.objectCountPerSnapshot))
					Expect(snapList[0].SnapName).Should(Equal(taggedSnapshot.SnapName))

					// removing the tag will make the snapshot appear in the List call with false
					mockClient.deleteTags(taggedSnapshotName)
					snapList, err = snapStore.List(false)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(snapList.Len()).Should(Equal(numberSnapshotsInObjectMap * snapStore.objectCountPerSnapshot))
					Expect(snapList[0].SnapName).Should(Equal(taggedSnapshot.SnapName))
				}

				// Fetch snap5
				rc, err := snapStore.Fetch(*snapList[secondSnapshotIndex])
				Expect(err).ShouldNot(HaveOccurred())
				defer rc.Close()
				buf := new(bytes.Buffer)
				_, err = io.Copy(buf, rc)
				Expect(err).ShouldNot(HaveOccurred())
				expectedBytes := []byte(generateContentsForSnapshot(&snap5))
				Expect(buf.Bytes()).To(Equal(expectedBytes))

				// Delete snap5
				prevLen := len(objectMap)
				err = snapStore.Delete(*snapList[0])
				Expect(err).ShouldNot(HaveOccurred())
				snapList, err = snapStore.List(false)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(snapList.Len()).To(Equal(prevLen - snapStore.objectCountPerSnapshot))

				// Reset the objectMap
				resetObjectMap()
				// Save a new snapshot 'snap4'
				dummyData := make([]byte, 6*1024*1024)
				err = snapStore.Save(snap4, io.NopCloser(bytes.NewReader(dummyData)))
				Expect(err).ShouldNot(HaveOccurred())
				Expect(len(objectMap)).Should(BeNumerically(">=", snapStore.objectCountPerSnapshot))
			}
		})
	})
})

type CredentialTestConfig struct {
	EnvVariable       string
	SnapstoreProvider string
	CredentialType    string // "file" or "directory"
	CredentialFiles   []string
}

var credentialTestConfigs = []CredentialTestConfig{
	// AWS
	{
		EnvVariable:       "AWS_APPLICATION_CREDENTIALS",
		SnapstoreProvider: brtypes.SnapstoreProviderS3,
		CredentialType:    "directory",
		CredentialFiles:   []string{"accessKeyID", "region", "secretAccessKey"},
	},
	{
		EnvVariable:       "AWS_APPLICATION_CREDENTIALS_JSON",
		SnapstoreProvider: brtypes.SnapstoreProviderS3,
		CredentialType:    "file",
		CredentialFiles:   []string{"credentials.json"},
	},
	// Azure
	{
		EnvVariable:       "AZURE_APPLICATION_CREDENTIALS",
		SnapstoreProvider: brtypes.SnapstoreProviderABS,
		CredentialType:    "directory",
		CredentialFiles:   []string{"storageAccount", "storageKey"},
	},
	{
		EnvVariable:       "AZURE_APPLICATION_CREDENTIALS_JSON",
		SnapstoreProvider: brtypes.SnapstoreProviderABS,
		CredentialType:    "file",
		CredentialFiles:   []string{"credentials.json"},
	},
	// GCS
	{
		EnvVariable:       "GOOGLE_APPLICATION_CREDENTIALS",
		SnapstoreProvider: brtypes.SnapstoreProviderGCS,
		CredentialType:    "file",
		CredentialFiles:   []string{"credentials.json"},
	},
	// Swift V3ApplicationCredentials
	{
		EnvVariable:       "OPENSTACK_APPLICATION_CREDENTIALS",
		SnapstoreProvider: brtypes.SnapstoreProviderSwift,
		CredentialType:    "directory",
		CredentialFiles:   []string{"authURL", "tenantName", "domainName", "applicationCredentialID", "applicationCredentialName", "applicationCredentialSecret"},
	},
	// Swift Password
	{
		EnvVariable:       "OPENSTACK_APPLICATION_CREDENTIALS",
		SnapstoreProvider: brtypes.SnapstoreProviderSwift,
		CredentialType:    "directory",
		CredentialFiles:   []string{"authURL", "tenantName", "domainName", "username", "password"},
	},
	// Swift JSON
	{
		EnvVariable:       "OPENSTACK_APPLICATION_CREDENTIALS_JSON",
		SnapstoreProvider: brtypes.SnapstoreProviderSwift,
		CredentialType:    "file",
		CredentialFiles:   []string{"credentials.json"},
	},
	// OSS
	{
		EnvVariable:       "ALICLOUD_APPLICATION_CREDENTIALS",
		SnapstoreProvider: brtypes.SnapstoreProviderOSS,
		CredentialType:    "directory",
		CredentialFiles:   []string{"accessKeyID", "accessKeySecret", "storageEndpoint"},
	},
	{
		EnvVariable:       "ALICLOUD_APPLICATION_CREDENTIALS_JSON",
		SnapstoreProvider: brtypes.SnapstoreProviderOSS,
		CredentialType:    "file",
		CredentialFiles:   []string{"credentials.json"},
	},
	// OCS
	{
		EnvVariable:       "OPENSHIFT_APPLICATION_CREDENTIALS",
		SnapstoreProvider: brtypes.SnapstoreProviderOCS,
		CredentialType:    "directory",
		CredentialFiles:   []string{"accessKeyID", "region", "endpoint", "secretAccessKey"},
	},
	{
		EnvVariable:       "OPENSHIFT_APPLICATION_CREDENTIALS_JSON",
		SnapstoreProvider: brtypes.SnapstoreProviderOCS,
		CredentialType:    "file",
		CredentialFiles:   []string{"credentials.json"},
	},
}

var _ = Describe("Dynamic access credential rotation test for each provider", func() {
	for _, config := range credentialTestConfigs {
		config := config
		Describe(fmt.Sprintf("testing secret modification for %q with %q", config.SnapstoreProvider, config.EnvVariable), func() {
			Context("environment variable not set", func() {
				It("should return error", func() {
					newSecretModifiedTime, err := GetSnapstoreSecretModifiedTime(config.SnapstoreProvider)
					Expect(err).Should(HaveOccurred())
					Expect(newSecretModifiedTime.IsZero()).Should(BeTrue())
				})
			})
			Context("environment variable set", func() {
				var credentialDirectory string
				BeforeEach(func() {
					credentialDirectory = GinkgoT().TempDir()
					GinkgoT().Setenv(config.EnvVariable, credentialDirectory)
					// config.CredentialType == "file" -> env variable set to file
					if config.CredentialType == "file" {
						GinkgoT().Setenv(config.EnvVariable, filepath.Join(credentialDirectory, config.CredentialFiles[0]))
					}
				})
				Context("credentials do not exist", func() {
					// all files are missing
					It("should return error when all files are missing", func() {
						newSecretModifiedTime, err := GetSnapstoreSecretModifiedTime(config.SnapstoreProvider)
						Expect(err).Should(HaveOccurred())
						Expect(newSecretModifiedTime.IsZero()).Should(BeTrue())
					})
					// one file is missing
					for _, credentialFile := range config.CredentialFiles {
						credentialFile := credentialFile
						It(fmt.Sprintf("should return error when the file %q is missing", credentialFile), func() {
							// create all credential files first
							lastCreationTime, err := createCredentialFilesInDirectory(credentialDirectory, config.CredentialFiles)
							Expect(err).ShouldNot(HaveOccurred())
							Expect(lastCreationTime.IsZero()).ShouldNot(BeTrue())
							err = os.Remove(filepath.Join(credentialDirectory, credentialFile))
							Expect(err).ShouldNot(HaveOccurred())
							// remove one credential file and then run the test
							newSecretModifiedTime, err := GetSnapstoreSecretModifiedTime(config.SnapstoreProvider)
							Expect(err).Should(HaveOccurred())
							Expect(newSecretModifiedTime.IsZero()).Should(BeTrue())
						})
					}
				})
				Context("credentials exist", func() {
					var lastCreationTime time.Time
					BeforeEach(func() {
						var err error
						lastCreationTime, err = createCredentialFilesInDirectory(credentialDirectory, config.CredentialFiles)
						Expect(err).ShouldNot(HaveOccurred())
						Expect(lastCreationTime.IsZero()).ShouldNot(BeTrue())
					})
					// unmodified credentials
					It("should return the latest creation time among the credential files", func() {
						newSecretModifiedTime, err := GetSnapstoreSecretModifiedTime(config.SnapstoreProvider)
						Expect(err).ShouldNot(HaveOccurred())
						Expect(newSecretModifiedTime.Equal(lastCreationTime)).Should(BeTrue())
					})
					// modified credentials
					for _, credentialFile := range config.CredentialFiles {
						credentialFile := credentialFile
						It(fmt.Sprintf("should return the modification time of the credential file %q", credentialFile), func() {
							err := modifyCredentialFileInDirectory(credentialDirectory, credentialFile)
							Expect(err).ShouldNot(HaveOccurred())
							newSecretModifiedTime, err := GetSnapstoreSecretModifiedTime(config.SnapstoreProvider)
							Expect(err).ShouldNot(HaveOccurred())
							Expect(newSecretModifiedTime.After(lastCreationTime)).Should(BeTrue())
						})
					}
				})
			})
		})
	}
})

var _ = Describe("Blob Service URL construction for Azure", func() {
	var credentials *container.SharedKeyCredential
	domain := "test.domain"
	BeforeEach(func() {
		var err error
		// test strings
		storageAccount, storageKey := "testAccountName", "dGVzdEFjY291bnRLZXk="
		credentials, err = container.NewSharedKeyCredential(storageAccount, storageKey)
		Expect(err).ShouldNot(HaveOccurred())
	})
	Context("when emulatorEnabled field is not set or set to false", func() {
		It("should return the default blob service URL with HTTPS scheme", func() {
			blobServiceURL, err := ConstructBlobServiceURL(credentials.AccountName(), domain, false)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(blobServiceURL).Should(Equal(fmt.Sprintf("https://%s.%s", credentials.AccountName(), domain)))
		})
	})
	Context("when emulatorEnabled field is set to true", func() {
		It("should return the Azurite blob service URL with HTTP scheme", func() {
			blobServiceURL, err := ConstructBlobServiceURL(credentials.AccountName(), domain, true)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(blobServiceURL).Should(Equal(fmt.Sprintf("http://%s/%s", domain, credentials.AccountName())))
		})
	})
})

var _ = Describe("Server Side Encryption Customer Managed Key for S3", func() {
	s3SnapstoreConfig := brtypes.SnapstoreConfig{
		Provider:  brtypes.SnapstoreProviderS3,
		Container: "etcd-test",
		Prefix:    "v2",
	}
	var credentialDirectory, credentialFilePath string
	BeforeEach(func() {
		credentialDirectory = GinkgoT().TempDir()
		credentialFilePath = filepath.Join(credentialDirectory, "credentials.json")
		GinkgoT().Setenv("AWS_APPLICATION_CREDENTIALS_JSON", credentialFilePath)
	})
	Context("when no SSE-C keys are provided", func() {
		It("should return the snapstore without errors", func() {
			// SSE-C fields not present
			err := os.WriteFile(credentialFilePath, []byte(`{
  "accessKeyID": "XXXXXXXXXXXXXXXXXXXX",
  "secretAccessKey": "XXXXXXXXXXXXXXXXXXXX",
  "region": "eu-west-1"
}`), os.ModePerm)
			Expect(err).ShouldNot(HaveOccurred())
			_, err = NewS3SnapStore(&s3SnapstoreConfig)
			Expect(err).ShouldNot(HaveOccurred())
		})
	})
	Context("when SSE-C is enabled", func() {
		It("should return an error if both fields for SSE-C are not provided", func() {
			// both SSE-C fields are not present
			// sseCustomerKey not present
			err := os.WriteFile(credentialFilePath, []byte(`{
  "accessKeyID": "XXXXXXXXXXXXXXXXXXXX",
  "secretAccessKey": "XXXXXXXXXXXXXXXXXXXX",
  "region": "eu-west-1",
  "sseCustomerAlgorithm": "AES256"
}`), os.ModePerm)
			Expect(err).ShouldNot(HaveOccurred())
			_, err = NewS3SnapStore(&s3SnapstoreConfig)
			Expect(err).Should(HaveOccurred())
			// sseCustomerAlgorithm not present
			err = os.WriteFile(credentialFilePath, []byte(`{
  "accessKeyID": "XXXXXXXXXXXXXXXXXXXX",
  "secretAccessKey": "XXXXXXXXXXXXXXXXXXXX",
  "region": "eu-west-1",
  "sseCustomerKey": "2b7e151628aed2a6abf7158809cf4f3c6afe5028f1959c27a11253edc6cf4f3c"
}`), os.ModePerm)
			Expect(err).ShouldNot(HaveOccurred())
			_, err = NewS3SnapStore(&s3SnapstoreConfig)
			Expect(err).Should(HaveOccurred())
		})
		It("should return the snapstore without errors if both fields are provided", func() {
			// both SSE-C fields are present
			err := os.WriteFile(credentialFilePath, []byte(`{
  "accessKeyID": "XXXXXXXXXXXXXXXXXXXX",
  "secretAccessKey": "XXXXXXXXXXXXXXXXXXXX",
  "region": "eu-west-1",
  "sseCustomerAlgorithm": "AES256",
  "sseCustomerKey": "2b7e151628aed2a6abf7158809cf4f3c6afe5028f1959c27a11253edc6cf4f3c"
}`), os.ModePerm)
			Expect(err).ShouldNot(HaveOccurred())
			_, err = NewS3SnapStore(&s3SnapstoreConfig)
			Expect(err).ShouldNot(HaveOccurred())
		})
	})
})

var _ = Describe("Get Bucket versioning status for S3 buckets", func() {
	var (
		awsS3Client *mockS3Client
		ctx         context.Context
	)

	BeforeEach(func() {
		awsS3Client = &mockS3Client{
			objects:          objectMap,
			prefix:           prefixV2,
			multiPartUploads: map[string]*[][]byte{},
		}
		ctx = context.TODO()
	})

	Context("S3 bucket with object lock enabled", func() {
		It("Should return enabled versioning status", func() {
			versioningStatus, err := awsS3Client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{
				Bucket: &s3ObjectLockedBucket,
			})
			Expect(err).ShouldNot(HaveOccurred())
			Expect(versioningStatus.Status).Should(Equal(s3types.BucketVersioningStatusEnabled))
		})
	})
	Context("S3 bucket with object lock not enabled", func() {
		It("Should return versioning status as nil", func() {
			versioningStatus, err := awsS3Client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{
				Bucket: &s3NonObjectLockedBucket,
			})
			Expect(err).ShouldNot(HaveOccurred())
			Expect(versioningStatus.Status).Should(BeEmpty())
		})
	})
})

var _ = Describe("Get Immutability time for S3 bucket", func() {
	awsS3Client := &mockS3Client{
		objects:          objectMap,
		prefix:           prefixV2,
		multiPartUploads: map[string]*[][]byte{},
	}
	var s3ObjectLockBucketButRulesNotDefined = "mock-s3ObjectLockBucketButRulesNotDefined"

	Context("S3 bucket with object lock enabled and object lock config defined", func() {
		It("Should return retention period", func() {
			snapStore := NewS3FromClient(s3ObjectLockedBucket, prefixV2, "/tmp", 5, brtypes.MinChunkSize, awsS3Client, SSECredentials{})
			isObjectLockEnabled, retentionPeriod, err := GetBucketImmutabilityTime(snapStore)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(retentionPeriod).Should(Equal(aws.Int32(2)))
			Expect(isObjectLockEnabled).Should(BeTrue())
		})
	})
	Context("S3 bucket with object lock enabled but object lock config is not defined", func() {
		It("Should return nil retention period", func() {
			snapStore := NewS3FromClient(s3ObjectLockBucketButRulesNotDefined, prefixV2, "/tmp", 5, brtypes.MinChunkSize, awsS3Client, SSECredentials{})
			isObjectLockEnabled, retentionPeriod, err := GetBucketImmutabilityTime(snapStore)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(retentionPeriod).Should(BeNil())
			Expect(isObjectLockEnabled).Should(BeTrue())
		})
	})
	Context("S3 bucket with object lock not enabled", func() {
		It("Should return an error", func() {
			snapStore := NewS3FromClient(s3NonObjectLockedBucket, prefixV2, "/tmp", 5, brtypes.MinChunkSize, awsS3Client, SSECredentials{})
			isObjectLockEnabled, retentionPeriod, err := GetBucketImmutabilityTime(snapStore)
			Expect(err).Should(HaveOccurred())
			Expect(retentionPeriod).Should(BeNil())
			Expect(isObjectLockEnabled).Should(BeFalse())
		})
	})
})

var _ = Describe("S3 snapshot object is marked to be ignored/skipped", func() {
	awsS3Client := &mockS3Client{
		objects: objectMap,
		prefix:  prefixV2,
	}

	mockS3BucketName := "mock-s3ObjectLockedBucket"
	mockSnapshotKey := "mock/v2/Full-000000xx-000000yy-yyxxzz.gz"

	Context("Snapshot object is correctly tagged to be ignored", func() {
		It("Should return true", func() {
			isMarkedToIgnore := IsSnapshotMarkedToBeIgnored(awsS3Client, mockS3BucketName, mockSnapshotKey, "mockVersion1")
			Expect(isMarkedToIgnore).Should(BeTrue())
		})
	})

	Context("Snapshot object is incorrectly tagged to be ignored", func() {
		It("Should return false", func() {
			isMarkedToIgnore := IsSnapshotMarkedToBeIgnored(awsS3Client, mockS3BucketName, mockSnapshotKey, "mockVersion2")
			Expect(isMarkedToIgnore).Should(BeFalse())
		})
	})

	Context("No tag is present for given snapshot", func() {
		It("Should return false", func() {
			isMarkedToIgnore := IsSnapshotMarkedToBeIgnored(awsS3Client, mockS3BucketName, mockSnapshotKey, "mockVersion3")
			Expect(isMarkedToIgnore).Should(BeFalse())
		})
	})

	Context("S3's GetObjectTagging API call fails", func() {
		It("Should return false", func() {
			isMarkedToIgnore := IsSnapshotMarkedToBeIgnored(awsS3Client, "mock-s3Bucket", mockSnapshotKey, "mockVersion1")
			Expect(isMarkedToIgnore).Should(BeFalse())
		})
	})
})

// createCredentialFilesInDirectory creates access credential files in the
// specified directory and returns the timestamp of the last modified file.
func createCredentialFilesInDirectory(directory string, filenames []string) (time.Time, error) {
	var fullFilePath string
	for _, filename := range filenames {
		fullFilePath = filepath.Join(directory, filename)
		// creates and writes content to the file
		err := os.WriteFile(fullFilePath, []byte("INITIAL CONTENT"), os.ModePerm)
		if err != nil {
			return time.Time{}, err
		}
	}
	// return the modification time (creation time here) of the last created file
	lastFileInfo, err := os.Stat(fullFilePath)
	if err != nil {
		return time.Time{}, err
	}
	return lastFileInfo.ModTime(), nil
}

// modifyCredentialFileInDirectory modifies a specific credential file within the given directory.
func modifyCredentialFileInDirectory(credentialDirectory, credentialFile string) error {
	// sleep before the file is modified, file modification timestamp does not change otherwise on concourse
	time.Sleep(time.Millisecond * 100)
	credentialFilePath := filepath.Join(credentialDirectory, credentialFile)
	return os.WriteFile(credentialFilePath, []byte("MODIFIED CONTENT"), os.ModePerm)
}

// generates a unique string that would be the contents of a snapshot
func generateContentsForSnapshot(snapshot *brtypes.Snapshot) string {
	return fmt.Sprintf("%s%d%d%s%s%t%s%s%t", snapshot.Kind, snapshot.StartRevision, snapshot.LastRevision,
		snapshot.SnapDir, snapshot.SnapName, snapshot.IsChunk, snapshot.Prefix, snapshot.CompressionSuffix, snapshot.IsFinal)
}

// Changes the contents of the objectMap according the to the snapshots and the provider and returns the number of snapshots added
func setObjectMap(provider string, snapshots brtypes.SnapList) int {
	var numberSnapshotsAdded int
	for _, snapshot := range snapshots {
		if provider == brtypes.SnapstoreProviderSwift {
			// contents of the snapshot split into segments
			generatedContents := generateContentsForSnapshot(snapshot)
			segmentBytes01 := []byte(generatedContents[:len(generatedContents)/2])
			segmentBytes02 := []byte(generatedContents[len(generatedContents)/2:])
			// segment objects
			objectMap[path.Join(snapshot.Prefix, snapshot.SnapDir, snapshot.SnapName, "0000000001")] = &segmentBytes01
			objectMap[path.Join(snapshot.Prefix, snapshot.SnapDir, snapshot.SnapName, "0000000002")] = &segmentBytes02
			// manifest object
			objectMap[path.Join(snapshot.Prefix, snapshot.SnapDir, snapshot.SnapName)] = &[]byte{}
		} else {
			expectedValue := []byte(generateContentsForSnapshot(snapshot))
			objectMap[path.Join(snapshot.Prefix, snapshot.SnapDir, snapshot.SnapName)] = &expectedValue
		}
		numberSnapshotsAdded++
	}
	return numberSnapshotsAdded
}

func resetObjectMap() {
	for k := range objectMap {
		delete(objectMap, k)
	}
}

func parseObjectNamefromURL(u *url.URL) string {
	path := u.Path
	if strings.HasPrefix(path, fmt.Sprintf("/%s", bucket)) {
		splits := strings.SplitAfterN(path, fmt.Sprintf("/%s", bucket), 2)
		if len(splits[1]) == 0 {
			return ""
		}
		return splits[1][1:]
	} else {
		logrus.Errorf("path should start with /%s: but received %s", bucket, u.String())
		return ""
	}
}
