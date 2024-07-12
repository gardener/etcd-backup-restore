// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore_test

import (
	"bytes"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	. "github.com/gardener/etcd-backup-restore/pkg/snapstore"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	fake "github.com/gophercloud/gophercloud/testhelper/client"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
)

const (
	prefixV1 string = "v1"
	prefixV2 string = "v2"
)

var (
	bucket    string = "mock-bucket"
	objectMap        = map[string]*[]byte{}
)

// testSnapStore embedds brtypes.Snapstore and contains the number of
// objects that are stored per snapshot for a given storage provider
type testSnapStore struct {
	brtypes.SnapStore
	objectCountPerSnapshot int
}

var _ = Describe("Save, List, Fetch, Delete from mock snapstore", func() {
	var (
		snap1      brtypes.Snapshot
		snap2      brtypes.Snapshot
		snap3      brtypes.Snapshot
		snap4      brtypes.Snapshot
		snap5      brtypes.Snapshot
		snapstores map[string]testSnapStore
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

		snapstores = map[string]testSnapStore{
			"s3": {
				SnapStore: NewS3FromClient(bucket, prefixV2, "/tmp", 5, brtypes.MinChunkSize, &mockS3Client{
					objects:          objectMap,
					prefix:           prefixV2,
					multiPartUploads: map[string]*[][]byte{},
				}, SSECredentials{}),
				objectCountPerSnapshot: 1,
			},
			"swift": {
				SnapStore:              NewSwiftSnapstoreFromClient(bucket, prefixV2, "/tmp", 5, brtypes.MinChunkSize, fake.ServiceClient()),
				objectCountPerSnapshot: 3,
			},
			"ABS": {
				SnapStore: NewABSSnapStoreFromClient(bucket, prefixV2, "/tmp", 5, brtypes.MinChunkSize, &fakeABSContainerClient{
					objects:     objectMap,
					prefix:      prefixV2,
					blobClients: make(map[string]*fakeBlockBlobClient),
				}),
				objectCountPerSnapshot: 1,
			},
			"GCS": {
				SnapStore: NewGCSSnapStoreFromClient(bucket, prefixV2, "/tmp", 5, brtypes.MinChunkSize, "", &mockGCSClient{
					objects: objectMap,
					prefix:  prefixV2,
				}),
				objectCountPerSnapshot: 1,
			},
			"OSS": {
				SnapStore: NewOSSFromBucket(prefixV2, "/tmp", 5, brtypes.MinChunkSize, &mockOSSBucket{
					objects:          objectMap,
					prefix:           prefixV2,
					multiPartUploads: map[string]*[][]byte{},
					bucketName:       bucket,
				}),
				objectCountPerSnapshot: 1,
			},
			"ECS": {
				SnapStore: NewS3FromClient(bucket, prefixV2, "/tmp", 5, brtypes.MinChunkSize, &mockS3Client{
					objects:          objectMap,
					prefix:           prefixV2,
					multiPartUploads: map[string]*[][]byte{},
				}, SSECredentials{}),
				objectCountPerSnapshot: 1,
			},
			"OCS": {
				SnapStore: NewS3FromClient(bucket, prefixV2, "/tmp", 5, brtypes.MinChunkSize, &mockS3Client{
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
				snapList, err := snapStore.List()
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
				snapList, err = snapStore.List()
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
				snapList, err := snapStore.List()
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
				snapList, err := snapStore.List()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(snapList.Len()).To(Equal(numberSnapshotsInObjectMap * snapStore.objectCountPerSnapshot))
				Expect(snapList[0].SnapName).To(Equal(snap4.SnapName))
				Expect(snapList[secondSnapshotIndex].SnapName).To(Equal(snap5.SnapName))

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
				snapList, err = snapStore.List()
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
	Provider          string
	EnvVariable       string
	SnapstoreProvider string
	CredentialType    string // "file" or "directory"
	CredentialFiles   []string
}

var credentialTestConfigs = []CredentialTestConfig{
	// AWS
	{
		Provider:          "AWS",
		EnvVariable:       "AWS_APPLICATION_CREDENTIALS",
		SnapstoreProvider: brtypes.SnapstoreProviderS3,
		CredentialType:    "directory",
		CredentialFiles:   []string{"accessKeyID", "region", "secretAccessKey"},
	},
	{
		Provider:          "AWS",
		EnvVariable:       "AWS_APPLICATION_CREDENTIALS_JSON",
		SnapstoreProvider: brtypes.SnapstoreProviderS3,
		CredentialType:    "file",
		CredentialFiles:   []string{"credentials.json"},
	},
	// Azure
	{
		Provider:          "ABS",
		EnvVariable:       "AZURE_APPLICATION_CREDENTIALS",
		SnapstoreProvider: brtypes.SnapstoreProviderABS,
		CredentialType:    "directory",
		CredentialFiles:   []string{"storageAccount", "storageKey"},
	},
	{
		Provider:          "ABS",
		EnvVariable:       "AZURE_APPLICATION_CREDENTIALS_JSON",
		SnapstoreProvider: brtypes.SnapstoreProviderABS,
		CredentialType:    "file",
		CredentialFiles:   []string{"credentials.json"},
	},
	// GCS
	{
		Provider:          "GCS",
		EnvVariable:       "GOOGLE_APPLICATION_CREDENTIALS",
		SnapstoreProvider: brtypes.SnapstoreProviderGCS,
		CredentialType:    "file",
		CredentialFiles:   []string{"credentials.json"},
	},
	// Swift V3ApplicationCredentials
	{
		Provider:          "Swift",
		EnvVariable:       "OPENSTACK_APPLICATION_CREDENTIALS",
		SnapstoreProvider: brtypes.SnapstoreProviderSwift,
		CredentialType:    "directory",
		CredentialFiles:   []string{"authURL", "tenantName", "domainName", "applicationCredentialID", "applicationCredentialName", "applicationCredentialSecret"},
	},
	// Swift Password
	{
		Provider:          "Swift",
		EnvVariable:       "OPENSTACK_APPLICATION_CREDENTIALS",
		SnapstoreProvider: brtypes.SnapstoreProviderSwift,
		CredentialType:    "directory",
		CredentialFiles:   []string{"authURL", "tenantName", "domainName", "username", "password"},
	},
	// Swift JSON
	{
		Provider:          "Swift",
		EnvVariable:       "OPENSTACK_APPLICATION_CREDENTIALS_JSON",
		SnapstoreProvider: brtypes.SnapstoreProviderSwift,
		CredentialType:    "file",
		CredentialFiles:   []string{"credentials.json"},
	},
	// OSS
	{
		Provider:          "OSS",
		EnvVariable:       "ALICLOUD_APPLICATION_CREDENTIALS",
		SnapstoreProvider: brtypes.SnapstoreProviderOSS,
		CredentialType:    "directory",
		CredentialFiles:   []string{"accessKeyID", "accessKeySecret", "storageEndpoint"},
	},
	{
		Provider:          "OSS",
		EnvVariable:       "ALICLOUD_APPLICATION_CREDENTIALS_JSON",
		SnapstoreProvider: brtypes.SnapstoreProviderOSS,
		CredentialType:    "file",
		CredentialFiles:   []string{"credentials.json"},
	},
	// OCS
	{
		Provider:          "OCS",
		EnvVariable:       "OPENSHIFT_APPLICATION_CREDENTIALS",
		SnapstoreProvider: brtypes.SnapstoreProviderOCS,
		CredentialType:    "directory",
		CredentialFiles:   []string{"accessKeyID", "region", "endpoint", "secretAccessKey"},
	},
	{
		Provider:          "OCS",
		EnvVariable:       "OPENSHIFT_APPLICATION_CREDENTIALS_JSON",
		SnapstoreProvider: brtypes.SnapstoreProviderOCS,
		CredentialType:    "file",
		CredentialFiles:   []string{"credentials.json"},
	},
}

var _ = Describe("Dynamic access credential rotation test for each provider", func() {
	for _, config := range credentialTestConfigs {
		config := config
		Describe(fmt.Sprintf("testing secret modification for %q with %q", config.Provider, config.EnvVariable), func() {
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
	const accountName string = "testAccountName"
	Context(fmt.Sprintf("when the environment variable %q is not set", EnvAzureEmulatorEnabled), func() {
		It("should return the default blob service URL", func() {
			absURI, err := ConstructABSURI(accountName)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(absURI).Should(Equal(fmt.Sprintf("https://%s.%s", accountName, brtypes.AzureBlobStorageHostName)))
		})
	})
	Context(fmt.Sprintf("when the environment variable %q is set", EnvAzureEmulatorEnabled), func() {
		Context("to values which are not \"true\"", func() {
			It("should error when the environment variable is not \"true\" or \"false\"", func() {
				GinkgoT().Setenv(EnvAzureEmulatorEnabled, "")
				_, err := ConstructABSURI(accountName)
				Expect(err).Should(HaveOccurred())
			})
			It("should return the default blob service URL when the environment variable is set to \"false\"", func() {
				GinkgoT().Setenv(EnvAzureEmulatorEnabled, "false")
				absURI, err := ConstructABSURI(accountName)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(absURI).Should(Equal(fmt.Sprintf("https://%s.%s", accountName, brtypes.AzureBlobStorageHostName)))
			})
		})
		Context("to \"true\"", func() {
			const endpoint string = "http://localhost:12345"
			BeforeEach(func() {
				GinkgoT().Setenv(EnvAzureEmulatorEnabled, "true")
			})
			It(fmt.Sprintf("should error when the %q environment variable is not set", AzuriteEndpoint), func() {
				_, err := ConstructABSURI(accountName)
				Expect(err).Should(HaveOccurred())
			})
			It(fmt.Sprintf("should return the Azurite blob service URL when the %q environment variable is set to %q", AzuriteEndpoint, endpoint), func() {
				GinkgoT().Setenv(AzuriteEndpoint, endpoint)
				absURI, err := ConstructABSURI(accountName)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(absURI).Should(Equal(fmt.Sprintf("%s/%s", endpoint, accountName)))
			})
		})
	})
})

var _ = Describe("Server Side Encryption Customer Managed Key for S3", func() {
	s3SnapstoreConfig := brtypes.SnapstoreConfig{
		Provider:  "S3",
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
		if provider == "swift" {
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
