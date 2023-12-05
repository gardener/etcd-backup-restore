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

package snapstore_test

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
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

var (
	bucket    string = "mock-bucket"
	objectMap        = map[string]*[]byte{}
	prefixV1  string = "v1"
	prefixV2  string = "v2"
)

var _ = Describe("Save, List, Fetch, Delete from mock snapstore", func() {
	var (
		snap1        brtypes.Snapshot
		snap2        brtypes.Snapshot
		snap3        brtypes.Snapshot
		snap4        brtypes.Snapshot
		snap5        brtypes.Snapshot
		expectedVal1 []byte
		expectedVal2 []byte
		//expectedVal3 []byte
		expectedVal4 []byte
		expectedVal5 []byte
		snapstores   map[string]brtypes.SnapStore
	)

	BeforeEach(func() {
		now := time.Now().Unix()
		snap1 = brtypes.Snapshot{
			CreatedOn:     time.Unix(now, 0).UTC(),
			StartRevision: 0,
			LastRevision:  2088,
			Kind:          brtypes.SnapshotKindFull,
		}
		snap2 = brtypes.Snapshot{
			CreatedOn:     time.Unix(now+100, 0).UTC(),
			StartRevision: 0,
			LastRevision:  1988,
			Kind:          brtypes.SnapshotKindFull,
		}
		snap3 = brtypes.Snapshot{
			CreatedOn:     time.Unix(now+200, 0).UTC(),
			StartRevision: 0,
			LastRevision:  1958,
			Kind:          brtypes.SnapshotKindFull,
		}
		snap4 = brtypes.Snapshot{
			CreatedOn:     time.Unix(now+300, 0).UTC(),
			StartRevision: 0,
			LastRevision:  3058,
			Kind:          brtypes.SnapshotKindFull,
		}
		snap5 = brtypes.Snapshot{
			CreatedOn:     time.Unix(now+400, 0).UTC(),
			StartRevision: 3058,
			LastRevision:  3088,
			Kind:          brtypes.SnapshotKindDelta,
		}
		snap1.GenerateSnapshotName()
		snap1.GenerateSnapshotDirectory()
		snap2.GenerateSnapshotName()
		snap2.GenerateSnapshotDirectory()
		snap3.GenerateSnapshotName()
		snap3.GenerateSnapshotDirectory()

		snap4.GenerateSnapshotName()
		snap5.GenerateSnapshotName()

		expectedVal1 = []byte("value1")
		expectedVal2 = []byte("value2")
		//expectedVal3 = []byte("value3")
		expectedVal4 = []byte("value4")
		expectedVal5 = []byte("value5")

		snapstores = map[string]brtypes.SnapStore{
			"s3": NewS3FromClient(bucket, prefixV2, "/tmp", 5, brtypes.MinChunkSize, &mockS3Client{
				objects:          objectMap,
				prefix:           prefixV2,
				multiPartUploads: map[string]*[][]byte{},
			}),
			"swift": NewSwiftSnapstoreFromClient(bucket, prefixV2, "/tmp", 5, brtypes.MinChunkSize, fake.ServiceClient()),
			"ABS":   newFakeABSSnapstore(),
			"GCS": NewGCSSnapStoreFromClient(bucket, prefixV2, "/tmp", 5, brtypes.MinChunkSize, &mockGCSClient{
				objects: objectMap,
				prefix:  prefixV2,
			}),
			"OSS": NewOSSFromBucket(prefixV2, "/tmp", 5, brtypes.MinChunkSize, &mockOSSBucket{
				objects:          objectMap,
				prefix:           prefixV2,
				multiPartUploads: map[string]*[][]byte{},
				bucketName:       bucket,
			}),
			"ECS": NewS3FromClient(bucket, prefixV2, "/tmp", 5, brtypes.MinChunkSize, &mockS3Client{
				objects:          objectMap,
				prefix:           prefixV2,
				multiPartUploads: map[string]*[][]byte{},
			}),
			"OCS": NewS3FromClient(bucket, prefixV2, "/tmp", 5, brtypes.MinChunkSize, &mockS3Client{
				objects:          objectMap,
				prefix:           prefixV2,
				multiPartUploads: map[string]*[][]byte{},
			}),
		}
	})
	AfterEach(func() {
		resetObjectMap()
	})

	Describe("When Only v1 is present", func() {
		It("When Only v1 is present", func() {
			for key, snapStore := range snapstores {
				// Create store for mock tests
				resetObjectMap()
				objectMap[path.Join(prefixV1, snap1.SnapDir, snap1.SnapName)] = &expectedVal1
				objectMap[path.Join(prefixV1, snap2.SnapDir, snap2.SnapName)] = &expectedVal2

				logrus.Infof("Running mock tests for %s when only v1 is present", key)
				// List snap1 and snap2
				snapList, err := snapStore.List()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(snapList.Len()).To(Equal(2))
				Expect(snapList[0].SnapName).To(Equal(snap2.SnapName))
				// Fetch snap2
				rc, err := snapStore.Fetch(*snapList[1])
				Expect(err).ShouldNot(HaveOccurred())
				defer rc.Close()
				buf := new(bytes.Buffer)
				_, err = io.Copy(buf, rc)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(buf.Bytes()).To(Equal(expectedVal1))
				// Delete snap2
				prevLen := len(objectMap)
				err = snapStore.Delete(*snapList[1])
				Expect(err).ShouldNot(HaveOccurred())
				snapList, err = snapStore.List()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(snapList.Len()).To(Equal(prevLen - 1))
				// reset the objectMap
				resetObjectMap()
				dummyData := make([]byte, 6*1024*1024)
				// Save a new snapshot 'snap3'
				err = snapStore.Save(snap3, io.NopCloser(bytes.NewReader(dummyData)))
				Expect(err).ShouldNot(HaveOccurred())
				Expect(len(objectMap)).Should(BeNumerically(">=", 1))
			}
		})
	})

	Describe("When both v1 and v2 are present", func() {
		It("When both v1 and v2 are present", func() {
			for key, snapStore := range snapstores {
				// Create store for mock tests
				resetObjectMap()
				objectMap[path.Join(prefixV1, snap1.SnapDir, snap1.SnapName)] = &expectedVal1
				objectMap[path.Join(prefixV2, snap4.SnapDir, snap4.SnapName)] = &expectedVal4
				objectMap[path.Join(prefixV2, snap5.SnapDir, snap5.SnapName)] = &expectedVal5

				logrus.Infof("Running mock tests for %s when both v1 and v2 are present", key)

				// List snap1, snap4, snap5
				snapList, err := snapStore.List()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(snapList.Len()).To(Equal(3))
				Expect(snapList[0].SnapName).To(Equal(snap1.SnapName))

				// Fetch snap1 and snap4
				rc, err := snapStore.Fetch(*snapList[0])
				Expect(err).ShouldNot(HaveOccurred())
				defer rc.Close()
				buf := new(bytes.Buffer)
				_, err = io.Copy(buf, rc)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(buf.Bytes()).To(Equal(expectedVal1))

				rc, err = snapStore.Fetch(*snapList[1])
				Expect(err).ShouldNot(HaveOccurred())
				defer rc.Close()
				buf = new(bytes.Buffer)
				_, err = io.Copy(buf, rc)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(buf.Bytes()).To(Equal(expectedVal4))

				// Delete snap1 and snap5
				prevLen := len(objectMap)
				err = snapStore.Delete(*snapList[0])
				Expect(err).ShouldNot(HaveOccurred())
				Expect(len(objectMap)).To(Equal(prevLen - 1))
				prevLen = len(objectMap)
				err = snapStore.Delete(*snapList[2])
				Expect(err).ShouldNot(HaveOccurred())
				Expect(len(objectMap)).To(Equal(prevLen - 1))
				// reset the objectMap
				resetObjectMap()
				// Save a new snapshot 'snap1'
				dummyData := make([]byte, 6*1024*1024)
				err = snapStore.Save(snap1, io.NopCloser(bytes.NewReader(dummyData)))
				Expect(err).ShouldNot(HaveOccurred())
				Expect(len(objectMap)).Should(BeNumerically(">=", 1))

				// Save another new snapshot 'snap4'
				prevLen = len(objectMap)
				dummyData = make([]byte, 6*1024*1024)
				err = snapStore.Save(snap4, io.NopCloser(bytes.NewReader(dummyData)))
				Expect(err).ShouldNot(HaveOccurred())
				Expect(len(objectMap)).Should(BeNumerically(">=", prevLen+1))
			}
		})
	})

	Describe("When Only v2 is present", func() {
		It("When Only v2 is present", func() {
			for key, snapStore := range snapstores {
				// Create store for mock tests
				resetObjectMap()
				objectMap[path.Join(prefixV2, snap4.SnapDir, snap4.SnapName)] = &expectedVal4
				objectMap[path.Join(prefixV2, snap5.SnapDir, snap5.SnapName)] = &expectedVal5

				logrus.Infof("Running mock tests for %s when only v2 is present", key)
				// List snap4 and snap5
				snapList, err := snapStore.List()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(snapList.Len()).To(Equal(2))
				Expect(snapList[0].SnapName).To(Equal(snap4.SnapName))
				Expect(snapList[1].SnapName).To(Equal(snap5.SnapName))
				// Fetch snap5
				rc, err := snapStore.Fetch(*snapList[1])
				Expect(err).ShouldNot(HaveOccurred())
				defer rc.Close()
				buf := new(bytes.Buffer)
				_, err = io.Copy(buf, rc)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(buf.Bytes()).To(Equal(expectedVal5))
				// Delete snap5
				prevLen := len(objectMap)
				err = snapStore.Delete(*snapList[1])
				Expect(err).ShouldNot(HaveOccurred())
				snapList, err = snapStore.List()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(snapList.Len()).To(Equal(prevLen - 1))
				// Reset the objectMap
				resetObjectMap()
				// Save a new snapshot 'snap4'
				dummyData := make([]byte, 6*1024*1024)
				err = snapStore.Save(snap4, io.NopCloser(bytes.NewReader(dummyData)))
				Expect(err).ShouldNot(HaveOccurred())
				Expect(len(objectMap)).Should(BeNumerically(">=", 1))
			}
		})
	})
})

// directory where the credential files created are stored
const (
	credentialDirectory = "../../test/credential"
)

var (
	// environment variables for each provider
	providers = []string{
		"AWS_APPLICATION_CREDENTIALS",
		"AZURE_APPLICATION_CREDENTIALS",
		"OPENSTACK_APPLICATION_CREDENTIALS", // V3ApplicationCredentials
		"OPENSTACK_APPLICATION_CREDENTIALS", // Password
		"ALICLOUD_APPLICATION_CREDENTIALS",
		"OPENSHIFT_APPLICATION_CREDENTIALS",
	}
	credentialFilesForProviders = [][]string{
		{"accessKeyID", "region", "secretAccessKey"},
		{"storageAccount", "storageKey"},
		{"authURL", "tenantName", "domainName", "applicationCredentialID", "applicationCredentialName", "applicationCredentialSecret"},
		{"authURL", "tenantName", "domainName", "username", "password"},
		{"accessKeyID", "accessKeySecret", "storageEndpoint"},
		{"accessKeyID", "region", "endpoint", "secretAccessKey"},
	}
	providerToSnapstoreProviderMap = map[string]string{
		"AWS_APPLICATION_CREDENTIALS":       brtypes.SnapstoreProviderS3,
		"AZURE_APPLICATION_CREDENTIALS":     brtypes.SnapstoreProviderABS,
		"OPENSTACK_APPLICATION_CREDENTIALS": brtypes.SnapstoreProviderSwift,
		"ALICLOUD_APPLICATION_CREDENTIALS":  brtypes.SnapstoreProviderOSS,
		"OPENSHIFT_APPLICATION_CREDENTIALS": brtypes.SnapstoreProviderOCS,
	}
	providerToSnapstoreProviderJSONMap = map[string]string{
		"AWS_APPLICATION_CREDENTIALS_JSON":       brtypes.SnapstoreProviderS3,
		"GOOGLE_APPLICATION_CREDENTIALS":         brtypes.SnapstoreProviderGCS,
		"AZURE_APPLICATION_CREDENTIALS_JSON":     brtypes.SnapstoreProviderABS,
		"OPENSTACK_APPLICATION_CREDENTIALS_JSON": brtypes.SnapstoreProviderSwift,
		"ALICLOUD_APPLICATION_CREDENTIALS_JSON":  brtypes.SnapstoreProviderOSS,
		"OPENSHIFT_APPLICATION_CREDENTIALS_JSON": brtypes.SnapstoreProviderOCS,
	}
)

var _ = FDescribe("Testing the file modification time for a file", func() {
	It("The file should return a new modification time", func() {
		filename := "temporary"

		err := os.WriteFile(filename, []byte("INITIAL CONTENT"), os.ModePerm)
		Expect(err).ShouldNot(HaveOccurred())
		file, err := os.Open(filename)
		Expect(err).ShouldNot(HaveOccurred())
		fileInfo, err := file.Stat()
		Expect(err).ShouldNot(HaveOccurred())
		timeCreation := fileInfo.ModTime()
		err = file.Close()
		Expect(err).ShouldNot(HaveOccurred())
		fmt.Println("The file was created at:", timeCreation)
		bytesRead, err := os.ReadFile(filename)
		Expect(err).ShouldNot(HaveOccurred())
		fmt.Println("The file contents are:", string(bytesRead))

		err = os.WriteFile(filename, []byte("FINAL CONTENT"), os.ModePerm)
		Expect(err).ShouldNot(HaveOccurred())
		file, err = os.Open(filename)
		Expect(err).ShouldNot(HaveOccurred())
		fileInfo, err = file.Stat()
		Expect(err).ShouldNot(HaveOccurred())
		timeModification := fileInfo.ModTime()
		err = file.Close()
		Expect(err).ShouldNot(HaveOccurred())
		fmt.Println("The file was modified at:", timeModification)
		bytesRead, err = os.ReadFile(filename)
		Expect(err).ShouldNot(HaveOccurred())
		fmt.Println("The file contents are:", string(bytesRead))

		Expect(timeModification.After(timeCreation)).Should(Equal(true))
		os.Remove(filename)
	})
})

var _ = Describe("Dynamic access credential rotation for each provider", func() {
	// Credentials in a directory
	for providerIndex, provider := range providers {
		// the value is to be used in the closure capture
		providerIndex := providerIndex
		provider := provider
		credentialFileNames := credentialFilesForProviders[providerIndex]
		Describe("Testing secret modification time for each provider (directory): "+providerToSnapstoreProviderMap[provider], func() {
			Context("No environment variables are present for the credentials", func() {
				It("Should return error", func() {
					newSecretModifiedTime, err := GetSnapstoreSecretModifiedTime(providerToSnapstoreProviderMap[provider])
					Expect(err).Should(HaveOccurred())
					Expect(newSecretModifiedTime.IsZero()).Should(Equal(true))
				})
			})
			Describe("Environment variable set, points to a directory", func() {
				BeforeEach(func() {
					os.Setenv(provider, credentialDirectory)
				})
				Context("Directory does not exist", func() {
					It("Should return error", func() {
						newSecretModifiedTime, err := GetSnapstoreSecretModifiedTime(providerToSnapstoreProviderMap[provider])
						Expect(err).Should(HaveOccurred())
						Expect(newSecretModifiedTime.IsZero()).Should(Equal(true))
					})
				})
				Describe("Directory exists", func() {
					BeforeEach(func() {
						err := os.MkdirAll(credentialDirectory, os.ModePerm)
						Expect(err).ShouldNot(HaveOccurred())
						os.Setenv(provider, credentialDirectory)
					})
					Context("Credential files don't exist", func() {
						It("Should return error", func() {
							newSecretModifiedTime, err := GetSnapstoreSecretModifiedTime(providerToSnapstoreProviderMap[provider])
							Expect(err).Should(HaveOccurred())
							Expect(newSecretModifiedTime.IsZero()).Should(Equal(true))
						})
					})
					// same suite to be tested multiple times for randomly chosen access credential files
					for repeat := 0; repeat < 5; repeat++ {
						Context("Some credentials files don't exist", func() {
							var credentialFullPaths []string
							BeforeEach(func() {
								randomFileSkipIndex := rand.Intn(len(credentialFileNames))
								for i := range credentialFileNames {
									// randomly chosen files are not created
									if i == randomFileSkipIndex {
										continue
									}
									credentialFullPaths = append(credentialFullPaths, filepath.Join(credentialDirectory, credentialFileNames[i]))
								}
								createCredentialFiles(credentialFullPaths)
							})
							It("Should return error", func() {
								newSecretModifiedTime, err := GetSnapstoreSecretModifiedTime(providerToSnapstoreProviderMap[provider])
								Expect(err).Should(HaveOccurred())
								Expect(newSecretModifiedTime.IsZero()).Should(Equal(true))
							})
						})
					}
					Describe("Credential files exist", func() {
						// var credentialFileCreationTimes []time.Time
						var lastCreationTimeForFile time.Time
						var credentialFileFullPaths []string
						BeforeEach(func() {
							for i := range credentialFileNames {
								credentialFileFullPaths = append(credentialFileFullPaths, filepath.Join(credentialDirectory, credentialFileNames[i]))
							}
							lastCreationTimeForFile = createCredentialFiles(credentialFileFullPaths)
						})
						Context("Files not modified", func() {
							It("Should return creation timestamp", func() {
								newSecretModifiedTime, err := GetSnapstoreSecretModifiedTime(providerToSnapstoreProviderMap[provider])
								Expect(err).ShouldNot(HaveOccurred())
								Expect(newSecretModifiedTime.Equal(lastCreationTimeForFile)).Should(Equal(true))
							})
						})
						// same suite to be tested multiple times for randomly chosen access credential files
						var fileIndex int
						for repeat := 0; repeat < 5; repeat++ {
							Context("Files have been modified", func() {
								fileIndex := fileIndex
								BeforeEach(func() {
									// random file should be modified
									fileIndex = rand.Intn(len(credentialFileFullPaths))
									fmt.Println("Testing for the provider:", providerToSnapstoreProviderMap[provider])
									bytesRead, err := os.ReadFile(credentialFileFullPaths[fileIndex])
									if err != nil {
										fmt.Println("Error while reading the unmodified credential file:", err)
									}
									fmt.Printf("The original contents of the file are: \"%s\"\n", string(bytesRead))
									err = modifyCredentialFile(fileIndex, credentialFileFullPaths)
									Expect(err).ShouldNot(HaveOccurred())
								})
								It("Should return modification timestamp", func() {
									newSecretModifiedTime, err := GetSnapstoreSecretModifiedTime(providerToSnapstoreProviderMap[provider])
									Expect(err).ShouldNot(HaveOccurred())
									fmt.Println("Creation time for the credential file:\t\t", lastCreationTimeForFile)
									fmt.Println("Modification time for the credential file:\t", newSecretModifiedTime)
									// printing contents of the file
									bytesRead, err := os.ReadFile(credentialFileFullPaths[fileIndex])
									if err != nil {
										fmt.Println("Error while reading the modified credential file:", err)
									}
									fmt.Printf("The new contents of the file are: \"%s\"\n", string(bytesRead))
									Expect(newSecretModifiedTime.After(lastCreationTimeForFile)).Should(Equal(true))
								})
							})
						}
					})
					AfterEach(func() {
						err := os.RemoveAll(credentialDirectory)
						Expect(err).ShouldNot(HaveOccurred())
					})
				})
				AfterEach(func() {
					os.Unsetenv(provider)
				})
			})

		})
	}
	// Credentials in JSON
	for provider, snapstoreProvider := range providerToSnapstoreProviderJSONMap {
		provider := provider
		snapstoreProvider := snapstoreProvider
		Describe("Testing secret modification time for each provider (JSON): "+snapstoreProvider, func() {
			Context("No environment variables are present for the credentials", func() {
				It("Should return error", func() {
					newSecretModifiedTime, err := GetSnapstoreSecretModifiedTime(snapstoreProvider)
					Expect(err).Should(HaveOccurred())
					Expect(newSecretModifiedTime.IsZero()).Should(Equal(true))
				})
			})
			Describe("Environment variable set, points to a file", func() {
				BeforeEach(func() {
					os.Setenv(provider, credentialDirectory)
				})
				Context("File does not exist", func() {
					It("Should return error", func() {
						newSecretModifiedTime, err := GetSnapstoreSecretModifiedTime(snapstoreProvider)
						Expect(err).Should(HaveOccurred())
						Expect(newSecretModifiedTime.IsZero()).Should(Equal(true))
					})
				})
				Describe("File exists", func() {
					var lastCreationTimeForFile time.Time
					var credentialFileFullPaths []string
					BeforeEach(func() {
						err := os.MkdirAll(credentialDirectory, os.ModePerm)
						Expect(err).ShouldNot(HaveOccurred())
						credentialFileFullPaths = append(credentialFileFullPaths, filepath.Join(credentialDirectory, "credentials.json"))
						lastCreationTimeForFile = createCredentialFiles(credentialFileFullPaths)
						os.Setenv(provider, credentialFileFullPaths[0])
						Expect(credentialFileFullPaths[0]).NotTo(Equal(""))
					})
					Context("File not modified", func() {
						It("Should return the creation timestamp", func() {
							newSecretModifiedTime, err := GetSnapstoreSecretModifiedTime(snapstoreProvider)
							Expect(err).ShouldNot(HaveOccurred())
							Expect(newSecretModifiedTime.Equal(lastCreationTimeForFile)).Should(Equal(true))
						})
					})
					Context("File modified", func() {
						BeforeEach(func() {
							fmt.Println("Testing for the provider (JSON):", snapstoreProvider)
							bytesRead, err := os.ReadFile(credentialFileFullPaths[0])
							if err != nil {
								fmt.Println("Error while reading the unmodified JSON credential file:", err)
							}
							fmt.Printf("The original contents of the JSON file are:\"%s\"\n", string(bytesRead))
							err = modifyCredentialFile(0, credentialFileFullPaths)
							Expect(err).ShouldNot(HaveOccurred())
						})
						It("Should return the modification timestamp", func() {
							newSecretModifiedTime, err := GetSnapstoreSecretModifiedTime(snapstoreProvider)
							Expect(err).ShouldNot(HaveOccurred())
							fmt.Println("Creation time for the JSON credential file:\t", lastCreationTimeForFile)
							fmt.Println("Modification time for the JSON credential file:\t", newSecretModifiedTime)
							// printing contents of the file:
							bytes, err := os.ReadFile(credentialFileFullPaths[0])
							if err != nil {
								fmt.Println("Error while reading the modified JSON credential file:", err)
							}
							fmt.Printf("The new contents of the JSON file are: \"%s\"\n", string(bytes))
							Expect(newSecretModifiedTime.After(lastCreationTimeForFile)).Should(Equal(true))
						})
					})
					AfterEach(func() {
						err := os.RemoveAll(credentialDirectory)
						Expect(err).ShouldNot(HaveOccurred())
					})
				})
				AfterEach(func() {
					os.Unsetenv(provider)
				})
			})
		})
	}
})

// creates the access credential files in the temporary directory.
// the function also returns the timestamp at which the last file was created
func createCredentialFiles(filenames []string) time.Time {
	for _, filename := range filenames {
		filename := filename
		// creates and writes content to the file
		err := os.WriteFile(filename, []byte("INITIAL CONTENT"), os.ModePerm)
		if err != nil {
			fmt.Println("Error while creating the credential file:", err)
			return time.Time{}
		}
	}
	// the last file created at the end has the latest creation time (modification time)
	lastFile, err := os.Open(filenames[len(filenames)-1])
	defer lastFile.Close()
	if err != nil {
		fmt.Println("Error while opening the last created credential file:", err)
		return time.Time{}
	}
	lastFileInfo, err := lastFile.Stat()
	if err != nil {
		fmt.Println("Error while fetching information of the last created credential file:", err)
		return time.Time{}
	}
	return lastFileInfo.ModTime()
}

// modifies the credential file to change the modification timestamp
func modifyCredentialFile(fileIndex int, credentialFileFullPaths []string) error {
	err := os.WriteFile(credentialFileFullPaths[fileIndex], []byte("MODIFIED CONTENT"), os.ModePerm)
	if err != nil {
		fmt.Println("Error while modifying the credential file: ", err)
	}
	fmt.Println("Modified the file! ", credentialFileFullPaths[fileIndex])
	return nil
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

func resetObjectMap() {
	for k := range objectMap {
		delete(objectMap, k)
	}
}
