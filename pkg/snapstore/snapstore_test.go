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
	"net/url"
	"path"
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
				// List snap1 and snap3
				snapList, err := snapStore.List()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(snapList.Len()).To(Equal(2))
				Expect(snapList[0].SnapName).To(Equal(snap2.SnapName))
				// Fetch snap3
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
				// Save snapshot
				resetObjectMap()
				dummyData := make([]byte, 6*1024*1024)
				err = snapStore.Save(snap3, io.NopCloser(bytes.NewReader(dummyData)))
				Expect(err).ShouldNot(HaveOccurred())
				Expect(len(objectMap)).Should(BeNumerically(">", 0))
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

				// Save snapshot
				resetObjectMap()
				dummyData := make([]byte, 6*1024*1024)
				err = snapStore.Save(snap1, io.NopCloser(bytes.NewReader(dummyData)))
				Expect(err).ShouldNot(HaveOccurred())
				Expect(len(objectMap)).Should(BeNumerically(">", 0))

				prevLen = len(objectMap)
				dummyData = make([]byte, 6*1024*1024)
				err = snapStore.Save(snap4, io.NopCloser(bytes.NewReader(dummyData)))
				Expect(err).ShouldNot(HaveOccurred())
				Expect(len(objectMap)).Should(BeNumerically(">", prevLen))
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
				// Save snapshot
				resetObjectMap()
				dummyData := make([]byte, 6*1024*1024)
				err = snapStore.Save(snap4, io.NopCloser(bytes.NewReader(dummyData)))
				Expect(err).ShouldNot(HaveOccurred())
				Expect(len(objectMap)).Should(BeNumerically(">", 0))
			}
		})
	})
})

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
