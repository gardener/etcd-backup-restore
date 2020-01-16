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
	"io/ioutil"
	"net/url"
	"path"
	"strings"
	"time"

	. "github.com/gardener/etcd-backup-restore/pkg/snapstore"
	fake "github.com/gophercloud/gophercloud/testhelper/client"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
)

var (
	bucket    string = "mock-bucket"
	objectMap        = map[string]*[]byte{}
	prefix    string = "v1"
)

var _ = Describe("Snapstore", func() {
	var (
		snap1        Snapshot
		snap2        Snapshot
		snap3        Snapshot
		expectedVal1 []byte
		expectedVal2 []byte
		snapstores   map[string]SnapStore
	)

	BeforeEach(func() {
		now := time.Now().Unix()
		snap1 = Snapshot{
			CreatedOn:     time.Unix(now, 0).UTC(),
			StartRevision: 0,
			LastRevision:  2088,
			Kind:          SnapshotKindFull,
		}
		snap2 = Snapshot{
			CreatedOn:     time.Unix(now+100, 0).UTC(),
			StartRevision: 0,
			LastRevision:  1988,
			Kind:          SnapshotKindFull,
		}
		snap3 = Snapshot{
			CreatedOn:     time.Unix(now+200, 0).UTC(),
			StartRevision: 0,
			LastRevision:  1958,
			Kind:          SnapshotKindFull,
		}
		snap1.GenerateSnapshotName()
		snap1.GenerateSnapshotDirectory()
		snap2.GenerateSnapshotName()
		snap2.GenerateSnapshotDirectory()
		snap3.GenerateSnapshotName()
		snap3.GenerateSnapshotDirectory()
		expectedVal1 = []byte("value1")
		expectedVal2 = []byte("value2")

		snapstores = map[string]SnapStore{
			"s3": NewS3FromClient(bucket, prefix, "/tmp", 5, &mockS3Client{
				objects:          objectMap,
				prefix:           prefix,
				multiPartUploads: map[string]*[][]byte{},
			}),
			"swift": NewSwiftSnapstoreFromClient(bucket, prefix, "/tmp", 5, fake.ServiceClient()),
			"ABS":   newFakeABSSnapstore(),
			"GCS": NewGCSSnapStoreFromClient(bucket, prefix, "/tmp", 5, &mockGCSClient{
				objects: objectMap,
				prefix:  prefix,
			}),
			"OSS": NewOSSFromBucket(prefix, "/tmp", 5, &mockOSSBucket{
				objects:          objectMap,
				prefix:           prefix,
				multiPartUploads: map[string]*[][]byte{},
				bucketName:       bucket,
			}),
		}
	})

	Describe("Fetch operation", func() {
		It("fetches snapshot", func() {
			for key, snapStore := range snapstores {
				logrus.Infof("Running tests for %s", key)
				resetObjectMap()
				objectMap[path.Join(prefix, snap1.SnapDir, snap1.SnapName)] = &expectedVal1
				objectMap[path.Join(prefix, snap2.SnapDir, snap2.SnapName)] = &expectedVal2
				rc, err := snapStore.Fetch(testCtx, snap1)
				defer rc.Close()
				Expect(err).ShouldNot(HaveOccurred())
				buf := new(bytes.Buffer)
				_, err = io.Copy(buf, rc)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(buf.Bytes()).To(Equal(expectedVal1))
			}
		})
	})

	Describe("Save snapshot", func() {
		It("saves snapshot", func() {
			for key, snapStore := range snapstores {
				logrus.Infof("Running tests for %s", key)
				resetObjectMap()
				dummyData := make([]byte, 6*1024*1024)
				err := snapStore.Save(testCtx, snap3, ioutil.NopCloser(bytes.NewReader(dummyData)))
				Expect(err).ShouldNot(HaveOccurred())
				Expect(len(objectMap)).Should(BeNumerically(">", 0))
			}
		})
	})

	Describe("List snapshot", func() {
		It("gives sorted list of snapshot", func() {
			for key, snapStore := range snapstores {
				logrus.Infof("Running tests for %s", key)
				resetObjectMap()
				objectMap[path.Join(prefix, snap1.SnapDir, snap1.SnapName)] = &expectedVal1
				objectMap[path.Join(prefix, snap2.SnapDir, snap2.SnapName)] = &expectedVal2
				snapList, err := snapStore.List(testCtx)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(snapList.Len()).To(Equal(2))
				Expect(snapList[0].SnapName).To(Equal(snap1.SnapName))
			}
		})
	})

	Describe("Delete snapshot", func() {
		It("deletes snapshot", func() {
			for key, snapStore := range snapstores {
				logrus.Infof("Running tests for %s", key)
				resetObjectMap()
				objectMap[path.Join(prefix, snap1.SnapDir, snap1.SnapName)] = &expectedVal1
				objectMap[path.Join(prefix, snap2.SnapDir, snap2.SnapName)] = &expectedVal2
				prevLen := len(objectMap)
				err := snapStore.Delete(testCtx, snap2)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(len(objectMap)).To(Equal(prevLen - 1))
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
	path := u.EscapedPath()
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
