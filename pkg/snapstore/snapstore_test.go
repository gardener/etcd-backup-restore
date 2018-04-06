// Copyright © 2018 The Gardener Authors.
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
	"path"
	"strings"
	"time"

	. "github.com/gardener/etcd-backup-restore/pkg/snapstore"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Snapstore", func() {
	var (
		bucket       string
		snap1        Snapshot
		snap2        Snapshot
		snap3        Snapshot
		expectedVal1 string
		expectedVal2 string
		m            mockS3Client
		snapstores   map[string]SnapStore
	)
	// S3SnapStore is snapstore with local disk as backend
	BeforeEach(func() {
		bucket = "mock-bucket"
		prefix := "v1"
		now := time.Now().Unix()
		snap1 = Snapshot{
			CreatedOn:     time.Unix(now, 0),
			StartRevision: 0,
			LastRevision:  2088,
			Kind:          SnapshotKindFull,
		}
		snap2 = Snapshot{
			CreatedOn:     time.Unix(now+100, 0),
			StartRevision: 0,
			LastRevision:  1988,
			Kind:          SnapshotKindFull,
		}
		snap3 = Snapshot{
			CreatedOn:     time.Unix(now+200, 0),
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
		expectedVal1 = "value1"
		expectedVal2 = "value2"

		m = mockS3Client{
			objects: map[string][]byte{
				path.Join(prefix, snap1.SnapDir, snap1.SnapName): []byte(expectedVal1),
				path.Join(prefix, snap2.SnapDir, snap2.SnapName): []byte(expectedVal2),
			},
			prefix: prefix,
		}
		snapstores = map[string]SnapStore{
			"s3": NewS3FromClient(bucket, prefix, &m),
		}
	})

	Describe("Fetch operation", func() {
		It("fetches snapshot", func() {
			for _, snapStore := range snapstores {
				rc, err := snapStore.Fetch(snap1)
				Expect(err).ShouldNot(HaveOccurred())
				temp := make([]byte, len(expectedVal1))
				n, err := rc.Read(temp)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(string(temp[:n])).To(Equal(expectedVal1))
			}
		})
	})
	Describe("Save snapshot", func() {
		It("saves snapshot", func() {
			for _, snapStore := range snapstores {
				prevLen := len(m.objects)
				err := snapStore.Save(snap3, strings.NewReader("value3"))
				Expect(err).ShouldNot(HaveOccurred())
				Expect(len(m.objects)).To(Equal(prevLen + 1))
			}
		})
	})
	Describe("List snapshot", func() {
		It("gives sorted list of snapshot", func() {
			for _, snapStore := range snapstores {
				snapList, err := snapStore.List()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(snapList.Len()).To(Equal(2))
				Expect(snapList[0].SnapName).To(Equal(snap1.SnapName))
			}
		})
	})
	Describe("Delete snapshot", func() {
		It("deletes snapshot", func() {
			for _, snapStore := range snapstores {
				prevLen := len(m.objects)
				err := snapStore.Delete(snap2)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(len(m.objects)).To(Equal(prevLen - 1))
			}
		})
	})
})
