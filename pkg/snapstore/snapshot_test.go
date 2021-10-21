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
	"fmt"
	"sort"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/compressor"
	. "github.com/gardener/etcd-backup-restore/pkg/snapstore"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Snapshot", func() {

	Describe("Snapshot service", func() {
		Context("when provied with list of snapshot", func() {
			It("sorts snapshot by last revision number", func() {
				interval := int64(5)
				now := time.Now().Unix()
				snapdir := fmt.Sprintf("Backup-%d", now)
				snap1 := brtypes.Snapshot{
					CreatedOn:     time.Unix(now, 0).UTC(),
					StartRevision: 0,
					LastRevision:  2088,
					Kind:          brtypes.SnapshotKindFull,
					SnapDir:       snapdir,
				}
				snap2 := brtypes.Snapshot{
					CreatedOn:     time.Unix(now+1*interval, 0).UTC(),
					StartRevision: 0,
					LastRevision:  1988,
					Kind:          brtypes.SnapshotKindFull,
					SnapDir:       snapdir,
				}
				snap3 := brtypes.Snapshot{
					CreatedOn:     time.Unix(now+2*interval, 0).UTC(),
					StartRevision: 0,
					LastRevision:  1888,
					Kind:          brtypes.SnapshotKindFull,
					SnapDir:       snapdir,
				}
				snap4 := brtypes.Snapshot{
					CreatedOn:     time.Unix(now+3*interval, 0).UTC(),
					StartRevision: 0,
					LastRevision:  1788,
					Kind:          brtypes.SnapshotKindFull,
					SnapDir:       snapdir,
				}
				snap5 := brtypes.Snapshot{
					CreatedOn:     time.Unix(now+4*interval, 0).UTC(),
					StartRevision: 0,
					LastRevision:  1688,
					Kind:          brtypes.SnapshotKindFull,
					SnapDir:       snapdir,
				}
				snap6 := brtypes.Snapshot{
					CreatedOn:     time.Unix(now+5*interval, 0).UTC(),
					StartRevision: 0,
					LastRevision:  1588,
					Kind:          brtypes.SnapshotKindFull,
					SnapDir:       snapdir,
				}
				snapList := brtypes.SnapList{&snap4, &snap1, &snap5, &snap3, &snap6, &snap2}
				sort.Sort(snapList)
				expectedSnapList := brtypes.SnapList{&snap6, &snap5, &snap4, &snap3, &snap2, &snap1}
				for i := 0; i < len(snapList); i++ {
					Expect(snapList[i]).Should(Equal(expectedSnapList[i]))
				}
			})

			It("sorts snapshot by creation time as well as start revision", func() {
				interval := int64(5)
				now := time.Now().Unix()
				snapdir := fmt.Sprintf("Backup-%d", now)
				snap1 := brtypes.Snapshot{
					CreatedOn:     time.Unix(now+2*interval, 0).UTC(),
					StartRevision: 1001,
					LastRevision:  1050,
					Kind:          brtypes.SnapshotKindDelta,
					SnapDir:       snapdir,
				}
				snap2 := brtypes.Snapshot{
					CreatedOn:     time.Unix(now+1*interval, 0).UTC(),
					StartRevision: 1051,
					LastRevision:  1200,
					Kind:          brtypes.SnapshotKindDelta,
					SnapDir:       snapdir,
				}
				snap3 := brtypes.Snapshot{
					CreatedOn:     time.Unix(now+3*interval, 0).UTC(),
					StartRevision: 1201,
					LastRevision:  1500,
					Kind:          brtypes.SnapshotKindDelta,
					SnapDir:       snapdir,
				}
				snap4 := brtypes.Snapshot{
					CreatedOn:     time.Unix(now+2*interval, 0).UTC(),
					StartRevision: 1501,
					LastRevision:  2000,
					Kind:          brtypes.SnapshotKindDelta,
					SnapDir:       snapdir,
				}
				snap5 := brtypes.Snapshot{
					CreatedOn:     time.Unix(now, 0).UTC(),
					StartRevision: 2001,
					LastRevision:  2150,
					Kind:          brtypes.SnapshotKindDelta,
					SnapDir:       snapdir,
				}
				snap6 := brtypes.Snapshot{
					CreatedOn:     time.Unix(now+1*interval, 0).UTC(),
					StartRevision: 2151,
					LastRevision:  2160,
					Kind:          brtypes.SnapshotKindDelta,
					SnapDir:       snapdir,
				}
				snapList := brtypes.SnapList{&snap5, &snap2, &snap1, &snap4, &snap6, &snap3}
				sort.Sort(snapList)
				expectedSnapList := brtypes.SnapList{&snap1, &snap2, &snap3, &snap4, &snap5, &snap6}
				for i := 1; i < len(snapList); i++ {
					Expect(snapList[i].LastRevision).Should(BeNumerically(">", snapList[i-1].LastRevision))
					Expect(snapList[i]).Should(Equal(expectedSnapList[i]))
				}
			})
		})

		Context("given a snapshot", func() {
			now := time.Now().Unix()
			snap1 := brtypes.Snapshot{
				CreatedOn:     time.Unix(now, 0).UTC(),
				StartRevision: 0,
				LastRevision:  2088,
				Kind:          brtypes.SnapshotKindFull,
			}
			It("generates snapshot name ", func() {
				snap1.GenerateSnapshotName()
				Expect(snap1.SnapName).Should(Equal(fmt.Sprintf("Full-00000000-00002088-%08d", now)))
			})
		})
	})

	Describe("Parse Snapshot path", func() {
		Context("when path with backup version v1 specified", func() {
			It("populate correct prefix, snapdir, snapname", func() {
				snapPath := "/abc/v1/Backup--00000000-00002088-2387428/Full-00000000-00002088-2387428"
				snap, err := ParseSnapshot(snapPath)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(snap.Prefix).To(Equal("/abc/v1/"))
				Expect(snap.SnapDir).To(Equal("Backup--00000000-00002088-2387428"))
				Expect(snap.SnapName).To(Equal("Full-00000000-00002088-2387428"))
				snapPath = "v1/Backup--00000000-00002088-2387428/Full-00000000-00002088-2387428"
				snap, err = ParseSnapshot(snapPath)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(snap.Prefix).To(Equal("v1/"))
				Expect(snap.SnapDir).To(Equal("Backup--00000000-00002088-2387428"))
				Expect(snap.SnapName).To(Equal("Full-00000000-00002088-2387428"))
			})
		})
		Context("when path with backup version v2 specified", func() {
			It("populate correct prefix, snapdir, snapname", func() {
				snapPath := "/abc/v2/Full-00000000-00002088-2387428"
				snap, err := ParseSnapshot(snapPath)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(snap.Prefix).To(Equal("/abc/v2/"))
				Expect(snap.SnapDir).To(Equal(""))
				Expect(snap.SnapName).To(Equal("Full-00000000-00002088-2387428"))
				snapPath = "v2/Full-00000000-00002088-2387428"
				snap, err = ParseSnapshot(snapPath)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(snap.Prefix).To(Equal("v2/"))
				Expect(snap.SnapDir).To(Equal(""))
				Expect(snap.SnapName).To(Equal("Full-00000000-00002088-2387428"))
			})
		})
		Context("when path without any backup version specified", func() {
			It("returns error", func() {
				snapPath := "/abc/Full-00000000-00002088-2387428"
				_, err := ParseSnapshot(snapPath)
				Expect(err).Should(HaveOccurred())
			})
		})
		Context("when only snapshot name specified", func() {
			It("returns error", func() {
				snapPath := "Full-00000000-00002088-2387428"
				_, err := ParseSnapshot(snapPath)
				Expect(err).Should(HaveOccurred())
			})
		})
	})

	Describe("Parse Snapshot name", func() {
		Context("when valid snapshot name provided under valid path", func() {
			It("correctly parses a snapshot name with neither a compression nor a final suffix", func() {
				snapPath := "v2/Full-00000000-00030009-1518427675"
				s, err := ParseSnapshot(snapPath)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(s).To(Equal(&brtypes.Snapshot{
					Kind:          brtypes.SnapshotKindFull,
					StartRevision: 0,
					LastRevision:  30009,
					CreatedOn:     time.Unix(1518427675, 0).UTC(),
					SnapName:      "Full-00000000-00030009-1518427675",
					Prefix:        "v2/",
				}))
			})
			It("correctly parses a snapshot name with a compression suffix", func() {
				snapPath := "v2/Full-00000000-00030009-1518427675.gz"
				s, err := ParseSnapshot(snapPath)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(s).To(Equal(&brtypes.Snapshot{
					Kind:              brtypes.SnapshotKindFull,
					StartRevision:     0,
					LastRevision:      30009,
					CreatedOn:         time.Unix(1518427675, 0).UTC(),
					SnapName:          "Full-00000000-00030009-1518427675.gz",
					Prefix:            "v2/",
					CompressionSuffix: compressor.GzipCompressionExtension,
				}))
			})
			It("correctly parses a snapshot name with a final suffix", func() {
				snapPath := "v2/Full-00000000-00030009-1518427675.final"
				s, err := ParseSnapshot(snapPath)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(s).To(Equal(&brtypes.Snapshot{
					Kind:          brtypes.SnapshotKindFull,
					StartRevision: 0,
					LastRevision:  30009,
					CreatedOn:     time.Unix(1518427675, 0).UTC(),
					SnapName:      "Full-00000000-00030009-1518427675.final",
					Prefix:        "v2/",
					IsFinal:       true,
				}))
			})
			It("correctly parses a snapshot name with both a compression and a final suffix", func() {
				snapPath := "v2/Full-00000000-00030009-1518427675.gz.final"
				s, err := ParseSnapshot(snapPath)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(s).To(Equal(&brtypes.Snapshot{
					Kind:              brtypes.SnapshotKindFull,
					StartRevision:     0,
					LastRevision:      30009,
					CreatedOn:         time.Unix(1518427675, 0).UTC(),
					SnapName:          "Full-00000000-00030009-1518427675.gz.final",
					Prefix:            "v2/",
					CompressionSuffix: compressor.GzipCompressionExtension,
					IsFinal:           true,
				}))
			})
		})

		Context("when number of separated tokens not equal to 4", func() {
			It("returns error", func() {
				snapPath := "v2/Full-00000000-00002088-2387428-43"
				_, err := ParseSnapshot(snapPath)
				Expect(err).Should(HaveOccurred())
			})
		})

		Context("when non integer start revision specified", func() {
			It("returns error", func() {
				snapPath := "v2/Full-00h000000-00002088-2387428"
				_, err := ParseSnapshot(snapPath)
				Expect(err).Should(HaveOccurred())
			})
		})
		Context("when not integer last revision specified", func() {
			It("returns error", func() {
				snapPath := "v2/Full-00000000-00sdf002088-2387428"
				_, err := ParseSnapshot(snapPath)
				Expect(err).Should(HaveOccurred())
			})
		})
		Context("when start revision is more than last revision", func() {
			It("returns error", func() {
				snapPath := "v2/Full-00012345-00002088-2387428"
				_, err := ParseSnapshot(snapPath)
				Expect(err).Should(HaveOccurred())
			})
		})
		Context("when non integer unix time specified", func() {
			It("returns error", func() {
				snapPath := "v2/Full-00000000-00002088-23874sdf43"
				_, err := ParseSnapshot(snapPath)
				Expect(err).Should(HaveOccurred())
			})
		})
		Context("when invalid kind specified", func() {
			It("returns error", func() {
				snapPath := "v2/meta-00000000-00002088-2387428"
				_, err := ParseSnapshot(snapPath)
				Expect(err).Should(HaveOccurred())
			})
		})
		Context("when invalid path specified", func() {
			It("returns error", func() {
				snapPath := "v2/Backup--00000000-00002088-2387428"
				_, err := ParseSnapshot(snapPath)
				Expect(err).Should(HaveOccurred())
			})
		})
	})
})
