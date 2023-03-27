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

package copier_test

import (
	"context"
	"os"

	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	. "github.com/gardener/etcd-backup-restore/pkg/snapshot/copier"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
)

var (
	sourceSnapstoreConfig *brtypes.SnapstoreConfig
	destSnapstoreConfig   *brtypes.SnapstoreConfig
	ss                    brtypes.SnapStore
	ds                    brtypes.SnapStore
	copier                *Copier
)

var _ = Describe("Running Copier", func() {
	BeforeEach(func() {
		sourceSnapstoreConfig = &brtypes.SnapstoreConfig{
			MaxParallelChunkUploads: 5,
			TempDir:                 "/tmp",
			Provider:                "Local",
			Container:               snapstoreDir,
		}
		destSnapstoreConfig = &brtypes.SnapstoreConfig{
			MaxParallelChunkUploads: 5,
			TempDir:                 "/tmp",
			Provider:                "Local",
			Container:               targetSnapstoreDir,
		}

		var err error
		ss, ds, err = GetSourceAndDestinationStores(sourceSnapstoreConfig, destSnapstoreConfig)
		Expect(err).ToNot(HaveOccurred())
		copier = NewCopier(logger, ss, ds, -1, -1, 10, false, 0)
	})
	AfterEach(func() {
		err = os.RemoveAll(targetSnapstoreDir)
		Expect(err).ShouldNot(HaveOccurred())
	})
	It("should run the copy command succesfully", func() {
		fullSourceStoreSnapshot, deltaSrourceStoreSnapList, err := miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(ss)
		Expect(err).NotTo(HaveOccurred())
		Expect(copier.Run(context.TODO())).ToNot(HaveOccurred())
		fullTargetStoreSnapshot, deltaTargetStoreSnapList, err := miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(ds)
		chekIfSnapsAreTheSame(fullSourceStoreSnapshot, fullTargetStoreSnapshot)
		Expect(len(deltaSrourceStoreSnapList)).To(Equal(len(deltaTargetStoreSnapList)))
		for i, ssnap := range deltaSrourceStoreSnapList {
			chekIfSnapsAreTheSame(ssnap, deltaSrourceStoreSnapList[i])
		}
	})
})

func chekIfSnapsAreTheSame(s1 *brtypes.Snapshot, s2 *brtypes.Snapshot) {
	// SnapName and Prefix could be different after the copy operation.
	logger.Logger.Infof("Comparing snaps: %v %v", s1, s2)
	Expect(*s1).To(MatchFields(IgnoreExtras, Fields{
		"Kind":              Equal(s2.Kind),
		"StartRevision":     Equal(s2.StartRevision),
		"LastRevision":      Equal(s2.LastRevision),
		"CreatedOn":         Equal(s2.CreatedOn),
		"SnapName":          Equal(s2.SnapName),
		"IsChunk":           Equal(s2.IsChunk),
		"CompressionSuffix": Equal(s2.CompressionSuffix),
	}))
}
