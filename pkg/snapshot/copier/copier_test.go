// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package copier_test

import (
	"context"
	"os"

	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

	. "github.com/gardener/etcd-backup-restore/pkg/snapshot/copier"
	. "github.com/onsi/ginkgo/v2"
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
