// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package etcdutil_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/gardener/etcd-backup-restore/pkg/compressor"
	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	mockfactory "github.com/gardener/etcd-backup-restore/pkg/mock/etcdutil/client"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"go.uber.org/mock/gomock"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("EtcdUtil Tests", func() {
	var (
		factory           *mockfactory.MockFactory
		ctrl              *gomock.Controller
		cm                *mockfactory.MockMaintenanceCloser
		store             brtypes.SnapStore
		etcdDBPath        string
		snapstoreConfig   *brtypes.SnapstoreConfig
		compressionConfig *compressor.CompressionConfig
	)

	BeforeEach(func() {
		compressionConfig = compressor.NewCompressorConfig()
		snapstoreConfig = &brtypes.SnapstoreConfig{Provider: "Local", TempDir: outputDir}
		store, err = snapstore.GetSnapstore(snapstoreConfig)
		Expect(err).ShouldNot(HaveOccurred())

		etcdDBPath = path.Join(etcdDir, "member/snap/db")

		ctrl = gomock.NewController(GinkgoT())
		factory = mockfactory.NewMockFactory(ctrl)
		cm = mockfactory.NewMockMaintenanceCloser(ctrl)
	})

	Describe("To take Full Snapshot of etcd", func() {
		var (
			dummyLastRevision = int64(77)
		)
		BeforeEach(func() {
			factory.EXPECT().NewMaintenance().Return(cm, nil).AnyTimes()
		})

		Context("Etcd's Snapshot API call failed", func() {
			It("should return error", func() {
				clientMaintenance, err := factory.NewMaintenance()
				Expect(err).ShouldNot(HaveOccurred())

				cm.EXPECT().Snapshot(gomock.Any()).DoAndReturn(func(_ context.Context) (io.ReadCloser, error) {
					return nil, fmt.Errorf("failed to take snapshot")
				})

				_, err = etcdutil.TakeAndSaveFullSnapshot(testCtx, clientMaintenance, store, snapstoreConfig.TempDir, dummyLastRevision, compressionConfig, compressor.UnCompressSnapshotExtension, false, logger)
				Expect(err).Should(HaveOccurred())
			})
		})

		Context("Etcd returns snapshot data with snapshot's correct SHA appended", func() {
			Context("with compression enabled", func() {
				It("shouldn't return error", func() {
					compressionConfig.Enabled = true
					client, err := factory.NewMaintenance()
					Expect(err).ShouldNot(HaveOccurred())

					cm.EXPECT().Snapshot(gomock.Any()).DoAndReturn(func(_ context.Context) (io.ReadCloser, error) {
						return getEtcdDBData(etcdDBPath, true), nil
					})

					_, err = etcdutil.TakeAndSaveFullSnapshot(testCtx, client, store, snapstoreConfig.TempDir, dummyLastRevision, compressionConfig, compressor.GzipCompressionExtension, false, logger)
					Expect(err).ShouldNot(HaveOccurred())
				})
			})

			Context("with compression not enabled", func() {
				It("shouldn't return error", func() {
					compressionConfig.Enabled = false
					client, err := factory.NewMaintenance()
					Expect(err).ShouldNot(HaveOccurred())

					cm.EXPECT().Snapshot(gomock.Any()).DoAndReturn(func(_ context.Context) (io.ReadCloser, error) {
						return getEtcdDBData(etcdDBPath, true), nil
					})

					_, err = etcdutil.TakeAndSaveFullSnapshot(testCtx, client, store, snapstoreConfig.TempDir, dummyLastRevision, compressionConfig, compressor.UnCompressSnapshotExtension, false, logger)
					Expect(err).ShouldNot(HaveOccurred())
				})
			})
		})

		Context("Etcd returns snapshot data without snapshot's SHA appended", func() {
			It("should return error", func() {
				client, err := factory.NewMaintenance()
				Expect(err).ShouldNot(HaveOccurred())

				cm.EXPECT().Snapshot(gomock.Any()).DoAndReturn(func(_ context.Context) (io.ReadCloser, error) {
					return getEtcdDBData(etcdDBPath, false), nil
				})

				_, err = etcdutil.TakeAndSaveFullSnapshot(testCtx, client, store, snapstoreConfig.TempDir, dummyLastRevision, compressionConfig, compressor.UnCompressSnapshotExtension, false, logger)
				Expect(err).Should(HaveOccurred())
			})
		})

		Context("full snapshot data got corrupted and SHA is not corrupted", func() {
			It("should return error", func() {
				withCorruptSHA := false
				client, err := factory.NewMaintenance()
				Expect(err).ShouldNot(HaveOccurred())

				cm.EXPECT().Snapshot(gomock.Any()).DoAndReturn(func(_ context.Context) (io.ReadCloser, error) {
					return getCorruptedEtcdDBData(etcdDBPath, withCorruptSHA), nil
				})

				_, err = etcdutil.TakeAndSaveFullSnapshot(testCtx, client, store, snapstoreConfig.TempDir, dummyLastRevision, compressionConfig, compressor.UnCompressSnapshotExtension, false, logger)
				Expect(err).Should(HaveOccurred())
			})
		})

		Context("full snapshot data is not corrupted but SHA got corrupted", func() {
			It("should return error", func() {
				withCorruptSHA := true
				client, err := factory.NewMaintenance()
				Expect(err).ShouldNot(HaveOccurred())

				cm.EXPECT().Snapshot(gomock.Any()).DoAndReturn(func(_ context.Context) (io.ReadCloser, error) {
					return getCorruptedEtcdDBData(etcdDBPath, withCorruptSHA), nil
				})

				_, err = etcdutil.TakeAndSaveFullSnapshot(testCtx, client, store, snapstoreConfig.TempDir, dummyLastRevision, compressionConfig, compressor.UnCompressSnapshotExtension, false, logger)
				Expect(err).Should(HaveOccurred())
			})
		})
	})

})

// getEtcdDBData is a helper function, use to mock snapshot api call of etcd.
func getEtcdDBData(pathToEtcdDB string, withSHA bool) io.ReadCloser {
	etcdSnapshotData, _ := os.ReadFile(pathToEtcdDB)
	if withSHA {
		hash := sha256.New()
		hash.Write(etcdSnapshotData)
		sha := hash.Sum(nil)
		snapDataWithHash := append(etcdSnapshotData, sha[:]...)

		// return data of snapshot with its SHA appended
		return io.NopCloser(bytes.NewReader(snapDataWithHash))
	}

	// return data of snapshot without its SHA appended
	return io.NopCloser(bytes.NewReader(etcdSnapshotData))
}

// getCorruptedEtcdDBData is a helper function, use to mock snapshot api call of etcd.
func getCorruptedEtcdDBData(pathToEtcdDB string, withCorruptSHA bool) io.ReadCloser {
	etcdSnapshotData, _ := os.ReadFile(pathToEtcdDB)
	hash := sha256.New()
	hash.Write(etcdSnapshotData)
	sha := hash.Sum(nil)

	if withCorruptSHA {
		// corrupt the SHA of snapshot
		sha = append([]byte("1"), sha[:len(sha)-1]...)
	} else {
		// corrupt the snapshot data
		etcdSnapshotData = append([]byte("corrupt"), etcdSnapshotData[:]...)
	}

	snapDataWithHash := append(etcdSnapshotData, sha[:]...)
	return io.NopCloser(bytes.NewReader(snapDataWithHash))
}
