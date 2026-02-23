// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package compactor_test

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/compactor"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/restorer"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/gardener/etcd-backup-restore/pkg/wrappers"
	"github.com/gardener/etcd-backup-restore/test/utils"

	"go.etcd.io/etcd/pkg/types"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Running Compactor", func() {
	var (
		dir             string
		store           brtypes.SnapStore
		cptr            *compactor.Compactor
		restorePeerURLs []string
		clusterUrlsMap  types.URLsMap
		peerUrls        types.URLs
	)
	const (
		restoreName            string = "default"
		restoreClusterToken    string = "etcd-cluster"
		restoreCluster         string = "default=http://localhost:2380"
		skipHashCheck          bool   = false
		maxFetchers            uint   = 6
		maxCallSendMsgSize            = 2 * 1024 * 1024 //2Mib
		maxRequestBytes               = 2 * 1024 * 1024 //2Mib
		maxTxnOps                     = 2 * 1024
		embeddedEtcdQuotaBytes int64  = 8 * 1024 * 1024 * 1024
		snapshotTimeout               = 30 * time.Second
		defragTimeout                 = 30 * time.Second
		needDefragmentation           = true
	)

	BeforeEach(func() {
		restorePeerURLs = []string{"http://localhost:2380"}
		clusterUrlsMap, err = types.NewURLsMap(restoreCluster)
		Expect(err).ShouldNot(HaveOccurred())
		peerUrls, err = types.NewURLs(restorePeerURLs)
		Expect(err).ShouldNot(HaveOccurred())
	})

	Describe("Compact while a etcd server is running", func() {
		var restoreOpts *brtypes.RestoreOptions
		var compactorConfig *brtypes.CompactorConfig
		var compactOptions *brtypes.CompactOptions
		var compactedSnapshot *brtypes.Snapshot
		var snapstoreConfig *brtypes.SnapstoreConfig
		var tempRestoreDir string
		var tempDataDir string

		BeforeEach(func() {
			dir = fmt.Sprintf("%s/etcd/snapshotter.bkp", testSuiteDir)
			snapstoreConfig = &brtypes.SnapstoreConfig{
				Container: dir,
				Provider:  "Local",
			}

			store, err = snapstore.GetSnapstore(snapstoreConfig)
			Expect(err).ShouldNot(HaveOccurred())
			fmt.Println("The store where compaction will save snapshot is: ", store)

			// temporary data directory should not be created, it will be created by the Restore API
			tempDataDir = filepath.Join(testSuiteDir, "data-directory")

			tempRestorationSnapshotsDir, err := os.MkdirTemp(testSuiteDir, "temp-snapshots-")
			Expect(err).ShouldNot(HaveOccurred())

			cptr = compactor.NewCompactor(store, logger, nil)
			restoreOpts = &brtypes.RestoreOptions{
				Config: &brtypes.RestorationConfig{
					InitialCluster:               restoreCluster,
					InitialClusterToken:          restoreClusterToken,
					DataDir:                      tempDataDir,
					TempSnapshotsDir:             tempRestorationSnapshotsDir,
					InitialAdvertisePeerURLs:     restorePeerURLs,
					Name:                         restoreName,
					SkipHashCheck:                skipHashCheck,
					MaxFetchers:                  maxFetchers,
					MaxRequestBytes:              maxRequestBytes,
					MaxTxnOps:                    maxTxnOps,
					MaxCallSendMsgSize:           maxCallSendMsgSize,
					EmbeddedEtcdQuotaBytes:       embeddedEtcdQuotaBytes,
					NextClusterVersionCompatible: true,
				},
				ClusterURLs: clusterUrlsMap,
				PeerURLs:    peerUrls,
			}
			compactorConfig = &brtypes.CompactorConfig{
				NeedDefragmentation:       needDefragmentation,
				SnapshotTimeout:           wrappers.Duration{Duration: snapshotTimeout},
				DefragTimeout:             wrappers.Duration{Duration: defragTimeout},
				EnabledLeaseRenewal:       false,
				MetricsScrapeWaitDuration: wrappers.Duration{Duration: 0},
			}
			compactOptions = &brtypes.CompactOptions{
				RestoreOptions:  restoreOpts,
				CompactorConfig: compactorConfig,
				TempDir:         snapstoreConfig.TempDir,
			}
		})

		Context("with defragmentation allowed", func() {
			AfterEach(func() {
				_, err := os.Stat(tempRestoreDir)
				if err == nil {
					os.RemoveAll(tempRestoreDir)
				}
				_, err = os.Stat(tempDataDir)
				if err == nil {
					os.RemoveAll(tempDataDir)
				}
				_ = store.Delete(*compactedSnapshot)
			})

			It("should create a snapshot", func() {
				restoreOpts.Config.MaxFetchers = 4

				// Fetch the latest set of snapshots
				baseSnapshot, deltaSnapList, err := miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
				Expect(err).ShouldNot(HaveOccurred())

				restoreOpts.BaseSnapshot = baseSnapshot
				restoreOpts.DeltaSnapList = deltaSnapList

				// Take the compacted full snapshot with defragmentation allowed
				_, err = cptr.Compact(testCtx, compactOptions)
				Expect(err).ShouldNot(HaveOccurred())

				// Check if the compacted full snapshot is really present
				snapList, err := store.List(false)
				Expect(err).ShouldNot(HaveOccurred())

				compactedSnapshot = snapList[len(snapList)-1]
				fi, err := os.Stat(path.Join(compactedSnapshot.Prefix, compactedSnapshot.SnapDir, compactedSnapshot.SnapName))
				Expect(err).ShouldNot(HaveOccurred())

				size := fi.Size()
				Expect(size).ShouldNot(BeZero())
			})
			It("should restore from compacted snapshot", func() {
				restoreOpts.Config.MaxFetchers = 4

				// Fetch the latest set of snapshots
				baseSnapshot, deltaSnapList, err := miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
				Expect(err).ShouldNot(HaveOccurred())

				restoreOpts.BaseSnapshot = baseSnapshot
				restoreOpts.DeltaSnapList = deltaSnapList

				// Take the compacted full snapshot with defragmnetation allowed
				_, err = cptr.Compact(testCtx, compactOptions)
				Expect(err).ShouldNot(HaveOccurred())

				compactedSnapshot, deltaSnapList, err = miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
				Expect(err).ShouldNot(HaveOccurred())

				fi, err := os.Stat(path.Join(compactedSnapshot.Prefix, compactedSnapshot.SnapDir, compactedSnapshot.SnapName))
				Expect(err).ShouldNot(HaveOccurred())

				size := fi.Size()
				Expect(size).ShouldNot(BeZero())

				// Remove the data directory since etcd will fail to restore if the data directory is present
				err = os.RemoveAll(tempDataDir)
				Expect(err).ShouldNot(HaveOccurred())

				restoreOpts.BaseSnapshot = compactedSnapshot
				restoreOpts.DeltaSnapList = deltaSnapList

				restorer, err := restorer.NewRestorer(store, logger)
				Expect(err).ShouldNot(HaveOccurred())

				err = restorer.RestoreAndStopEtcd(*restoreOpts, nil)

				Expect(err).ShouldNot(HaveOccurred())
				err = utils.CheckDataConsistency(testCtx, restoreOpts.Config.DataDir, keyTo, "", "", logger)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})
		Context("with defragmentation not allowed", func() {
			AfterEach(func() {
				_, err := os.Stat(tempRestoreDir)
				if err != nil {
					os.RemoveAll(tempRestoreDir)
				}
				_, err = os.Stat(tempDataDir)
				if err == nil {
					os.RemoveAll(tempDataDir)
				}
				_ = store.Delete(*compactedSnapshot)
			})
			It("should create a snapshot", func() {
				restoreOpts.Config.MaxFetchers = 4

				// Fetch the latest set of snapshots
				baseSnapshot, deltaSnapList, err := miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
				Expect(err).ShouldNot(HaveOccurred())

				restoreOpts.BaseSnapshot = baseSnapshot
				restoreOpts.DeltaSnapList = deltaSnapList

				// Take the compacted full snapshot with defragmnetation not allowed
				compactOptions.NeedDefragmentation = false
				_, err = cptr.Compact(testCtx, compactOptions)
				Expect(err).ShouldNot(HaveOccurred())

				// Check if the compacted full snapshot is really present
				snapList, err := store.List(false)
				Expect(err).ShouldNot(HaveOccurred())

				compactedSnapshot = snapList[len(snapList)-1]
				fi, err := os.Stat(path.Join(compactedSnapshot.Prefix, compactedSnapshot.SnapDir, compactedSnapshot.SnapName))
				Expect(err).ShouldNot(HaveOccurred())

				size := fi.Size()
				Expect(size).ShouldNot(BeZero())
			})
			It("should restore from compacted snapshot", func() {
				restoreOpts.Config.MaxFetchers = 4

				// Fetch the latest set of snapshots
				baseSnapshot, deltaSnapList, err := miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
				Expect(err).ShouldNot(HaveOccurred())

				restoreOpts.BaseSnapshot = baseSnapshot
				restoreOpts.DeltaSnapList = deltaSnapList

				// Take the compacted full snapshot with defragmnetation not allowed
				compactOptions.NeedDefragmentation = false
				_, err = cptr.Compact(testCtx, compactOptions)
				Expect(err).ShouldNot(HaveOccurred())

				// Check if the compacted full snapshot is really present
				compactedSnapshot, deltaSnapList, err = miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
				Expect(err).ShouldNot(HaveOccurred())
				fi, err := os.Stat(path.Join(compactedSnapshot.Prefix, compactedSnapshot.SnapDir, compactedSnapshot.SnapName))
				Expect(err).ShouldNot(HaveOccurred())

				size := fi.Size()
				Expect(size).ShouldNot(BeZero())

				// Remove the data directory since etcd will fail to restore if the data directory is present
				err = os.RemoveAll(tempDataDir)
				Expect(err).ShouldNot(HaveOccurred())

				restoreOpts.BaseSnapshot = compactedSnapshot
				restoreOpts.DeltaSnapList = deltaSnapList

				restorer, err := restorer.NewRestorer(store, logger)
				Expect(err).ShouldNot(HaveOccurred())

				err = restorer.RestoreAndStopEtcd(*restoreOpts, nil)

				Expect(err).ShouldNot(HaveOccurred())
				err = utils.CheckDataConsistency(testCtx, restoreOpts.Config.DataDir, keyTo, "", "", logger)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})
		Context("with no base snapshot in backup store", func() {
			It("should not run compaction", func() {
				restoreOpts.Config.MaxFetchers = 4

				// Fetch the latest snapshots which are one compacted full snapshot and subsequent delta snapshots
				_, deltaSnapList, err := miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
				Expect(err).ShouldNot(HaveOccurred())

				// But set the BaseSnapshot as nil
				restoreOpts.BaseSnapshot = nil
				restoreOpts.DeltaSnapList = deltaSnapList

				// Try capturing the compacted full snapshot
				_, err = cptr.Compact(testCtx, compactOptions)
				Expect(err).Should(HaveOccurred())
			})
		})
	})
})
