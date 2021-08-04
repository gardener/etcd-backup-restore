package compactor_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"

	"github.com/gardener/etcd-backup-restore/pkg/compactor"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/restorer"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/gardener/etcd-backup-restore/test/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"go.etcd.io/etcd/pkg/types"
)

var _ = Describe("Running Compactor", func() {
	var (
		dir             string
		store           brtypes.SnapStore
		cptr            *compactor.Compactor
		restorePeerURLs []string
		clusterUrlsMap  types.URLsMap
		peerUrls        types.URLs
		// deltaSnapshotPeriod time.Duration
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
	)

	BeforeEach(func() {
		//wg = &sync.WaitGroup{}
		restorePeerURLs = []string{"http://localhost:2380"}
		clusterUrlsMap, err = types.NewURLsMap(restoreCluster)
		Expect(err).ShouldNot(HaveOccurred())
		peerUrls, err = types.NewURLs(restorePeerURLs)
		Expect(err).ShouldNot(HaveOccurred())
	})

	Describe("Compact while a etcd server is running", func() {
		var restoreOpts *brtypes.RestoreOptions
		var compactedSnapshot *brtypes.Snapshot
		var restoreDir string

		BeforeEach(func() {
			dir = fmt.Sprintf("%s/etcd/snapshotter.bkp", testSuitDir)

			store, err = snapstore.GetSnapstore(&brtypes.SnapstoreConfig{Container: dir, Provider: "Local"})
			Expect(err).ShouldNot(HaveOccurred())
			fmt.Println("The store where compaction will save snapshot is: ", store)

			cptr = compactor.NewCompactor(store, logger)
			restoreOpts = &brtypes.RestoreOptions{
				Config: &brtypes.RestorationConfig{
					InitialCluster:           restoreCluster,
					InitialClusterToken:      restoreClusterToken,
					RestoreDataDir:           "/tmp",
					InitialAdvertisePeerURLs: restorePeerURLs,
					Name:                     restoreName,
					SkipHashCheck:            skipHashCheck,
					MaxFetchers:              maxFetchers,
					MaxRequestBytes:          maxRequestBytes,
					MaxTxnOps:                maxTxnOps,
					MaxCallSendMsgSize:       maxCallSendMsgSize,
					EmbeddedEtcdQuotaBytes:   embeddedEtcdQuotaBytes,
				},
				ClusterURLs: clusterUrlsMap,
				PeerURLs:    peerUrls,
			}
		})

		Context("with defragmention allowed", func() {
			AfterEach(func() {
				_, err := os.Stat(restoreDir)
				if err == nil {
					os.RemoveAll(restoreDir)
				}
				store.Delete(*compactedSnapshot)
			})

			It("should create a snapshot", func() {
				restoreOpts.Config.MaxFetchers = 4

				// Fetch latest snapshots
				baseSnapshot, deltaSnapList, err := miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
				Expect(err).ShouldNot(HaveOccurred())

				restoreOpts.BaseSnapshot = baseSnapshot
				restoreOpts.DeltaSnapList = deltaSnapList

				// Take the compacted full snapshot with defragmnetation allowed
				_, err = cptr.Compact(restoreOpts, true)
				Expect(err).ShouldNot(HaveOccurred())

				// Check if the compacted full snapshot is really present
				snapList, err := store.List()
				Expect(err).ShouldNot(HaveOccurred())

				compactedSnapshot = snapList[len(snapList)-1]
				fi, err := os.Stat(path.Join(compactedSnapshot.Prefix, compactedSnapshot.SnapDir, compactedSnapshot.SnapName))
				Expect(err).ShouldNot(HaveOccurred())

				size := fi.Size()
				Expect(size).ShouldNot(BeZero())
			})
			It("should restore from compacted snapshot", func() {
				restoreOpts.Config.MaxFetchers = 4

				// Fetch latest snapshots
				baseSnapshot, deltaSnapList, err := miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
				Expect(err).ShouldNot(HaveOccurred())

				restoreOpts.BaseSnapshot = baseSnapshot
				restoreOpts.DeltaSnapList = deltaSnapList

				// Take the compacted full snapshot with defragmnetation allowed
				_, err = cptr.Compact(restoreOpts, true)
				Expect(err).ShouldNot(HaveOccurred())

				compactedSnapshot, deltaSnapList, err = miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
				Expect(err).ShouldNot(HaveOccurred())

				fi, err := os.Stat(path.Join(compactedSnapshot.Prefix, compactedSnapshot.SnapDir, compactedSnapshot.SnapName))
				Expect(err).ShouldNot(HaveOccurred())

				size := fi.Size()
				Expect(size).ShouldNot(BeZero())

				// Restore from the compacted snapshot
				restoreDir, err = ioutil.TempDir("/tmp", "restore-")
				Expect(err).ShouldNot(HaveOccurred())

				restoreOpts.Config.RestoreDataDir = restoreDir

				restoreOpts.BaseSnapshot = compactedSnapshot
				restoreOpts.DeltaSnapList = deltaSnapList

				rstr := restorer.NewRestorer(store, logger)

				err = rstr.RestoreAndStopEtcd(*restoreOpts)

				Expect(err).ShouldNot(HaveOccurred())
				err = utils.CheckDataConsistency(testCtx, restoreOpts.Config.RestoreDataDir, keyTo, logger)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})
		Context("with defragmention not allowed", func() {
			AfterEach(func() {
				_, err := os.Stat(restoreDir)
				if err != nil {
					os.RemoveAll(restoreDir)
				}
				store.Delete(*compactedSnapshot)
			})
			It("should create a snapshot", func() {
				restoreOpts.Config.MaxFetchers = 4

				// Fetch latest snapshots
				baseSnapshot, deltaSnapList, err := miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
				Expect(err).ShouldNot(HaveOccurred())

				restoreOpts.BaseSnapshot = baseSnapshot
				restoreOpts.DeltaSnapList = deltaSnapList

				// Take the compacted full snapshot with defragmnetation not allowed
				_, err = cptr.Compact(restoreOpts, false)
				Expect(err).ShouldNot(HaveOccurred())

				// Check if the compacted full snapshot is really present
				snapList, err := store.List()
				Expect(err).ShouldNot(HaveOccurred())

				compactedSnapshot = snapList[len(snapList)-1]
				fi, err := os.Stat(path.Join(compactedSnapshot.Prefix, compactedSnapshot.SnapDir, compactedSnapshot.SnapName))
				Expect(err).ShouldNot(HaveOccurred())

				size := fi.Size()
				Expect(size).ShouldNot(BeZero())
			})
			It("should restore from compacted snapshot", func() {
				restoreOpts.Config.MaxFetchers = 4

				// Fetch latest snapshots
				baseSnapshot, deltaSnapList, err := miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
				Expect(err).ShouldNot(HaveOccurred())

				restoreOpts.BaseSnapshot = baseSnapshot
				restoreOpts.DeltaSnapList = deltaSnapList

				// Take the compacted full snapshot with defragmnetation not allowed
				_, err = cptr.Compact(restoreOpts, false)
				Expect(err).ShouldNot(HaveOccurred())

				// Check if the compacted full snapshot is really present
				compactedSnapshot, deltaSnapList, err = miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
				Expect(err).ShouldNot(HaveOccurred())
				fi, err := os.Stat(path.Join(compactedSnapshot.Prefix, compactedSnapshot.SnapDir, compactedSnapshot.SnapName))
				Expect(err).ShouldNot(HaveOccurred())

				size := fi.Size()
				Expect(size).ShouldNot(BeZero())

				// Restore from the compacted snapshot
				restoreDir, err = ioutil.TempDir("/tmp", "restore-")
				Expect(err).ShouldNot(HaveOccurred())

				restoreOpts.Config.RestoreDataDir = restoreDir

				restoreOpts.BaseSnapshot = compactedSnapshot
				restoreOpts.DeltaSnapList = deltaSnapList

				rstr := restorer.NewRestorer(store, logger)

				err = rstr.RestoreAndStopEtcd(*restoreOpts)

				Expect(err).ShouldNot(HaveOccurred())
				err = utils.CheckDataConsistency(testCtx, restoreOpts.Config.RestoreDataDir, keyTo, logger)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})
		Context("with no basesnapshot in backup store", func() {
			It("should not run compaction", func() {
				restoreOpts.Config.MaxFetchers = 4

				// Fetch the latest snapshots which are one compacted full snapshot and subsequent delta snapshots
				_, deltaSnapList, err := miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
				Expect(err).ShouldNot(HaveOccurred())

				// But set the BaseSnapshot as nil
				restoreOpts.BaseSnapshot = nil
				restoreOpts.DeltaSnapList = deltaSnapList

				// Try capturing the compacted full snapshot
				_, err = cptr.Compact(restoreOpts, false)
				Expect(err).Should(HaveOccurred())
			})
		})
	})
})
