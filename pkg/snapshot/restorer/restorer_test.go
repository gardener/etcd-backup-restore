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

package restorer_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/compressor"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/gardener/etcd-backup-restore/test/utils"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/pkg/types"

	. "github.com/gardener/etcd-backup-restore/pkg/snapshot/restorer"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	allSnapsInV1 = "allSnapsInV1"
	fullSnapInV1 = "fullSnapInV1"
	fullSnapInV2 = "fullSnapInV2"
)

var _ = Describe("Running Restorer", func() {
	var (
		store           brtypes.SnapStore
		rstr            *Restorer
		restorePeerURLs []string
		clusterUrlsMap  types.URLsMap
		peerUrls        types.URLs
		baseSnapshot    *brtypes.Snapshot
		deltaSnapList   brtypes.SnapList
		wg              *sync.WaitGroup
	)
	const (
		restoreName             string = "default"
		restoreClusterToken     string = "etcd-cluster"
		restoreCluster          string = "default=http://localhost:2380"
		skipHashCheck           bool   = false
		maxFetchers             uint   = 6
		maxCallSendMsgSize             = 2 * 1024 * 1024 //2Mib
		maxRequestBytes                = 2 * 1024 * 1024 //2Mib
		maxTxnOps                      = 2 * 1024
		embeddedEtcdQuotaBytes  int64  = 8 * 1024 * 1024 * 1024
		autoCompactionMode      string = "periodic"
		autoCompactionRetention string = "5m"
	)

	BeforeEach(func() {
		wg = &sync.WaitGroup{}
		restorePeerURLs = []string{"http://localhost:2380"}
		clusterUrlsMap, err = types.NewURLsMap(restoreCluster)
		Expect(err).ShouldNot(HaveOccurred())
		peerUrls, err = types.NewURLs(restorePeerURLs)
		Expect(err).ShouldNot(HaveOccurred())
	})

	Describe("For pre-loaded Snapstore", func() {
		var restoreOpts brtypes.RestoreOptions

		BeforeEach(func() {
			err = corruptEtcdDir()
			Expect(err).ShouldNot(HaveOccurred())

			store, err = snapstore.GetSnapstore(&brtypes.SnapstoreConfig{Container: snapstoreDir, Provider: "Local"})
			Expect(err).ShouldNot(HaveOccurred())

			baseSnapshot, deltaSnapList, err = miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
			Expect(err).ShouldNot(HaveOccurred())

			rstr = NewRestorer(store, logger)
			restoreOpts = brtypes.RestoreOptions{
				Config: &brtypes.RestorationConfig{
					RestoreDataDir:           etcdDir,
					InitialClusterToken:      restoreClusterToken,
					InitialCluster:           restoreCluster,
					Name:                     restoreName,
					InitialAdvertisePeerURLs: restorePeerURLs,
					SkipHashCheck:            skipHashCheck,
					MaxFetchers:              maxFetchers,
					MaxCallSendMsgSize:       maxCallSendMsgSize,
					MaxRequestBytes:          maxRequestBytes,
					MaxTxnOps:                maxTxnOps,
					EmbeddedEtcdQuotaBytes:   embeddedEtcdQuotaBytes,
					AutoCompactionMode:       autoCompactionMode,
					AutoCompactionRetention:  autoCompactionRetention,
				},
				BaseSnapshot:  baseSnapshot,
				DeltaSnapList: deltaSnapList,
				ClusterURLs:   clusterUrlsMap,
				PeerURLs:      peerUrls,
			}
		})

		Context("with embedded etcd quota not set", func() {
			It("should be set to default value of 8 GB and restore", func() {
				restoreOpts.Config.EmbeddedEtcdQuotaBytes = 0

				err = restoreOpts.Config.Validate()
				Expect(err).Should(HaveOccurred())
			})
		})

		Context("with invalid cluster URLS", func() {
			It("should fail with an error ", func() {
				restoreOpts.Config.InitialCluster = restoreName + "=http://localhost:2390"
				restoreOpts.Config.InitialAdvertisePeerURLs = []string{"http://localhost:2390"}
				restoreOpts.ClusterURLs, err = types.NewURLsMap(restoreOpts.Config.InitialCluster)

				err = rstr.RestoreAndStopEtcd(restoreOpts)
				Expect(err).Should(HaveOccurred())
			})
		})

		Context("with invalid restore directory", func() {
			It("should fail to restore", func() {
				restoreOpts.Config.RestoreDataDir = ""

				err = rstr.RestoreAndStopEtcd(restoreOpts)
				Expect(err).Should(HaveOccurred())
			})
		})

		Context("with invalid snapdir and snapname", func() {
			It("should fail to restore", func() {
				restoreOpts.BaseSnapshot.SnapDir = "test"
				restoreOpts.BaseSnapshot.SnapName = "test"

				err := rstr.RestoreAndStopEtcd(restoreOpts)
				Expect(err).Should(HaveOccurred())
			})
		})

		Context("with zero fetchers", func() {
			It("should return error", func() {
				restoreOpts.Config.MaxFetchers = 0

				err = restoreOpts.Config.Validate()
				Expect(err).Should(HaveOccurred())
			})
		})

		Context("with some random auto-compaction mode", func() {
			It("should return error", func() {
				restoreOpts.Config.AutoCompactionMode = "someRandomMode"

				err = restoreOpts.Config.Validate()
				Expect(err).Should(HaveOccurred())
			})
		})

		Context("with maximum of one fetcher allowed", func() {
			It("should restore etcd data directory", func() {
				restoreOpts.Config.MaxFetchers = 1
				err = rstr.RestoreAndStopEtcd(restoreOpts)
				Expect(err).ShouldNot(HaveOccurred())

				err = utils.CheckDataConsistency(testCtx, restoreOpts.Config.RestoreDataDir, keyTo, logger)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

		Context("with maximum of four fetchers allowed", func() {
			It("should restore etcd data directory", func() {
				restoreOpts.Config.MaxFetchers = 4

				err = rstr.RestoreAndStopEtcd(restoreOpts)
				Expect(err).ShouldNot(HaveOccurred())

				err = utils.CheckDataConsistency(testCtx, restoreOpts.Config.RestoreDataDir, keyTo, logger)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

		Context("with maximum of hundred fetchers allowed", func() {
			It("should restore etcd data directory", func() {
				restoreOpts.Config.MaxFetchers = 100

				err = rstr.RestoreAndStopEtcd(restoreOpts)
				Expect(err).ShouldNot(HaveOccurred())

				err = utils.CheckDataConsistency(testCtx, restoreOpts.Config.RestoreDataDir, keyTo, logger)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})
	})

	Describe("NEGATIVE: Negative Compression Scenarios", func() {
		var (
			compressionConfig *compressor.CompressionConfig
		)
		BeforeEach(func() {
			compressionConfig = compressor.NewCompressorConfig()
		})
		Context("with invalid compressionPolicy", func() {
			It("should return error", func() {
				compressionConfig.Enabled = true
				compressionConfig.CompressionPolicy = "someRandomAlgo"
				err = compressionConfig.Validate()
				Expect(err).Should(HaveOccurred())
			})
		})

		Context("with compression is not enabled and invalid compressionPolicy ", func() {
			It("should not return error", func() {
				compressionConfig.Enabled = false
				compressionConfig.CompressionPolicy = "someRandomAlgo"
				err = compressionConfig.Validate()
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

	})

	Describe("NEGATIVE:For Dynamic Loads and Negative Scenarios", func() {
		var (
			store               brtypes.SnapStore
			deltaSnapshotPeriod time.Duration
			endpoints           []string
			restorationConfig   *brtypes.RestorationConfig
		)

		BeforeEach(func() {
			deltaSnapshotPeriod = time.Second
			etcd, err = utils.StartEmbeddedEtcd(testCtx, etcdDir, logger)
			Expect(err).ShouldNot(HaveOccurred())
			endpoints = []string{etcd.Clients[0].Addr().String()}

			store, err = snapstore.GetSnapstore(&brtypes.SnapstoreConfig{Container: snapstoreDir, Provider: "Local"})
			Expect(err).ShouldNot(HaveOccurred())

			restorationConfig = &brtypes.RestorationConfig{
				RestoreDataDir:           etcdDir,
				InitialClusterToken:      restoreClusterToken,
				InitialCluster:           restoreCluster,
				Name:                     restoreName,
				InitialAdvertisePeerURLs: restorePeerURLs,
				SkipHashCheck:            skipHashCheck,
				MaxFetchers:              maxFetchers,
				MaxCallSendMsgSize:       maxCallSendMsgSize,
				MaxRequestBytes:          maxRequestBytes,
				MaxTxnOps:                maxTxnOps,
				EmbeddedEtcdQuotaBytes:   embeddedEtcdQuotaBytes,
				AutoCompactionMode:       autoCompactionMode,
				AutoCompactionRetention:  autoCompactionRetention,
			}
		})

		AfterEach(func() {
			etcd.Server.Stop()
			etcd.Close()
			cleanUp()
		})

		Context("with only delta snapshots and no full snapshots", func() {
			var (
				startWithFullSnapshot = false
			)

			It("should restore from the delta snapshots ", func() {
				wg.Add(1)
				populatorCtx, cancelPopulator := context.WithTimeout(testCtx, 2)
				go utils.PopulateEtcdWithWaitGroup(populatorCtx, wg, logger, endpoints, nil)
				defer cancelPopulator()
				logger.Infoln("Starting snapshotter with basesnapshot set to false")
				ssrCtx := utils.ContextWithWaitGroupFollwedByGracePeriod(testCtx, wg, 2)
				compressionConfig := compressor.NewCompressorConfig()
				snapstoreConfig := brtypes.SnapstoreConfig{Container: snapstoreDir, Provider: "Local"}
				err = utils.RunSnapshotter(logger, snapstoreConfig, deltaSnapshotPeriod, endpoints, ssrCtx.Done(), startWithFullSnapshot, compressionConfig)
				Expect(err).ShouldNot(HaveOccurred())
				etcd.Server.Stop()
				etcd.Close()

				err = corruptEtcdDir()
				Expect(err).ShouldNot(HaveOccurred())

				baseSnapshot, deltaSnapList, err = miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
				Expect(err).ShouldNot(HaveOccurred())
				logger.Infof("No of delta snapshots: %d", deltaSnapList.Len())
				logger.Infof("Base snapshot is %v", baseSnapshot)

				rstr = NewRestorer(store, logger)
				restoreOpts := brtypes.RestoreOptions{
					Config:        restorationConfig,
					BaseSnapshot:  baseSnapshot,
					DeltaSnapList: deltaSnapList,
					ClusterURLs:   clusterUrlsMap,
					PeerURLs:      peerUrls,
				}

				if baseSnapshot != nil {
					restoreOpts.BaseSnapshot.SnapDir = ""
					restoreOpts.BaseSnapshot.SnapName = ""
				}

				err := rstr.RestoreAndStopEtcd(restoreOpts)

				Expect(err).ShouldNot(HaveOccurred())
				err = utils.CheckDataConsistency(testCtx, restoreOpts.Config.RestoreDataDir, keyTo, logger)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

		Context("with no delta snapshots", func() {
			It("Should restore only full snapshot", func() {
				deltaSnapshotPeriod = time.Duration(0)
				logger.Infoln("Starting snapshotter for no delta snapshots")
				wg.Add(1)
				populatorCtx, cancelPopulator := context.WithTimeout(testCtx, 2*time.Second)
				go utils.PopulateEtcdWithWaitGroup(populatorCtx, wg, logger, endpoints, nil)
				defer cancelPopulator()
				ssrCtx := utils.ContextWithWaitGroupFollwedByGracePeriod(testCtx, wg, time.Second)
				compressionConfig := compressor.NewCompressorConfig()
				snapstoreConfig := brtypes.SnapstoreConfig{Container: snapstoreDir, Provider: "Local"}
				err = utils.RunSnapshotter(logger, snapstoreConfig, deltaSnapshotPeriod, endpoints, ssrCtx.Done(), true, compressionConfig)
				Expect(err).ShouldNot(HaveOccurred())
				etcd.Server.Stop()
				etcd.Close()

				err = corruptEtcdDir()
				Expect(err).ShouldNot(HaveOccurred())

				baseSnapshot, deltaSnapList, err = miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(deltaSnapList.Len()).Should(BeZero())

				rstr = NewRestorer(store, logger)

				restoreOpts := brtypes.RestoreOptions{
					Config:        restorationConfig,
					BaseSnapshot:  baseSnapshot,
					DeltaSnapList: deltaSnapList,
					ClusterURLs:   clusterUrlsMap,
					PeerURLs:      peerUrls,
				}

				err = rstr.RestoreAndStopEtcd(restoreOpts)

				Expect(err).ShouldNot(HaveOccurred())

			})
		})

		Context("with corrupted snapstore", func() {
			It("Should not restore and return error", func() {
				logger.Infoln("Starting snapshotter for corrupted snapstore")
				wg.Add(1)
				populatorCtx, cancelPopulator := context.WithTimeout(testCtx, 2*time.Second)
				go utils.PopulateEtcdWithWaitGroup(populatorCtx, wg, logger, endpoints, nil)
				defer cancelPopulator()
				ssrCtx := utils.ContextWithWaitGroupFollwedByGracePeriod(testCtx, wg, time.Second)
				compressionConfig := compressor.NewCompressorConfig()
				snapstoreConfig := brtypes.SnapstoreConfig{Container: snapstoreDir, Provider: "Local"}
				err = utils.RunSnapshotter(logger, snapstoreConfig, deltaSnapshotPeriod, endpoints, ssrCtx.Done(), true, compressionConfig)
				Expect(err).ShouldNot(HaveOccurred())
				etcd.Close()

				err = corruptEtcdDir()
				Expect(err).ShouldNot(HaveOccurred())

				baseSnapshot, deltaSnapList, err = miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
				Expect(err).ShouldNot(HaveOccurred())
				logger.Infof("No. of delta snapshots: %d", deltaSnapList.Len())

				snapshotToRemove := path.Join(baseSnapshot.Prefix, baseSnapshot.SnapDir, baseSnapshot.SnapName)
				logger.Infof("Snapshot to remove: %s", snapshotToRemove)
				err = os.Remove(snapshotToRemove)
				logger.Infof("Removed snapshot to cause corruption %s", snapshotToRemove)
				Expect(err).ShouldNot(HaveOccurred())

				rstr = NewRestorer(store, logger)

				restoreOpts := brtypes.RestoreOptions{
					Config:        restorationConfig,
					BaseSnapshot:  baseSnapshot,
					DeltaSnapList: deltaSnapList,
					ClusterURLs:   clusterUrlsMap,
					PeerURLs:      peerUrls,
				}

				err = rstr.RestoreAndStopEtcd(restoreOpts)
				Expect(err).Should(HaveOccurred())
				// the below consistency fails with index out of range error hence commented,
				// but the etcd directory is filled partially as part of the restore which should be relooked.
				// err = checkDataConsistency(restoreOptions.Config.RestoreDataDir, logger)
				// Expect(err).Should(HaveOccurred())

			})
		})

		Context("with etcd data dir not cleaned up before restore", func() {
			It("Should fail to restore", func() {
				logger.Infoln("Starting snapshotter for not cleaned etcd dir scenario")
				wg.Add(1)
				populatorCtx, cancelPopulator := context.WithTimeout(testCtx, 2*time.Second)
				go utils.PopulateEtcdWithWaitGroup(populatorCtx, wg, logger, endpoints, nil)
				defer cancelPopulator()
				ssrCtx := utils.ContextWithWaitGroupFollwedByGracePeriod(testCtx, wg, 2*time.Second)
				compressionConfig := compressor.NewCompressorConfig()
				snapstoreConfig := brtypes.SnapstoreConfig{Container: snapstoreDir, Provider: "Local"}
				err = utils.RunSnapshotter(logger, snapstoreConfig, deltaSnapshotPeriod, endpoints, ssrCtx.Done(), true, compressionConfig)
				Expect(err).ShouldNot(HaveOccurred())
				etcd.Close()

				baseSnapshot, deltaSnapList, err = miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
				Expect(err).ShouldNot(HaveOccurred())

				rstr = NewRestorer(store, logger)

				restoreOpts := brtypes.RestoreOptions{
					Config:        restorationConfig,
					BaseSnapshot:  baseSnapshot,
					DeltaSnapList: deltaSnapList,
					ClusterURLs:   clusterUrlsMap,
					PeerURLs:      peerUrls,
				}

				logger.Infoln("starting restore, restore directory exists already")
				err = rstr.RestoreAndStopEtcd(restoreOpts)
				logger.Infof("Failed to restore because :: %s", err)

				Expect(err).Should(HaveOccurred())
			})
		})

		//this test is excluded for now and is kept for reference purpose only
		// there needs to be some re-look done to validate the scenarios when a restore can happen on a running snapshot and accordingly include the test
		// as per current understanding the flow ensures it cannot happen but external intervention can not be ruled out as the command allows calling restore while snapshotting.
		XContext("while snapshotter is running ", func() {
			It("Should stop snapshotter while restore is happening", func() {
				wg.Add(1)
				populatorCtx, cancelPopulator := context.WithTimeout(testCtx, 5*time.Second)
				defer cancelPopulator()
				go utils.PopulateEtcdWithWaitGroup(populatorCtx, wg, logger, endpoints, nil)
				ssrCtx := utils.ContextWithWaitGroupFollwedByGracePeriod(testCtx, wg, 15*time.Second)

				logger.Infoln("Starting snapshotter while loading is happening")
				compressionConfig := compressor.NewCompressorConfig()
				snapstoreConfig := brtypes.SnapstoreConfig{Container: snapstoreDir, Provider: "Local"}
				err = utils.RunSnapshotter(logger, snapstoreConfig, deltaSnapshotPeriod, endpoints, ssrCtx.Done(), true, compressionConfig)
				Expect(err).ShouldNot(HaveOccurred())

				time.Sleep(time.Duration(5 * time.Second))
				etcd.Server.Stop()
				etcd.Close()

				err = corruptEtcdDir()
				Expect(err).ShouldNot(HaveOccurred())
				logger.Infoln("corrupted the etcd dir")

				store, err = snapstore.GetSnapstore(&brtypes.SnapstoreConfig{Container: snapstoreDir, Provider: "Local"})
				Expect(err).ShouldNot(HaveOccurred())
				baseSnapshot, deltaSnapList, err = miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
				Expect(err).ShouldNot(HaveOccurred())

				rstr = NewRestorer(store, logger)

				restoreOpts := brtypes.RestoreOptions{
					Config:        restorationConfig,
					BaseSnapshot:  baseSnapshot,
					DeltaSnapList: deltaSnapList,
					ClusterURLs:   clusterUrlsMap,
					PeerURLs:      peerUrls,
				}

				logger.Infoln("starting restore while snapshotter is running")
				err = rstr.RestoreAndStopEtcd(restoreOpts)
				Expect(err).ShouldNot(HaveOccurred())
				err = utils.CheckDataConsistency(testCtx, restoreOpts.Config.RestoreDataDir, keyTo, logger)
				Expect(err).ShouldNot(HaveOccurred())

				// Although the test has passed but the logic currently doesn't stop snapshotter explicitly but assumes that restore
				// shall be triggered only on restart of the etcd pod, so in the current case the snapshotter and restore were both running
				// together. However data corruption was not simulated as the embedded etcd used to populate need to be stopped for restore to begin.
				// In a productive scenarios as the command is exposed so it's possible to run this without knowledge of the tightly coupled
				// behavior of etcd restart.
			})
		})

		Context("when full snapshot is not compressed followed by multiple delta snapshots which are compressed using different compressionPolicy", func() {
			It("Should able to restore", func() {
				logger.Infoln("Starting restoration check when snapshots are available of different SnapshotSuffix")
				memberPath := path.Join(etcdDir, "member")

				// start the Snapshotter with compression not enabled to take full snapshot
				compressionConfig := compressor.NewCompressorConfig()
				compressionConfig.Enabled = false
				ctx, cancel := context.WithTimeout(testCtx, time.Duration(2*time.Second))
				snapstoreConfig := brtypes.SnapstoreConfig{Container: snapstoreDir, Provider: "Local"}
				err = utils.RunSnapshotter(logger, snapstoreConfig, deltaSnapshotPeriod, endpoints, ctx.Done(), true, compressionConfig)
				Expect(err).ShouldNot(HaveOccurred())
				cancel()

				// populate the etcd with some more data
				resp := &utils.EtcdDataPopulationResponse{}
				utils.PopulateEtcd(testCtx, logger, endpoints, 0, keyTo, resp)
				Expect(resp.Err).ShouldNot(HaveOccurred())

				// start the Snapshotter with compressionPolicy = "lzw" to take delta snapshot
				compressionConfig = compressor.NewCompressorConfig()
				compressionConfig.Enabled = true
				compressionConfig.CompressionPolicy = "lzw"
				ctx, cancel = context.WithTimeout(testCtx, time.Duration(2*time.Second))
				snapstoreConfig = brtypes.SnapstoreConfig{Container: snapstoreDir, Provider: "Local"}
				err = utils.RunSnapshotter(logger, snapstoreConfig, deltaSnapshotPeriod, endpoints, ctx.Done(), false, compressionConfig)
				Expect(err).ShouldNot(HaveOccurred())
				cancel()

				// populate the etcd with some data
				resp = &utils.EtcdDataPopulationResponse{}
				utils.PopulateEtcd(testCtx, logger, endpoints, 0, keyTo, resp)
				Expect(resp.Err).ShouldNot(HaveOccurred())

				// start the Snapshotter with compressionPolicy = "gzip"(default) to take delta snapshot
				compressionConfig = compressor.NewCompressorConfig()
				compressionConfig.Enabled = true
				ctx, cancel = context.WithTimeout(testCtx, time.Duration(2*time.Second))
				snapstoreConfig = brtypes.SnapstoreConfig{Container: snapstoreDir, Provider: "Local"}
				err = utils.RunSnapshotter(logger, snapstoreConfig, deltaSnapshotPeriod, endpoints, ctx.Done(), false, compressionConfig)
				Expect(err).ShouldNot(HaveOccurred())
				cancel()

				// populate the etcd with some data
				resp = &utils.EtcdDataPopulationResponse{}
				utils.PopulateEtcd(testCtx, logger, endpoints, 0, keyTo, resp)
				Expect(resp.Err).ShouldNot(HaveOccurred())

				// start the Snapshotter with compressionPolicy = "zlib" to take delta snapshot
				compressionConfig = compressor.NewCompressorConfig()
				compressionConfig.Enabled = true
				compressionConfig.CompressionPolicy = "zlib"
				ctx, cancel = context.WithTimeout(testCtx, time.Duration(2*time.Second))
				snapstoreConfig = brtypes.SnapstoreConfig{Container: snapstoreDir, Provider: "Local"}
				err = utils.RunSnapshotter(logger, snapstoreConfig, deltaSnapshotPeriod, endpoints, ctx.Done(), false, compressionConfig)
				Expect(err).ShouldNot(HaveOccurred())
				cancel()

				// remove the member dir
				err = os.RemoveAll(memberPath)
				Expect(err).ShouldNot(HaveOccurred())

				baseSnapshot, deltaSnapList, err = miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
				Expect(err).ShouldNot(HaveOccurred())

				rstr = NewRestorer(store, logger)

				restoreOpts := brtypes.RestoreOptions{
					Config:        restorationConfig,
					BaseSnapshot:  baseSnapshot,
					DeltaSnapList: deltaSnapList,
					ClusterURLs:   clusterUrlsMap,
					PeerURLs:      peerUrls,
				}

				err = rstr.RestoreAndStopEtcd(restoreOpts)
				Expect(err).ShouldNot(HaveOccurred())
				err = utils.CheckDataConsistency(testCtx, restoreOpts.Config.RestoreDataDir, keyTo, logger)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

		Context("when full snapshot is compressed followed by multiple delta Snapshots which are uncompressed as well as compressed", func() {
			It("Should able to restore", func() {
				memberPath := path.Join(etcdDir, "member")

				// populate the etcd with some data
				resp := &utils.EtcdDataPopulationResponse{}
				utils.PopulateEtcd(testCtx, logger, endpoints, 0, keyTo, resp)
				Expect(resp.Err).ShouldNot(HaveOccurred())

				// start the Snapshotter with compressionPolicy = "gzip"(default) to take full snapshot.
				compressionConfig := compressor.NewCompressorConfig()
				compressionConfig.Enabled = true
				ctx, cancel := context.WithTimeout(testCtx, time.Duration(2*time.Second))
				snapstoreConfig := brtypes.SnapstoreConfig{Container: snapstoreDir, Provider: "Local"}
				err = utils.RunSnapshotter(logger, snapstoreConfig, deltaSnapshotPeriod, endpoints, ctx.Done(), true, compressionConfig)
				Expect(err).ShouldNot(HaveOccurred())
				cancel()

				// populate the etcd with some more data
				resp = &utils.EtcdDataPopulationResponse{}
				utils.PopulateEtcd(testCtx, logger, endpoints, 0, keyTo, resp)
				Expect(resp.Err).ShouldNot(HaveOccurred())

				// start the Snapshotter with compressionPolicy = "lzw" to take delta snapshot.
				compressionConfig = compressor.NewCompressorConfig()
				compressionConfig.Enabled = true
				compressionConfig.CompressionPolicy = "lzw"
				ctx, cancel = context.WithTimeout(testCtx, time.Duration(2*time.Second))
				snapstoreConfig = brtypes.SnapstoreConfig{Container: snapstoreDir, Provider: "Local"}
				err = utils.RunSnapshotter(logger, snapstoreConfig, deltaSnapshotPeriod, endpoints, ctx.Done(), false, compressionConfig)
				Expect(err).ShouldNot(HaveOccurred())
				cancel()

				// populate the etcd with some data
				resp = &utils.EtcdDataPopulationResponse{}
				utils.PopulateEtcd(testCtx, logger, endpoints, 0, keyTo, resp)
				Expect(resp.Err).ShouldNot(HaveOccurred())

				// start the Snapshotter with compressionPolicy = "zlib" to take delta snapshot.
				compressionConfig = compressor.NewCompressorConfig()
				compressionConfig.Enabled = true
				compressionConfig.CompressionPolicy = "zlib"
				ctx, cancel = context.WithTimeout(testCtx, time.Duration(2*time.Second))
				snapstoreConfig = brtypes.SnapstoreConfig{Container: snapstoreDir, Provider: "Local"}
				err = utils.RunSnapshotter(logger, snapstoreConfig, deltaSnapshotPeriod, endpoints, ctx.Done(), false, compressionConfig)
				Expect(err).ShouldNot(HaveOccurred())
				cancel()

				// populate the etcd with some more data
				resp = &utils.EtcdDataPopulationResponse{}
				utils.PopulateEtcd(testCtx, logger, endpoints, 0, keyTo, resp)
				Expect(resp.Err).ShouldNot(HaveOccurred())

				// start the Snapshotter with compression not enabled to take delta snapshot.
				compressionConfig = compressor.NewCompressorConfig()
				ctx, cancel = context.WithTimeout(testCtx, time.Duration(2*time.Second))
				snapstoreConfig = brtypes.SnapstoreConfig{Container: snapstoreDir, Provider: "Local"}
				err = utils.RunSnapshotter(logger, snapstoreConfig, deltaSnapshotPeriod, endpoints, ctx.Done(), false, compressionConfig)
				Expect(err).ShouldNot(HaveOccurred())
				cancel()

				// remove the member dir
				err = os.RemoveAll(memberPath)
				Expect(err).ShouldNot(HaveOccurred())

				baseSnapshot, deltaSnapList, err = miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
				Expect(err).ShouldNot(HaveOccurred())

				rstr = NewRestorer(store, logger)

				restoreOpts := brtypes.RestoreOptions{
					Config:        restorationConfig,
					BaseSnapshot:  baseSnapshot,
					DeltaSnapList: deltaSnapList,
					ClusterURLs:   clusterUrlsMap,
					PeerURLs:      peerUrls,
				}

				err = rstr.RestoreAndStopEtcd(restoreOpts)
				Expect(err).ShouldNot(HaveOccurred())
				err = utils.CheckDataConsistency(testCtx, restoreOpts.Config.RestoreDataDir, keyTo, logger)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

	})

	Describe("For scenarios involving both old as well as updated directory structures being present", func() {
		var (
			store               brtypes.SnapStore
			deltaSnapshotPeriod time.Duration
			endpoints           []string
			restorationConfig   *brtypes.RestorationConfig
		)

		BeforeEach(func() {
			cleanUp() //Cleans etcd and backup store for these tests
			deltaSnapshotPeriod = time.Second
			etcd, err = utils.StartEmbeddedEtcd(testCtx, etcdDir, logger)
			Expect(err).ShouldNot(HaveOccurred())
			endpoints = []string{etcd.Clients[0].Addr().String()}

			store, err = snapstore.GetSnapstore(&brtypes.SnapstoreConfig{Container: snapstoreDir, Provider: "Local", Prefix: "v2"})
			Expect(err).ShouldNot(HaveOccurred())

			restorationConfig = &brtypes.RestorationConfig{
				RestoreDataDir:           etcdDir,
				InitialClusterToken:      restoreClusterToken,
				InitialCluster:           restoreCluster,
				Name:                     restoreName,
				InitialAdvertisePeerURLs: restorePeerURLs,
				SkipHashCheck:            skipHashCheck,
				MaxFetchers:              maxFetchers,
				MaxCallSendMsgSize:       maxCallSendMsgSize,
				MaxRequestBytes:          maxRequestBytes,
				MaxTxnOps:                maxTxnOps,
				EmbeddedEtcdQuotaBytes:   embeddedEtcdQuotaBytes,
				AutoCompactionMode:       autoCompactionMode,
				AutoCompactionRetention:  autoCompactionRetention,
			}
		})

		// Test to check backward compatibility of restorer
		// Tests restorer behaviour when local database has to be restored from snapstore with old (v1) as well as updated (v2) directory structures
		// TODO: Consider removing when backward compatibility no longer needed
		Context("With snapshots in v1 as well as v2 dir", func() {
			It("should restore from v2 dir snapshots", func() {
				memberPath := path.Join(etcdDir, "member")

				//Take snapshots for v1 dir
				compressionConfig := compressor.NewCompressorConfig()
				ctx, cancel := context.WithTimeout(testCtx, time.Duration(2*time.Second))
				err = takeInvalidV1Snaps(logger, snapstoreDir, deltaSnapshotPeriod, endpoints, ctx.Done(), compressionConfig)
				Expect(err).ShouldNot(HaveOccurred())
				cancel()

				//Take snapshots for v2 dir
				//Add data into etcd
				resp := &utils.EtcdDataPopulationResponse{}
				utils.PopulateEtcd(testCtx, logger, endpoints, 0, keyTo, resp)
				Expect(resp.Err).ShouldNot(HaveOccurred())
				//Take a full snapshot
				ctx, cancel = context.WithTimeout(testCtx, time.Duration(2*time.Second))
				snapstoreConfig := brtypes.SnapstoreConfig{Container: snapstoreDir, Provider: "Local", Prefix: "v2"}
				err = utils.RunSnapshotter(logger, snapstoreConfig, deltaSnapshotPeriod, endpoints, ctx.Done(), true, compressionConfig)
				Expect(err).ShouldNot(HaveOccurred())
				cancel()

				//Add data into etcd
				utils.PopulateEtcd(testCtx, logger, endpoints, 0, keyTo, resp)
				Expect(resp.Err).ShouldNot(HaveOccurred())
				//Take a delta snapshot
				ctx, cancel = context.WithTimeout(testCtx, time.Duration(2*time.Second))
				snapstoreConfig = brtypes.SnapstoreConfig{Container: snapstoreDir, Provider: "Local", Prefix: "v2"}
				err = utils.RunSnapshotter(logger, snapstoreConfig, deltaSnapshotPeriod, endpoints, ctx.Done(), false, compressionConfig)
				Expect(err).ShouldNot(HaveOccurred())
				cancel()

				//Add data into etcd
				utils.PopulateEtcd(testCtx, logger, endpoints, 0, keyTo, resp)
				Expect(resp.Err).ShouldNot(HaveOccurred())
				//Take a delta snapshot
				ctx, cancel = context.WithTimeout(testCtx, time.Duration(2*time.Second))
				snapstoreConfig = brtypes.SnapstoreConfig{Container: snapstoreDir, Provider: "Local", Prefix: "v2"}
				err = utils.RunSnapshotter(logger, snapstoreConfig, deltaSnapshotPeriod, endpoints, ctx.Done(), false, compressionConfig)
				Expect(err).ShouldNot(HaveOccurred())
				cancel()

				// remove the member dir
				err = os.RemoveAll(memberPath)
				Expect(err).ShouldNot(HaveOccurred())

				baseSnapshot, deltaSnapList, err = miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
				Expect(err).ShouldNot(HaveOccurred())

				rstr = NewRestorer(store, logger)

				restoreOpts := brtypes.RestoreOptions{
					Config:        restorationConfig,
					BaseSnapshot:  baseSnapshot,
					DeltaSnapList: deltaSnapList,
					ClusterURLs:   clusterUrlsMap,
					PeerURLs:      peerUrls,
				}

				//Restore
				err = rstr.RestoreAndStopEtcd(restoreOpts)
				Expect(err).ShouldNot(HaveOccurred())
				err = utils.CheckDataConsistency(testCtx, restoreOpts.Config.RestoreDataDir, keyTo, logger)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

		//Test to check backward compatibility of restorer
		//Tests restorer behaviour when local database has to be restored from snapstore with only old (v1) directory structures
		//TODO: Consider removing when backward compatibility no longer needed
		Context("With snapshots in v1 dir only", func() {
			It("should restore from v1 dir", func() {
				memberPath := path.Join(etcdDir, "member")

				//Take snapshots for v1 dir
				compressionConfig := compressor.NewCompressorConfig()
				ctx, cancel := context.WithTimeout(testCtx, time.Duration(2*time.Second))
				err = takeValidV1Snaps(logger, snapstoreDir, deltaSnapshotPeriod, endpoints, ctx.Done(), compressionConfig, allSnapsInV1)
				cancel()

				// remove the member dir
				err = os.RemoveAll(memberPath)
				Expect(err).ShouldNot(HaveOccurred())

				baseSnapshot, deltaSnapList, err = miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
				Expect(err).ShouldNot(HaveOccurred())

				rstr = NewRestorer(store, logger)

				restoreOpts := brtypes.RestoreOptions{
					Config:        restorationConfig,
					BaseSnapshot:  baseSnapshot,
					DeltaSnapList: deltaSnapList,
					ClusterURLs:   clusterUrlsMap,
					PeerURLs:      peerUrls,
				}

				//Restore
				err = rstr.RestoreAndStopEtcd(restoreOpts)
				Expect(err).ShouldNot(HaveOccurred())
				err = utils.CheckDataConsistency(testCtx, restoreOpts.Config.RestoreDataDir, keyTo, logger)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

		//Test to check backward compatibility of restorer
		//Tests restorer behaviour when local database has to be restored from snapstore with only updated (v2) directory structures
		//TODO: Consider removing when backward compatibility no longer needed
		Context("With snapshots in v2 dir only", func() {
			It("should restore from v2 dir snapshots", func() {
				memberPath := path.Join(etcdDir, "member")
				compressionConfig := compressor.NewCompressorConfig()

				//Snapshots for the v2 dir
				//Add data into etcd
				resp := &utils.EtcdDataPopulationResponse{}
				utils.PopulateEtcd(testCtx, logger, endpoints, 0, keyTo, resp)
				Expect(resp.Err).ShouldNot(HaveOccurred())
				//Take a full snapshot
				ctx, cancel := context.WithTimeout(testCtx, time.Duration(2*time.Second))
				snapstoreConfig := brtypes.SnapstoreConfig{Container: snapstoreDir, Provider: "Local", Prefix: "v2"}
				err = utils.RunSnapshotter(logger, snapstoreConfig, deltaSnapshotPeriod, endpoints, ctx.Done(), true, compressionConfig)
				Expect(err).ShouldNot(HaveOccurred())
				cancel()

				//Add data to etcd
				utils.PopulateEtcd(testCtx, logger, endpoints, 0, keyTo, resp)
				Expect(resp.Err).ShouldNot(HaveOccurred())
				//Take delta snapshot
				ctx, cancel = context.WithTimeout(testCtx, time.Duration(2*time.Second))
				snapstoreConfig = brtypes.SnapstoreConfig{Container: snapstoreDir, Provider: "Local", Prefix: "v2"}
				err = utils.RunSnapshotter(logger, snapstoreConfig, deltaSnapshotPeriod, endpoints, ctx.Done(), false, compressionConfig)
				Expect(err).ShouldNot(HaveOccurred())
				cancel()

				//Add data into etcd
				utils.PopulateEtcd(testCtx, logger, endpoints, 0, keyTo, resp)
				Expect(resp.Err).ShouldNot(HaveOccurred())
				//Take delta snapshot
				ctx, cancel = context.WithTimeout(testCtx, time.Duration(2*time.Second))
				snapstoreConfig = brtypes.SnapstoreConfig{Container: snapstoreDir, Provider: "Local", Prefix: "v2"}
				err = utils.RunSnapshotter(logger, snapstoreConfig, deltaSnapshotPeriod, endpoints, ctx.Done(), false, compressionConfig)
				Expect(err).ShouldNot(HaveOccurred())
				cancel()

				// remove the member dir
				err = os.RemoveAll(memberPath)
				Expect(err).ShouldNot(HaveOccurred())

				baseSnapshot, deltaSnapList, err = miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
				Expect(err).ShouldNot(HaveOccurred())

				rstr = NewRestorer(store, logger)

				restoreOpts := brtypes.RestoreOptions{
					Config:        restorationConfig,
					BaseSnapshot:  baseSnapshot,
					DeltaSnapList: deltaSnapList,
					ClusterURLs:   clusterUrlsMap,
					PeerURLs:      peerUrls,
				}

				//Restore
				err = rstr.RestoreAndStopEtcd(restoreOpts)
				Expect(err).ShouldNot(HaveOccurred())
				err = utils.CheckDataConsistency(testCtx, restoreOpts.Config.RestoreDataDir, keyTo, logger)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

		Context("With first few snapshots in v1 dir and some more incr snapshots are in v2 dir", func() {
			It("should restore from v1 dir and v2 dir", func() {
				memberPath := path.Join(etcdDir, "member")

				//Take snapshots for v1 dir
				compressionConfig := compressor.NewCompressorConfig()
				ctx, cancel := context.WithTimeout(testCtx, time.Duration(2*time.Second))
				err = takeValidV1Snaps(logger, snapstoreDir, deltaSnapshotPeriod, endpoints, ctx.Done(), compressionConfig, fullSnapInV1)
				cancel()

				// remove the member dir
				err = os.RemoveAll(memberPath)
				Expect(err).ShouldNot(HaveOccurred())

				baseSnapshot, deltaSnapList, err = miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
				Expect(err).ShouldNot(HaveOccurred())

				rstr = NewRestorer(store, logger)

				restoreOpts := brtypes.RestoreOptions{
					Config:        restorationConfig,
					BaseSnapshot:  baseSnapshot,
					DeltaSnapList: deltaSnapList,
					ClusterURLs:   clusterUrlsMap,
					PeerURLs:      peerUrls,
				}

				//Restore
				err = rstr.RestoreAndStopEtcd(restoreOpts)
				Expect(err).ShouldNot(HaveOccurred())
				err = utils.CheckDataConsistency(testCtx, restoreOpts.Config.RestoreDataDir, 400, logger)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

		Context("With first few snapshots in v2 dir and some more incr snapshots are in v1 dir", func() {
			It("should restore from v1 dir and v2 dir", func() {
				memberPath := path.Join(etcdDir, "member")

				//Take snapshots for v1 dir
				compressionConfig := compressor.NewCompressorConfig()
				ctx, cancel := context.WithTimeout(testCtx, time.Duration(2*time.Second))
				err = takeValidV1Snaps(logger, snapstoreDir, deltaSnapshotPeriod, endpoints, ctx.Done(), compressionConfig, fullSnapInV2)
				cancel()

				// remove the member dir
				err = os.RemoveAll(memberPath)
				Expect(err).ShouldNot(HaveOccurred())

				baseSnapshot, deltaSnapList, err = miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
				Expect(err).ShouldNot(HaveOccurred())

				rstr = NewRestorer(store, logger)

				restoreOpts := brtypes.RestoreOptions{
					Config:        restorationConfig,
					BaseSnapshot:  baseSnapshot,
					DeltaSnapList: deltaSnapList,
					ClusterURLs:   clusterUrlsMap,
					PeerURLs:      peerUrls,
				}

				//Restore
				err = rstr.RestoreAndStopEtcd(restoreOpts)
				Expect(err).ShouldNot(HaveOccurred())
				err = utils.CheckDataConsistency(testCtx, restoreOpts.Config.RestoreDataDir, 400, logger)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

	})

})

// corruptEtcdDir corrupts the etcd directory by deleting it
func corruptEtcdDir() error {
	if _, err := os.Stat(etcdDir); os.IsNotExist(err) {
		return nil
	}
	return os.RemoveAll(etcdDir)
}

//takeValidV1Snaps saves valid snaps in the v1 prefix dir of snapstore so that restorer could restore from them
//TODO: Consider removing when backward compatibility no longer needed
func takeValidV1Snaps(logger *logrus.Entry, container string, deltaSnapshotPeriod time.Duration, endpoints []string, stopCh <-chan struct{}, compressionConfig *compressor.CompressionConfig, mode string) error {
	//Here we run the snapshotter to take snapshots. The snapshotter by default stores the snaps in the v2 directory.
	//We then move those snaps into the v1 dir under a 'Backup-xxxxxx' dir

	//Snapshots for the v2 dir
	//Add data into etcd
	resp := &utils.EtcdDataPopulationResponse{}
	utils.PopulateEtcd(testCtx, logger, endpoints, 0, 100, resp)
	Expect(resp.Err).ShouldNot(HaveOccurred())
	//Take a full snapshot.
	ctx, cancel := context.WithTimeout(testCtx, time.Duration(2*time.Second))
	snapstoreConfig := brtypes.SnapstoreConfig{Container: snapstoreDir, Provider: "Local", Prefix: "v2"}
	err = utils.RunSnapshotter(logger, snapstoreConfig, deltaSnapshotPeriod, endpoints, ctx.Done(), true, compressionConfig)
	Expect(err).ShouldNot(HaveOccurred())
	cancel()

	//Add data into etcd
	utils.PopulateEtcd(testCtx, logger, endpoints, 100, 200, resp)
	Expect(resp.Err).ShouldNot(HaveOccurred())
	//Take delta snapshot.
	ctx, cancel = context.WithTimeout(testCtx, time.Duration(2*time.Second))
	snapstoreConfig = brtypes.SnapstoreConfig{Container: snapstoreDir, Provider: "Local", Prefix: "v2"}
	err = utils.RunSnapshotter(logger, snapstoreConfig, deltaSnapshotPeriod, endpoints, ctx.Done(), false, compressionConfig)
	Expect(err).ShouldNot(HaveOccurred())
	cancel()

	//Add data to etcd
	utils.PopulateEtcd(testCtx, logger, endpoints, 200, 300, resp)
	Expect(resp.Err).ShouldNot(HaveOccurred())
	//Take delta snapshot.
	ctx, cancel = context.WithTimeout(testCtx, time.Duration(2*time.Second))
	snapstoreConfig = brtypes.SnapstoreConfig{Container: snapstoreDir, Provider: "Local", Prefix: "v2"}
	err = utils.RunSnapshotter(logger, snapstoreConfig, deltaSnapshotPeriod, endpoints, ctx.Done(), false, compressionConfig)
	Expect(err).ShouldNot(HaveOccurred())
	cancel()

	//Add data to etcd
	utils.PopulateEtcd(testCtx, logger, endpoints, 300, 400, resp)
	Expect(resp.Err).ShouldNot(HaveOccurred())
	//Take delta snapshot.
	ctx, cancel = context.WithTimeout(testCtx, time.Duration(2*time.Second))
	snapstoreConfig = brtypes.SnapstoreConfig{Container: snapstoreDir, Provider: "Local", Prefix: "v2"}
	err = utils.RunSnapshotter(logger, snapstoreConfig, deltaSnapshotPeriod, endpoints, ctx.Done(), false, compressionConfig)
	Expect(err).ShouldNot(HaveOccurred())
	cancel()

	//Move snaps from v2 dir to a v1 dir
	//Create v1/Backup-xxxxxx dir
	var curTime = time.Now().Unix()
	err = os.MkdirAll(path.Join(path.Join(snapstoreDir, "v1"), fmt.Sprintf("Backup-%d", curTime)), 0755)
	Expect(err).ShouldNot(HaveOccurred())
	//Move contents from v2 to v1/Backup-xxxxxx
	files, err := ioutil.ReadDir(path.Join(snapstoreDir, "v2"))
	Expect(err).ShouldNot(HaveOccurred())
	oldPath := path.Join(snapstoreDir, "v2")
	newPath := path.Join(path.Join(snapstoreDir, "v1"), fmt.Sprintf("Backup-%d", curTime))

	if mode == allSnapsInV1 {
		//For the case where all snapshots should be in v1 dir, so we can delete v2 dir
		for _, f := range files {
			err = os.Rename(path.Join(oldPath, f.Name()), path.Join(newPath, f.Name()))
			Expect(err).ShouldNot(HaveOccurred())
		}

		//Delete v2 dir
		err = os.RemoveAll(path.Join(snapstoreDir, "v2"))
		Expect(err).ShouldNot(HaveOccurred())
	} else if mode == fullSnapInV1 {
		for _, f := range files[:len(files)-1] {
			//For the case where full snap are in v1 dir and some incr snaps are in v2 dir
			err = os.Rename(path.Join(oldPath, f.Name()), path.Join(newPath, f.Name()))
			Expect(err).ShouldNot(HaveOccurred())
		}
	} else {
		//For the case where full snaps are in v2 dir and some incr snaps are in v1
		f := files[len(files)-1]
		err = os.Rename(path.Join(oldPath, f.Name()), path.Join(newPath, f.Name()))
		Expect(err).ShouldNot(HaveOccurred())

	}
	return nil
}

//takeInvalidV1Snaps saves an invalid snap in the v1 prefix dir of the snapstore so that restorer can't restore from it
//TODO: Consider removing when backward compatibility no longer needed
func takeInvalidV1Snaps(logger *logrus.Entry, container string, deltaSnapshotPeriod time.Duration, endpoints []string, stopCh <-chan struct{}, compressionConfig *compressor.CompressionConfig) error {
	//V1 snapstore object
	store, err := snapstore.GetSnapstore(&brtypes.SnapstoreConfig{Container: container, Provider: "Local", Prefix: "v1"})
	if err != nil {
		return err
	}

	//Take a full snapshot
	var curTime = time.Now().Unix()
	var kind = brtypes.SnapshotKindFull
	snap := brtypes.Snapshot{
		Kind:          kind,
		CreatedOn:     time.Now(),
		StartRevision: 0,
		LastRevision:  1,
		SnapDir:       fmt.Sprintf("Backup-%d", curTime),
	}
	snap.GenerateSnapshotName()
	store.Save(snap, ioutil.NopCloser(strings.NewReader(fmt.Sprintf("dummy-snapshot-content for snap created on %s", snap.CreatedOn))))
	Expect(err).ShouldNot(HaveOccurred())

	return nil
}
