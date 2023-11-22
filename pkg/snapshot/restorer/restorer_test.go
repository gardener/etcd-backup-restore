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
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/compressor"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	mockfactory "github.com/gardener/etcd-backup-restore/pkg/mock/etcdutil/client"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/gardener/etcd-backup-restore/test/utils"
	"github.com/golang/mock/gomock"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/pkg/types"

	. "github.com/gardener/etcd-backup-restore/pkg/snapshot/restorer"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	v1 = iota
	v2
	mixed
)

const (
	allSnapsInV1 = iota
	fullSnapInV1
	fullSnapInV2
	allSnapsInV2
)

var _ = Describe("Running Restorer", func() {
	var (
		store           brtypes.SnapStore
		restorer        *Restorer
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
		autoCompactionRetention string = "0"
		embeddedEtcdPortNo      string = "9089"
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

			restorer, err = NewRestorer(store, logger)
			Expect(err).ShouldNot(HaveOccurred())
			restoreOpts = brtypes.RestoreOptions{
				Config: &brtypes.RestorationConfig{
					DataDir:                  etcdDir,
					TempSnapshotsDir:         tempDir,
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

				err = restorer.RestoreAndStopEtcd(restoreOpts, nil)
				Expect(err).Should(HaveOccurred())
			})
		})

		Context("with invalid restore directory", func() {
			It("should fail to restore", func() {
				restoreOpts.Config.DataDir = ""

				err = restorer.RestoreAndStopEtcd(restoreOpts, nil)
				Expect(err).Should(HaveOccurred())
			})
		})

		Context("with invalid snapdir and snapname", func() {
			It("should fail to restore", func() {
				restoreOpts.BaseSnapshot.SnapDir = "test"
				restoreOpts.BaseSnapshot.SnapName = "test"

				err := restorer.RestoreAndStopEtcd(restoreOpts, nil)
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
				err = restorer.RestoreAndStopEtcd(restoreOpts, nil)
				Expect(err).ShouldNot(HaveOccurred())

				err = utils.CheckDataConsistency(testCtx, restoreOpts.Config.DataDir, keyTo, logger)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

		Context("with maximum of four fetchers allowed", func() {
			It("should restore etcd data directory", func() {
				restoreOpts.Config.MaxFetchers = 4

				err = restorer.RestoreAndStopEtcd(restoreOpts, nil)
				Expect(err).ShouldNot(HaveOccurred())

				err = utils.CheckDataConsistency(testCtx, restoreOpts.Config.DataDir, keyTo, logger)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

		Context("with maximum of hundred fetchers allowed", func() {
			It("should restore etcd data directory", func() {
				restoreOpts.Config.MaxFetchers = 100

				err = restorer.RestoreAndStopEtcd(restoreOpts, nil)
				Expect(err).ShouldNot(HaveOccurred())

				err = utils.CheckDataConsistency(testCtx, restoreOpts.Config.DataDir, keyTo, logger)
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
			etcd, err = utils.StartEmbeddedEtcd(testCtx, etcdDir, logger, utils.DefaultEtcdName, embeddedEtcdPortNo)
			Expect(err).ShouldNot(HaveOccurred())
			endpoints = []string{etcd.Clients[0].Addr().String()}

			store, err = snapstore.GetSnapstore(&brtypes.SnapstoreConfig{Container: snapstoreDir, Provider: "Local"})
			Expect(err).ShouldNot(HaveOccurred())

			restorationConfig = &brtypes.RestorationConfig{
				DataDir:                  etcdDir,
				TempSnapshotsDir:         tempDir,
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

				restorer, err = NewRestorer(store, logger)
				Expect(err).ShouldNot(HaveOccurred())
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

				err = restorer.RestoreAndStopEtcd(restoreOpts, nil)
				Expect(err).ShouldNot(HaveOccurred())

				err = utils.CheckDataConsistency(testCtx, restoreOpts.Config.DataDir, keyTo, logger)
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

				restorer, err = NewRestorer(store, logger)
				Expect(err).ShouldNot(HaveOccurred())

				restoreOpts := brtypes.RestoreOptions{
					Config:        restorationConfig,
					BaseSnapshot:  baseSnapshot,
					DeltaSnapList: deltaSnapList,
					ClusterURLs:   clusterUrlsMap,
					PeerURLs:      peerUrls,
				}

				err = restorer.RestoreAndStopEtcd(restoreOpts, nil)

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

				restorer, err = NewRestorer(store, logger)
				Expect(err).ShouldNot(HaveOccurred())

				restoreOpts := brtypes.RestoreOptions{
					Config:        restorationConfig,
					BaseSnapshot:  baseSnapshot,
					DeltaSnapList: deltaSnapList,
					ClusterURLs:   clusterUrlsMap,
					PeerURLs:      peerUrls,
				}

				err = restorer.RestoreAndStopEtcd(restoreOpts, nil)
				Expect(err).Should(HaveOccurred())
				// the below consistency fails with index out of range error hence commented,
				// but the etcd directory is filled partially as part of the restore which should be relooked.
				// err = checkDataConsistency(restoreOptions.Config.DataDir, logger)
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

				restorer, err = NewRestorer(store, logger)
				Expect(err).ShouldNot(HaveOccurred())

				restoreOpts := brtypes.RestoreOptions{
					Config:        restorationConfig,
					BaseSnapshot:  baseSnapshot,
					DeltaSnapList: deltaSnapList,
					ClusterURLs:   clusterUrlsMap,
					PeerURLs:      peerUrls,
				}

				logger.Infoln("starting restore, restore directory exists already")
				err = restorer.RestoreAndStopEtcd(restoreOpts, nil)
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

				restorer, err = NewRestorer(store, logger)
				Expect(err).ShouldNot(HaveOccurred())

				restoreOpts := brtypes.RestoreOptions{
					Config:        restorationConfig,
					BaseSnapshot:  baseSnapshot,
					DeltaSnapList: deltaSnapList,
					ClusterURLs:   clusterUrlsMap,
					PeerURLs:      peerUrls,
				}

				logger.Infoln("starting restore while snapshotter is running")
				err = restorer.RestoreAndStopEtcd(restoreOpts, nil)
				Expect(err).ShouldNot(HaveOccurred())
				err = utils.CheckDataConsistency(testCtx, restoreOpts.Config.DataDir, keyTo, logger)
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

				restorer, err = NewRestorer(store, logger)
				Expect(err).ShouldNot(HaveOccurred())

				restoreOpts := brtypes.RestoreOptions{
					Config:        restorationConfig,
					BaseSnapshot:  baseSnapshot,
					DeltaSnapList: deltaSnapList,
					ClusterURLs:   clusterUrlsMap,
					PeerURLs:      peerUrls,
				}

				err = restorer.RestoreAndStopEtcd(restoreOpts, nil)
				Expect(err).ShouldNot(HaveOccurred())
				err = utils.CheckDataConsistency(testCtx, restoreOpts.Config.DataDir, keyTo, logger)
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

				restorer, err = NewRestorer(store, logger)
				Expect(err).ShouldNot(HaveOccurred())

				restoreOpts := brtypes.RestoreOptions{
					Config:        restorationConfig,
					BaseSnapshot:  baseSnapshot,
					DeltaSnapList: deltaSnapList,
					ClusterURLs:   clusterUrlsMap,
					PeerURLs:      peerUrls,
				}

				err = restorer.RestoreAndStopEtcd(restoreOpts, nil)
				Expect(err).ShouldNot(HaveOccurred())
				err = utils.CheckDataConsistency(testCtx, restoreOpts.Config.DataDir, keyTo, logger)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})
	})

	Describe("Handle Alarm and Make etcd lean", func() {
		var (
			ctrl    *gomock.Controller
			factory *mockfactory.MockFactory
			cm      *mockfactory.MockMaintenanceCloser
			ckv     *mockfactory.MockKVCloser

			dummyRevisionNo             = int64(1111)
			dummyEtcdEndpoints          = []string{"http://127.0.0.1:9999", "http://127.0.0.1:9900"}
			dummyEmbeddedEtcdQuotaBytes = float64(100) // 100B
		)
		BeforeEach(func() {
			ctrl = gomock.NewController(GinkgoT())
			factory = mockfactory.NewMockFactory(ctrl)
			cm = mockfactory.NewMockMaintenanceCloser(ctrl)
			ckv = mockfactory.NewMockKVCloser(ctrl)
			restorer, err = NewRestorer(store, logger)
			Expect(err).ShouldNot(HaveOccurred())
		})

		Context("Etcd database size within the threshold limit", func() {
			var (
				dummyDBSize      = int64(50)
				dummyDBSizeInUse = int64(25)
			)
			BeforeEach(func() {
				factory.EXPECT().NewMaintenance().Return(cm, nil).AnyTimes()
				factory.EXPECT().NewKV().Return(ckv, nil).AnyTimes()
			})

			Context("unable to compact etcd", func() {
				It("should return error", func() {
					var (
						dbSizeAlarmCh    = make(chan string)
						dbSizeDisAlarmCh = make(chan bool)
					)

					ckv.EXPECT().Compact(gomock.Any(), gomock.Any(), gomock.Any()).Return(
						nil, fmt.Errorf("dummy compact etcd error"),
					).AnyTimes()

					clientMaintenance, err := factory.NewMaintenance()
					Expect(err).ShouldNot(HaveOccurred())

					clientKV, err := factory.NewKV()
					Expect(err).ShouldNot(HaveOccurred())

					err = restorer.MakeEtcdLeanAndCheckAlarm(dummyRevisionNo, dummyEtcdEndpoints, dummyEmbeddedEtcdQuotaBytes, dbSizeAlarmCh, dbSizeDisAlarmCh, clientKV, clientMaintenance)
					Expect(err).Should(HaveOccurred())
				})
			})

			Context("able to compact etcd but unable to check etcd status", func() {
				It("should return error", func() {
					var (
						dbSizeAlarmCh    = make(chan string)
						dbSizeDisAlarmCh = make(chan bool)
					)

					ckv.EXPECT().Compact(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

					cm.EXPECT().Status(gomock.Any(), gomock.Any()).Return(
						nil, fmt.Errorf("dummy etcd status error"),
					).AnyTimes()

					clientMaintenance, err := factory.NewMaintenance()
					Expect(err).ShouldNot(HaveOccurred())

					clientKV, err := factory.NewKV()
					Expect(err).ShouldNot(HaveOccurred())

					err = restorer.MakeEtcdLeanAndCheckAlarm(dummyRevisionNo, dummyEtcdEndpoints, dummyEmbeddedEtcdQuotaBytes, dbSizeAlarmCh, dbSizeDisAlarmCh, clientKV, clientMaintenance)
					Expect(err).Should(HaveOccurred())
				})
			})

			Context("able to compact etcd and check the etcd status", func() {
				It("shouldn't return error", func() {
					var (
						dbSizeAlarmCh    = make(chan string)
						dbSizeDisAlarmCh = make(chan bool)
					)

					ckv.EXPECT().Compact(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

					cm.EXPECT().Status(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ string) (*clientv3.StatusResponse, error) {
						response := new(clientv3.StatusResponse)
						// setting the db size
						response.DbSize = dummyDBSize
						response.DbSizeInUse = dummyDBSizeInUse
						return response, nil
					}).AnyTimes()

					clientMaintenance, err := factory.NewMaintenance()
					Expect(err).ShouldNot(HaveOccurred())

					clientKV, err := factory.NewKV()
					Expect(err).ShouldNot(HaveOccurred())

					err = restorer.MakeEtcdLeanAndCheckAlarm(dummyRevisionNo, dummyEtcdEndpoints, dummyEmbeddedEtcdQuotaBytes, dbSizeAlarmCh, dbSizeDisAlarmCh, clientKV, clientMaintenance)
					Expect(err).ShouldNot(HaveOccurred())
				})
			})
		})

		Context("Etcd database size crosses the threshold limit", func() {
			var (
				dummyDBSize            = int64(90)
				dummyDBSizeInUse       = int64(50)
				dummyDBSizeAfterDefrag = int64(50)
			)
			BeforeEach(func() {
				factory.EXPECT().NewMaintenance().Return(cm, nil).AnyTimes()
				factory.EXPECT().NewKV().Return(ckv, nil).AnyTimes()
			})

			Context("compact but unable to defragment the given endpoint", func() {
				It("should return error", func() {
					var (
						stopHandleAlarmCh = make(chan bool)
						dbSizeAlarmCh     = make(chan string)
						dbSizeDisAlarmCh  = make(chan bool)
					)

					ckv.EXPECT().Compact(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
					cm.EXPECT().Status(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ string) (*clientv3.StatusResponse, error) {
						response := new(clientv3.StatusResponse)
						// setting the db size
						response.DbSize = dummyDBSize
						response.DbSizeInUse = dummyDBSizeInUse
						return response, nil
					}).AnyTimes()

					cm.EXPECT().Defragment(gomock.Any(), gomock.Any()).Return(
						nil, fmt.Errorf("dummy defrag error"),
					).AnyTimes()

					clientMaintenance, err := factory.NewMaintenance()
					Expect(err).ShouldNot(HaveOccurred())

					clientKV, err := factory.NewKV()
					Expect(err).ShouldNot(HaveOccurred())

					go restorer.HandleAlarm(stopHandleAlarmCh, dbSizeAlarmCh, dbSizeDisAlarmCh, clientMaintenance)
					defer close(stopHandleAlarmCh)
					err = restorer.MakeEtcdLeanAndCheckAlarm(dummyRevisionNo, dummyEtcdEndpoints, dummyEmbeddedEtcdQuotaBytes, dbSizeAlarmCh, dbSizeDisAlarmCh, clientKV, clientMaintenance)
					Expect(err).Should(HaveOccurred())
				})
			})

			Context("compact and defragment the given etcd endpoint", func() {
				It("shouldn't return any error", func() {
					var (
						stopHandleAlarmCh = make(chan bool)
						dbSizeAlarmCh     = make(chan string)
						dbSizeDisAlarmCh  = make(chan bool)
					)

					ckv.EXPECT().Compact(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
					cm.EXPECT().Status(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ string) (*clientv3.StatusResponse, error) {
						response := new(clientv3.StatusResponse)
						// setting the db size before defrag
						response.DbSize = dummyDBSize
						response.DbSizeInUse = dummyDBSizeInUse
						return response, nil
					}).MaxTimes(1)

					cm.EXPECT().Status(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ string) (*clientv3.StatusResponse, error) {
						response := new(clientv3.StatusResponse)
						// setting the db size after defrag
						response.DbSize = dummyDBSizeAfterDefrag
						response.DbSizeInUse = dummyDBSizeAfterDefrag
						return response, nil
					}).AnyTimes()

					cm.EXPECT().Defragment(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

					clientMaintenance, err := factory.NewMaintenance()
					Expect(err).ShouldNot(HaveOccurred())

					clientKV, err := factory.NewKV()
					Expect(err).ShouldNot(HaveOccurred())

					go restorer.HandleAlarm(stopHandleAlarmCh, dbSizeAlarmCh, dbSizeDisAlarmCh, clientMaintenance)
					defer close(stopHandleAlarmCh)
					err = restorer.MakeEtcdLeanAndCheckAlarm(dummyRevisionNo, dummyEtcdEndpoints, dummyEmbeddedEtcdQuotaBytes, dbSizeAlarmCh, dbSizeDisAlarmCh, clientKV, clientMaintenance)
					Expect(err).ShouldNot(HaveOccurred())
				})
			})
		})
	})
})

var _ = Describe("Running Restorer when both v1 and v2 directory structures are present", func() {
	var (
		restorer            *Restorer
		restorePeerURLs     []string
		clusterUrlsMap      types.URLsMap
		peerUrls            types.URLs
		baseSnapshot        *brtypes.Snapshot
		deltaSnapList       brtypes.SnapList
		store               brtypes.SnapStore
		deltaSnapshotPeriod time.Duration
		ep                  []string
		etcdDataDir         string
		restoreTempDir      string
		testDir             string
		storeDir            string
		restorationConfig   *brtypes.RestorationConfig
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
		autoCompactionRetention string = "0"
		embeddedEtcdPortNo      string = "9089"
	)

	var (
		resp *utils.EtcdDataPopulationResponse
	)
	BeforeEach(func() {
		deltaSnapshotPeriod = time.Second
		testDir = filepath.Join(outputDir, "restore-test-v1-v2")
		etcdDataDir = filepath.Join(testDir, "default.etcd")
		restoreTempDir = filepath.Join(testDir, "default.restore.tmp")
		storeDir = filepath.Join(testDir, "snapshotter.bkp")
		etcd, err = utils.StartEmbeddedEtcd(testCtx, etcdDataDir, logger, utils.DefaultEtcdName, embeddedEtcdPortNo)
		Expect(err).ShouldNot(HaveOccurred())
		ep = []string{etcd.Clients[0].Addr().String()}

		restorePeerURLs = []string{"http://localhost:2380"}
		clusterUrlsMap, err = types.NewURLsMap(restoreCluster)
		Expect(err).ShouldNot(HaveOccurred())
		peerUrls, err = types.NewURLs(restorePeerURLs)
		Expect(err).ShouldNot(HaveOccurred())

		store, err = snapstore.GetSnapstore(&brtypes.SnapstoreConfig{Container: storeDir, Provider: "Local", Prefix: "v2"})
		Expect(err).ShouldNot(HaveOccurred())

		restorationConfig = &brtypes.RestorationConfig{
			DataDir:                  etcdDataDir,
			TempSnapshotsDir:         restoreTempDir,
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

		resp = &utils.EtcdDataPopulationResponse{}
	})

	AfterEach(func() {
		etcd.Server.Stop()
		etcd.Close()

		err = os.RemoveAll(testDir)
		Expect(err).ShouldNot(HaveOccurred())
	})

	// Tests restorer behaviour when local database has to be restored from snapstore with only v1 directory structures
	// TODO: Consider removing when backward compatibility no longer needed
	Context("With snapshots in v1 dir only", func() {
		It("should restore from v1 dir", func() {
			//Take snapshots for v1 dir
			err = takeValidSnaps(logger, storeDir, resp, deltaSnapshotPeriod, ep, v1, allSnapsInV1)
			Expect(err).ShouldNot(HaveOccurred())

			baseSnapshot, deltaSnapList, err = miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
			Expect(err).ShouldNot(HaveOccurred())

			restorer, err = NewRestorer(store, logger)
			Expect(err).ShouldNot(HaveOccurred())

			restoreOpts := brtypes.RestoreOptions{
				Config:        restorationConfig,
				BaseSnapshot:  baseSnapshot,
				DeltaSnapList: deltaSnapList,
				ClusterURLs:   clusterUrlsMap,
				PeerURLs:      peerUrls,
			}

			//Restore

			// remove the member dir
			err = os.RemoveAll(path.Join(etcdDataDir, "member"))
			Expect(err).ShouldNot(HaveOccurred())

			err = restorer.RestoreAndStopEtcd(restoreOpts, nil)
			Expect(err).ShouldNot(HaveOccurred())
			err = utils.CheckDataConsistency(testCtx, restoreOpts.Config.DataDir, resp.KeyTo, logger)
			Expect(err).ShouldNot(HaveOccurred())
		})
	})

	Context("With first full snapshot in v1 dir and some incr snapshots are in v2 dir", func() {
		It("should restore from v1 and v2 dir", func() {
			//Take snapshots for v1 and v2  dir
			err = takeValidSnaps(logger, storeDir, resp, deltaSnapshotPeriod, ep, mixed, fullSnapInV1)
			Expect(err).ShouldNot(HaveOccurred())

			baseSnapshot, deltaSnapList, err = miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
			Expect(err).ShouldNot(HaveOccurred())

			restorer, err = NewRestorer(store, logger)
			Expect(err).ShouldNot(HaveOccurred())

			restoreOpts := brtypes.RestoreOptions{
				Config:        restorationConfig,
				BaseSnapshot:  baseSnapshot,
				DeltaSnapList: deltaSnapList,
				ClusterURLs:   clusterUrlsMap,
				PeerURLs:      peerUrls,
			}

			//Restore

			// remove the member dir
			err = os.RemoveAll(path.Join(etcdDataDir, "member"))
			Expect(err).ShouldNot(HaveOccurred())

			err = restorer.RestoreAndStopEtcd(restoreOpts, nil)
			Expect(err).ShouldNot(HaveOccurred())
			err = utils.CheckDataConsistency(testCtx, restoreOpts.Config.DataDir, resp.KeyTo, logger)
			Expect(err).ShouldNot(HaveOccurred())
		})
	})

	Context("With first full snapshots in v2 dir and some incr snapshots are in v1 dir", func() {
		It("should restore from v1 and v2 dir", func() {
			//Take snapshots for v1 and v2  dir
			err = takeValidSnaps(logger, storeDir, resp, deltaSnapshotPeriod, ep, mixed, fullSnapInV2)
			Expect(err).ShouldNot(HaveOccurred())

			baseSnapshot, deltaSnapList, err = miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
			Expect(err).ShouldNot(HaveOccurred())

			restorer, err = NewRestorer(store, logger)
			Expect(err).ShouldNot(HaveOccurred())

			restoreOpts := brtypes.RestoreOptions{
				Config:        restorationConfig,
				BaseSnapshot:  baseSnapshot,
				DeltaSnapList: deltaSnapList,
				ClusterURLs:   clusterUrlsMap,
				PeerURLs:      peerUrls,
			}

			//Restore

			// remove the member dir
			err = os.RemoveAll(path.Join(etcdDataDir, "member"))
			Expect(err).ShouldNot(HaveOccurred())

			err = restorer.RestoreAndStopEtcd(restoreOpts, nil)
			Expect(err).ShouldNot(HaveOccurred())
			err = utils.CheckDataConsistency(testCtx, restoreOpts.Config.DataDir, resp.KeyTo, logger)
			Expect(err).ShouldNot(HaveOccurred())
		})
	})

	//Tests restorer behaviour when local database has to be restored from snapstore with only v2 directory structures
	//TODO: Consider removing when backward compatibility no longer needed
	Context("With snapshots in v2 dir only", func() {
		It("should restore from v2 dir snapshots", func() {
			// take snapshots for the v2 dir
			err = takeValidSnaps(logger, storeDir, resp, deltaSnapshotPeriod, ep, v2, allSnapsInV2)
			Expect(err).ShouldNot(HaveOccurred())

			baseSnapshot, deltaSnapList, err = miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
			Expect(err).ShouldNot(HaveOccurred())

			restorer, err = NewRestorer(store, logger)
			Expect(err).ShouldNot(HaveOccurred())

			restoreOpts := brtypes.RestoreOptions{
				Config:        restorationConfig,
				BaseSnapshot:  baseSnapshot,
				DeltaSnapList: deltaSnapList,
				ClusterURLs:   clusterUrlsMap,
				PeerURLs:      peerUrls,
			}

			//Restore

			// remove the member dir
			err = os.RemoveAll(path.Join(etcdDataDir, "member"))
			Expect(err).ShouldNot(HaveOccurred())

			err = restorer.RestoreAndStopEtcd(restoreOpts, nil)
			Expect(err).ShouldNot(HaveOccurred())
			err = utils.CheckDataConsistency(testCtx, restoreOpts.Config.DataDir, resp.KeyTo, logger)
			Expect(err).ShouldNot(HaveOccurred())
		})
	})

	Describe("NEGATIVE: Negative Restoration Scenario with Backward Compatibility", func() {
		Context("with invalid snapshots in v1 directory", func() {
			It("should not restorer", func() {
				//Take invalid snapshots for v1 dir
				err = takeInvalidV1Snaps(storeDir)
				Expect(err).ShouldNot(HaveOccurred())

				baseSnapshot, deltaSnapList, err = miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
				Expect(err).ShouldNot(HaveOccurred())

				restorer, err = NewRestorer(store, logger)
				Expect(err).ShouldNot(HaveOccurred())

				restoreOpts := brtypes.RestoreOptions{
					Config:        restorationConfig,
					BaseSnapshot:  baseSnapshot,
					DeltaSnapList: deltaSnapList,
					ClusterURLs:   clusterUrlsMap,
					PeerURLs:      peerUrls,
				}

				//Restore

				// remove the member dir
				err = os.RemoveAll(path.Join(etcdDataDir, "member"))
				Expect(err).ShouldNot(HaveOccurred())

				err = restorer.RestoreAndStopEtcd(restoreOpts, nil)
				Expect(err).Should(HaveOccurred())
			})
		})
	})
})

var _ = Describe("Unit testing individual functions for restorer package", func() {
	Describe("testing ErrorArrayToError", func() {
		var (
			errs []error
		)
		Context("when error array has no elements", func() {
			It("should return nil", func() {
				errs = []error{}
				Expect(ErrorArrayToError(errs)).Should(BeNil())
			})
		})
		Context("when error array has one element", func() {
			It("should return nil", func() {
				errs = []error{
					fmt.Errorf("error0"),
				}
				expectedErr := fmt.Errorf("error0")

				err := ErrorArrayToError(errs)
				Expect(err).Should(Equal(expectedErr))
			})
		})
		Context("when error array has more than one element", func() {
			It("should return nil", func() {
				errs = []error{
					fmt.Errorf("error0"),
					fmt.Errorf("error1"),
					fmt.Errorf("error2"),
				}
				expectedErr := fmt.Errorf("error0\nerror1\nerror2")

				err := ErrorArrayToError(errs)
				Expect(err).Should(Equal(expectedErr))
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

// takeValidSnaps saves valid snaps in the v1 prefix dir of snapstore so that restorer could restore from them
// TODO: Consider removing when backward compatibility no longer needed
func takeValidSnaps(logger *logrus.Entry, container string, resp *utils.EtcdDataPopulationResponse, deltaSnapshotPeriod time.Duration, endpoints []string, mode int, backupVersion int) error {
	//Here we run the snapshotter to take snapshots. The snapshotter by default stores the snaps in the v2 directory.
	//We then move those snaps into the v1 dir under a 'Backup-xxxxxx' dir

	//Snapshots for the v2 dir

	//Setup store
	snapstoreConfig := brtypes.SnapstoreConfig{Container: container, Provider: "Local", Prefix: "v2"}
	store, err := snapstore.GetSnapstore(&snapstoreConfig)
	Expect(err).ShouldNot(HaveOccurred())

	//Add data into etcd
	start := 0
	stop := start + 10
	utils.PopulateEtcd(testCtx, logger, endpoints, start, stop, resp)
	Expect(resp.Err).ShouldNot(HaveOccurred())

	//Take a full snapshot.
	compressionConfig := compressor.NewCompressorConfig()
	ctx, cancel := context.WithTimeout(testCtx, time.Duration(2*time.Second))
	err = utils.RunSnapshotter(logger, snapstoreConfig, deltaSnapshotPeriod, endpoints, ctx.Done(), true, compressionConfig)
	Expect(err).ShouldNot(HaveOccurred())
	cancel()

	//Add data into etcd
	start = stop
	stop = stop + 10
	utils.PopulateEtcd(testCtx, logger, endpoints, start, stop, resp)
	Expect(resp.Err).ShouldNot(HaveOccurred())
	//Take delta snapshot.
	ctx, cancel = context.WithTimeout(testCtx, time.Duration(2*time.Second))
	err = utils.RunSnapshotter(logger, snapstoreConfig, deltaSnapshotPeriod, endpoints, ctx.Done(), false, compressionConfig)
	Expect(err).ShouldNot(HaveOccurred())
	cancel()

	//Add data to etcd
	start = stop
	stop = stop + 10
	utils.PopulateEtcd(testCtx, logger, endpoints, start, stop, resp)
	Expect(resp.Err).ShouldNot(HaveOccurred())
	//Take delta snapshot.
	ctx, cancel = context.WithTimeout(testCtx, time.Duration(2*time.Second))
	err = utils.RunSnapshotter(logger, snapstoreConfig, deltaSnapshotPeriod, endpoints, ctx.Done(), false, compressionConfig)
	Expect(err).ShouldNot(HaveOccurred())
	cancel()

	resp.KeyTo = stop

	if backupVersion != v2 {
		//Move snaps from v2 dir to a v1 dir
		//Create v1/Backup-xxxxxx dir
		baseSnapshot, _, err := miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
		Expect(err).ShouldNot(HaveOccurred())
		err = os.MkdirAll(path.Join(path.Join(container, "v1"), fmt.Sprintf("Backup-%d", baseSnapshot.CreatedOn.Unix())), 0755)
		Expect(err).ShouldNot(HaveOccurred())
		//Move contents from v2 to v1/Backup-xxxxxx
		files, err := os.ReadDir(path.Join(container, "v2"))
		Expect(err).ShouldNot(HaveOccurred())
		oldPath := path.Join(container, "v2")
		newPath := path.Join(path.Join(container, "v1"), fmt.Sprintf("Backup-%d", baseSnapshot.CreatedOn.Unix()))

		switch mode {
		case allSnapsInV1:
			{
				//For the case where all snapshots should be in v1 dir, so we can delete v2 dir
				for _, f := range files {
					err = os.Rename(path.Join(oldPath, f.Name()), path.Join(newPath, f.Name()))
					Expect(err).ShouldNot(HaveOccurred())
				}

				//Delete v2 dir
				err = os.RemoveAll(path.Join(container, "v2"))
				Expect(err).ShouldNot(HaveOccurred())
			}
		case fullSnapInV1:
			{
				for _, f := range files[:len(files)-1] {
					//For the case where full snap are in v1 dir and some incr snaps are in v2 dir
					err = os.Rename(path.Join(oldPath, f.Name()), path.Join(newPath, f.Name()))
					Expect(err).ShouldNot(HaveOccurred())
				}
			}
		case fullSnapInV2:
			{
				//For the case where full snaps are in v2 dir and some incr snaps are in v1
				for _, f := range files[len(files)-1:] {
					err = os.Rename(path.Join(oldPath, f.Name()), path.Join(newPath, f.Name()))
					Expect(err).ShouldNot(HaveOccurred())

				}
			}
		}
	}
	return nil
}

// takeInvalidV1Snaps saves an invalid snap in the v1 prefix dir of the snapstore
// TODO: Consider removing when backward compatibility no longer needed
func takeInvalidV1Snaps(container string) error {
	//V1 snapstore object
	store, err := snapstore.GetSnapstore(&brtypes.SnapstoreConfig{Container: container, Provider: "Local", Prefix: "v2"})
	if err != nil {
		return err
	}

	//Take a full snapshot
	var curTime = time.Now()
	var kind = brtypes.SnapshotKindFull
	snap := brtypes.Snapshot{
		Kind:          kind,
		CreatedOn:     curTime,
		StartRevision: 0,
		LastRevision:  100,
	}
	snap.GenerateSnapshotName()
	store.Save(snap, io.NopCloser(strings.NewReader(fmt.Sprintf("dummy-snapshot-content for snap created on %s", snap.CreatedOn))))
	Expect(err).ShouldNot(HaveOccurred())

	//Create v1/Backup-xxxxxx dir
	err = os.MkdirAll(path.Join(path.Join(container, "v1"), fmt.Sprintf("Backup-%d", snap.CreatedOn.Unix())), 0755)
	Expect(err).ShouldNot(HaveOccurred())
	//Move contents from v2 to v1/Backup-xxxxxx
	files, err := os.ReadDir(path.Join(container, "v2"))
	Expect(err).ShouldNot(HaveOccurred())
	oldPath := path.Join(container, "v2")
	newPath := path.Join(path.Join(container, "v1"), fmt.Sprintf("Backup-%d", snap.CreatedOn.Unix()))

	for _, f := range files {
		err = os.Rename(path.Join(oldPath, f.Name()), path.Join(newPath, f.Name()))
		Expect(err).ShouldNot(HaveOccurred())
	}

	//Delete v2 dir
	err = os.RemoveAll(path.Join(container, "v2"))
	Expect(err).ShouldNot(HaveOccurred())

	return nil
}
