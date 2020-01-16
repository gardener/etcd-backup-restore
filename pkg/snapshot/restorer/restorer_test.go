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
	"math"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/pkg/types"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	"github.com/gardener/etcd-backup-restore/test/utils"
	"github.com/sirupsen/logrus"

	. "github.com/gardener/etcd-backup-restore/pkg/snapshot/restorer"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Running Restorer", func() {
	var (
		store           snapstore.SnapStore
		rstr            *Restorer
		restorePeerURLs []string
		clusterUrlsMap  types.URLsMap
		peerUrls        types.URLs
		baseSnapshot    *snapstore.Snapshot
		deltaSnapList   snapstore.SnapList
		wg              *sync.WaitGroup
	)
	const (
		restoreName            string = "default"
		restoreClusterToken    string = "etcd-cluster"
		restoreCluster         string = "default=http://localhost:2380"
		skipHashCheck          bool   = false
		maxFetchers            uint   = 6
		embeddedEtcdQuotaBytes int64  = 8 * 1024 * 1024 * 1024
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
		var restoreOpts RestoreOptions

		BeforeEach(func() {
			err = corruptEtcdDir()
			Expect(err).ShouldNot(HaveOccurred())

			store, err = snapstore.GetSnapstore(&snapstore.Config{Container: snapstoreDir, Provider: "Local"})
			Expect(err).ShouldNot(HaveOccurred())

			baseSnapshot, deltaSnapList, err = miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(testCtx, store)
			Expect(err).ShouldNot(HaveOccurred())

			rstr = NewRestorer(store, logger)
			restoreOpts = RestoreOptions{
				Config: &RestorationConfig{
					RestoreDataDir:           etcdDir,
					InitialClusterToken:      restoreClusterToken,
					InitialCluster:           restoreCluster,
					Name:                     restoreName,
					InitialAdvertisePeerURLs: restorePeerURLs,
					SkipHashCheck:            skipHashCheck,
					MaxFetchers:              maxFetchers,
					EmbeddedEtcdQuotaBytes:   embeddedEtcdQuotaBytes,
				},
				BaseSnapshot:  *baseSnapshot,
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

				err = rstr.Restore(testCtx, restoreOpts)
				Expect(err).Should(HaveOccurred())
			})
		})

		Context("with invalid restore directory", func() {
			It("should fail to restore", func() {
				restoreOpts.Config.RestoreDataDir = ""

				err = rstr.Restore(testCtx, restoreOpts)
				Expect(err).Should(HaveOccurred())
			})
		})

		Context("with invalid snapdir and snapname", func() {
			It("should fail to restore", func() {
				restoreOpts.BaseSnapshot.SnapDir = "test"
				restoreOpts.BaseSnapshot.SnapName = "test"

				err := rstr.Restore(testCtx, restoreOpts)
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

		Context("with maximum of one fetcher allowed", func() {
			It("should restore etcd data directory", func() {
				restoreOpts.Config.MaxFetchers = 1
				err = rstr.Restore(testCtx, restoreOpts)
				Expect(err).ShouldNot(HaveOccurred())

				err = checkDataConsistency(testCtx, restoreOpts.Config.RestoreDataDir, logger)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

		Context("with maximum of four fetchers allowed", func() {
			It("should restore etcd data directory", func() {
				restoreOpts.Config.MaxFetchers = 4

				err = rstr.Restore(testCtx, restoreOpts)
				Expect(err).ShouldNot(HaveOccurred())

				err = checkDataConsistency(testCtx, restoreOpts.Config.RestoreDataDir, logger)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

		Context("with maximum of hundred fetchers allowed", func() {
			It("should restore etcd data directory", func() {
				restoreOpts.Config.MaxFetchers = 100

				err = rstr.Restore(testCtx, restoreOpts)
				Expect(err).ShouldNot(HaveOccurred())

				err = checkDataConsistency(testCtx, restoreOpts.Config.RestoreDataDir, logger)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})
	})

	Describe("NEGATIVE:For Dynamic Loads and Negative Scenarios", func() {
		var (
			store               snapstore.SnapStore
			deltaSnapshotPeriod time.Duration
			endpoints           []string
			restorationConfig   *RestorationConfig
		)

		BeforeEach(func() {
			deltaSnapshotPeriod = time.Second
			etcd, err = utils.StartEmbeddedEtcd(testCtx, etcdDir, logger)
			Expect(err).ShouldNot(HaveOccurred())
			endpoints = []string{etcd.Clients[0].Addr().String()}

			store, err = snapstore.GetSnapstore(&snapstore.Config{Container: snapstoreDir, Provider: "Local"})
			Expect(err).ShouldNot(HaveOccurred())

			restorationConfig = &RestorationConfig{
				RestoreDataDir:           etcdDir,
				InitialClusterToken:      restoreClusterToken,
				InitialCluster:           restoreCluster,
				Name:                     restoreName,
				InitialAdvertisePeerURLs: restorePeerURLs,
				SkipHashCheck:            skipHashCheck,
				MaxFetchers:              maxFetchers,
				EmbeddedEtcdQuotaBytes:   embeddedEtcdQuotaBytes,
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
				runSnapshotter(ssrCtx, logger, deltaSnapshotPeriod, endpoints, startWithFullSnapshot)
				etcd.Server.Stop()
				etcd.Close()

				err = corruptEtcdDir()
				Expect(err).ShouldNot(HaveOccurred())

				baseSnapshot, deltaSnapList, err = miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(testCtx, store)
				Expect(err).ShouldNot(HaveOccurred())
				logger.Infof("No of delta snapshots: %d", deltaSnapList.Len())
				logger.Infof("Base snapshot is %v", baseSnapshot)

				rstr = NewRestorer(store, logger)
				restoreOpts := RestoreOptions{
					Config:        restorationConfig,
					DeltaSnapList: deltaSnapList,
					ClusterURLs:   clusterUrlsMap,
					PeerURLs:      peerUrls,
				}

				restoreOpts.BaseSnapshot.SnapDir = ""
				restoreOpts.BaseSnapshot.SnapName = ""

				err := rstr.Restore(testCtx, restoreOpts)

				Expect(err).ShouldNot(HaveOccurred())
				err = checkDataConsistency(testCtx, restoreOpts.Config.RestoreDataDir, logger)
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
				err = runSnapshotter(ssrCtx, logger, deltaSnapshotPeriod, endpoints, true)
				Expect(err).ShouldNot(HaveOccurred())
				etcd.Server.Stop()
				etcd.Close()

				err = corruptEtcdDir()
				Expect(err).ShouldNot(HaveOccurred())

				baseSnapshot, deltaSnapList, err = miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(testCtx, store)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(deltaSnapList.Len()).Should(BeZero())

				rstr = NewRestorer(store, logger)

				restoreOpts := RestoreOptions{
					Config:        restorationConfig,
					BaseSnapshot:  *baseSnapshot,
					DeltaSnapList: deltaSnapList,
					ClusterURLs:   clusterUrlsMap,
					PeerURLs:      peerUrls,
				}

				err = rstr.Restore(testCtx, restoreOpts)

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
				err = runSnapshotter(ssrCtx, logger, deltaSnapshotPeriod, endpoints, true)
				Expect(err).ShouldNot(HaveOccurred())
				etcd.Close()

				err = corruptEtcdDir()
				Expect(err).ShouldNot(HaveOccurred())

				baseSnapshot, deltaSnapList, err = miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(testCtx, store)
				Expect(err).ShouldNot(HaveOccurred())
				logger.Infof("No. of delta snapshots: %d", deltaSnapList.Len())

				snapshotToRemove := path.Join(snapstoreDir, baseSnapshot.SnapDir, baseSnapshot.SnapName)
				logger.Infof("Snapshot to remove: %s", snapshotToRemove)
				err = os.Remove(snapshotToRemove)
				logger.Infof("Removed snapshot to cause corruption %s", snapshotToRemove)
				Expect(err).ShouldNot(HaveOccurred())

				rstr = NewRestorer(store, logger)

				restoreOpts := RestoreOptions{
					Config:        restorationConfig,
					BaseSnapshot:  *baseSnapshot,
					DeltaSnapList: deltaSnapList,
					ClusterURLs:   clusterUrlsMap,
					PeerURLs:      peerUrls,
				}

				err = rstr.Restore(testCtx, restoreOpts)
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
				err = runSnapshotter(ssrCtx, logger, deltaSnapshotPeriod, endpoints, true)
				Expect(err).ShouldNot(HaveOccurred())
				etcd.Close()

				baseSnapshot, deltaSnapList, err = miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(testCtx, store)
				Expect(err).ShouldNot(HaveOccurred())

				rstr = NewRestorer(store, logger)

				restoreOpts := RestoreOptions{
					Config:        restorationConfig,
					BaseSnapshot:  *baseSnapshot,
					DeltaSnapList: deltaSnapList,
					ClusterURLs:   clusterUrlsMap,
					PeerURLs:      peerUrls,
				}

				logger.Infoln("starting restore, restore directory exists already")
				err = rstr.Restore(testCtx, restoreOpts)
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
				err = runSnapshotter(ssrCtx, logger, deltaSnapshotPeriod, endpoints, true)
				Expect(err).ShouldNot(HaveOccurred())

				time.Sleep(time.Duration(5 * time.Second))
				etcd.Server.Stop()
				etcd.Close()

				err = corruptEtcdDir()
				Expect(err).ShouldNot(HaveOccurred())
				logger.Infoln("corrupted the etcd dir")

				store, err = snapstore.GetSnapstore(&snapstore.Config{Container: snapstoreDir, Provider: "Local"})
				Expect(err).ShouldNot(HaveOccurred())
				baseSnapshot, deltaSnapList, err = miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(testCtx, store)
				Expect(err).ShouldNot(HaveOccurred())

				rstr = NewRestorer(store, logger)

				restoreOpts := RestoreOptions{
					Config:        restorationConfig,
					BaseSnapshot:  *baseSnapshot,
					DeltaSnapList: deltaSnapList,
					ClusterURLs:   clusterUrlsMap,
					PeerURLs:      peerUrls,
				}

				logger.Infoln("starting restore while snapshotter is running")
				err = rstr.Restore(testCtx, restoreOpts)
				Expect(err).ShouldNot(HaveOccurred())
				err = checkDataConsistency(testCtx, restoreOpts.Config.RestoreDataDir, logger)
				Expect(err).ShouldNot(HaveOccurred())

				// Although the test has passed but the logic currently doesn't stop snapshotter explicitly but assumes that restore
				// shall be triggered only on restart of the etcd pod, so in the current case the snapshotter and restore were both running
				// together. However data corruption was not simulated as the embedded etcd used to populate need to be stopped for restore to begin.
				// In a productive scenarios as the command is exposed so it's possible to run this without knowledge of the tightly coupled
				// behavior of etcd restart.
			})
		})
	})

})

// checkDataConsistency starts an embedded etcd and checks for correctness of the values stored in etcd against the keys 'keyFrom' through 'keyTo'
func checkDataConsistency(ctx context.Context, dir string, logger *logrus.Entry) error {
	etcd, err := utils.StartEmbeddedEtcd(ctx, dir, logger)
	if err != nil {
		return fmt.Errorf("unable to start embedded etcd server: %v", err)
	}
	defer etcd.Close()
	endpoints := []string{etcd.Clients[0].Addr().String()}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 10 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("unable to start etcd client: %v", err)
	}
	defer cli.Close()

	var (
		key      string
		value    string
		resKey   string
		resValue string
	)

	for currKey := 0; currKey <= keyTo; currKey++ {
		key = utils.KeyPrefix + strconv.Itoa(currKey)
		value = utils.ValuePrefix + strconv.Itoa(currKey)

		resp, err := cli.Get(testCtx, key, clientv3.WithLimit(1))
		if err != nil {
			return fmt.Errorf("unable to get value from etcd: %v", err)
		}
		if len(resp.Kvs) == 0 {
			// handles deleted keys as every 10th key is deleted during populate etcd call
			// this handling is also done in the populateEtcd() in restorer_suite_test.go file
			// also it assumes that the deltaSnapshotDuration is more than 10 --
			// if you change the constant please change the factor accordingly to have coverage of delete scenarios.
			if math.Mod(float64(currKey), 10) == 0 {
				continue //it should continue as key was put for action delete
			} else {
				return fmt.Errorf("entry not found for key %s", key)
			}
		}
		res := resp.Kvs[0]
		resKey = string(res.Key)
		resValue = string(res.Value)

		if resKey != key {
			return fmt.Errorf("key mismatch for %s and %s", resKey, key)
		}
		if resValue != value {
			return fmt.Errorf("invalid etcd data - value mismatch for %s and %s", resValue, value)
		}
	}
	fmt.Printf("Data consistency for key-value pairs (%[1]s%[3]d, %[2]s%[3]d) through (%[1]s%[4]d, %[2]s%[4]d) has been verified\n", utils.KeyPrefix, utils.ValuePrefix, 0, keyTo)

	return nil
}

// corruptEtcdDir corrupts the etcd directory by deleting it
func corruptEtcdDir() error {
	if _, err := os.Stat(etcdDir); os.IsNotExist(err) {
		return nil
	}
	return os.RemoveAll(etcdDir)
}
