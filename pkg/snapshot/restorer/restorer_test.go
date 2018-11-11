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
	"os"
	"strconv"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/pkg/types"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"

	. "github.com/gardener/etcd-backup-restore/pkg/snapshot/restorer"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
)

var _ = Describe("Running Restorer", func() {
	var (
		logger *logrus.Logger

		store snapstore.SnapStore
		rstr  *Restorer

		restoreCluster      string
		restoreClusterToken string
		restoreDataDir      string
		restorePeerURLs     []string
		restoreName         string
		skipHashCheck       bool
		maxFetchers         int

		clusterUrlsMap types.URLsMap
		peerUrls       types.URLs
		baseSnapshot   *snapstore.Snapshot
		deltaSnapList  snapstore.SnapList
	)

	BeforeEach(func() {
		fmt.Println("Initializing snapstore and restorer")

		logger = logrus.New()
		restoreDataDir = etcdDir
		restoreClusterToken = "etcd-cluster"
		restoreName = "default"
		restoreCluster = restoreName + "=http://localhost:2380"
		restorePeerURLs = []string{"http://localhost:2380"}
		clusterUrlsMap, err = types.NewURLsMap(restoreCluster)
		Expect(err).ShouldNot(HaveOccurred())
		peerUrls, err = types.NewURLs(restorePeerURLs)
		Expect(err).ShouldNot(HaveOccurred())
		skipHashCheck = false
		maxFetchers = 6

		err = corruptEtcdDir()
		Expect(err).ShouldNot(HaveOccurred())

		store, err = snapstore.GetSnapstore(&snapstore.Config{Container: snapstoreDir, Provider: "Local"})
		Expect(err).ShouldNot(HaveOccurred())

		baseSnapshot, deltaSnapList, err = miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
		Expect(err).ShouldNot(HaveOccurred())

		rstr = NewRestorer(store, logger)
	})

	Context("with zero fetchers", func() {
		fmt.Println("Testing for max-fetchers=0")
		It("should return error", func() {
			maxFetchers = 0

			restoreOptions := RestoreOptions{
				ClusterURLs:    clusterUrlsMap,
				ClusterToken:   restoreClusterToken,
				RestoreDataDir: restoreDataDir,
				PeerURLs:       peerUrls,
				SkipHashCheck:  skipHashCheck,
				Name:           restoreName,
				MaxFetchers:    maxFetchers,
				BaseSnapshot:   *baseSnapshot,
				DeltaSnapList:  deltaSnapList,
			}
			err = rstr.Restore(restoreOptions)
			Expect(err).Should(HaveOccurred())
		})
	})

	Context("with maximum of one fetcher allowed", func() {
		fmt.Println("Testing for max-fetchers=1")
		It("should restore etcd data directory", func() {
			maxFetchers = 1

			restoreOptions := RestoreOptions{
				ClusterURLs:    clusterUrlsMap,
				ClusterToken:   restoreClusterToken,
				RestoreDataDir: restoreDataDir,
				PeerURLs:       peerUrls,
				SkipHashCheck:  skipHashCheck,
				Name:           restoreName,
				MaxFetchers:    maxFetchers,
				BaseSnapshot:   *baseSnapshot,
				DeltaSnapList:  deltaSnapList,
			}
			err = rstr.Restore(restoreOptions)
			Expect(err).ShouldNot(HaveOccurred())

			err = checkDataConsistency(restoreDataDir, logger)
		})
	})

	Context("with maximum of four fetchers allowed", func() {
		fmt.Println("Testing for max-fetchers=4")
		It("should restore etcd data directory", func() {
			maxFetchers = 4

			restoreOptions := RestoreOptions{
				ClusterURLs:    clusterUrlsMap,
				ClusterToken:   restoreClusterToken,
				RestoreDataDir: restoreDataDir,
				PeerURLs:       peerUrls,
				SkipHashCheck:  skipHashCheck,
				Name:           restoreName,
				MaxFetchers:    maxFetchers,
				BaseSnapshot:   *baseSnapshot,
				DeltaSnapList:  deltaSnapList,
			}
			err = rstr.Restore(restoreOptions)
			Expect(err).ShouldNot(HaveOccurred())

			err = checkDataConsistency(restoreDataDir, logger)
			Expect(err).ShouldNot(HaveOccurred())
		})
	})

	Context("with maximum of hundred fetchers allowed", func() {
		fmt.Println("Testing for max-fetchers=100")
		It("should restore etcd data directory", func() {
			maxFetchers = 100

			restoreOptions := RestoreOptions{
				ClusterURLs:    clusterUrlsMap,
				ClusterToken:   restoreClusterToken,
				RestoreDataDir: restoreDataDir,
				PeerURLs:       peerUrls,
				SkipHashCheck:  skipHashCheck,
				Name:           restoreName,
				MaxFetchers:    maxFetchers,
				BaseSnapshot:   *baseSnapshot,
				DeltaSnapList:  deltaSnapList,
			}
			err = rstr.Restore(restoreOptions)
			Expect(err).ShouldNot(HaveOccurred())

			err = checkDataConsistency(restoreDataDir, logger)
			Expect(err).ShouldNot(HaveOccurred())
		})
	})
})

// checkDataConsistency starts an embedded etcd and checks for correctness of the values stored in etcd against the keys 'keyFrom' through 'keyTo'
func checkDataConsistency(dir string, logger *logrus.Logger) error {
	etcd, err := startEmbeddedEtcd(dir, logger)
	if err != nil {
		return fmt.Errorf("unable to start embedded etcd server: %v", err)
	}
	defer func() {
		etcd.Server.Stop()
		etcd.Close()
	}()

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

	opts := []clientv3.OpOption{
		clientv3.WithLimit(1),
	}

	for currKey := keyFrom; currKey <= keyTo; currKey++ {
		key = keyPrefix + strconv.Itoa(currKey)
		value = valuePrefix + strconv.Itoa(currKey)

		resp, err := cli.Get(context.TODO(), key, opts...)
		if err != nil {
			return fmt.Errorf("unable to get value from etcd: %v", err)
		}
		if len(resp.Kvs) == 0 {
			return fmt.Errorf("entry not found for key %s", resKey)
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
	fmt.Printf("Data consistency for key-value pairs (%[1]s%[3]d, %[2]s%[3]d) through (%[1]s%[4]d, %[2]s%[4]d) has been verified\n", keyPrefix, valuePrefix, keyFrom, keyTo)

	return nil
}

// corruptEtcdDir corrupts the etcd directory by deleting it
func corruptEtcdDir() error {
	if _, err := os.Stat(etcdDir); os.IsNotExist(err) {
		return nil
	}
	return os.RemoveAll(etcdDir)
}
