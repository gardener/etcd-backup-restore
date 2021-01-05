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

package compactor_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/compressor"
	"github.com/gardener/etcd-backup-restore/test/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/embed"
)

var (
	testSuitDir, testEtcdDir, testSnapshotDir string
	testCtx                                   = context.Background()
	logger                                    = logrus.New().WithField("suite", "compactor")
	etcd                                      *embed.Etcd
	err                                       error
	keyTo                                     int
	endpoints                                 []string
)

func TestCompactor(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Compactor Suite")
}

var _ = SynchronizedBeforeSuite(func() []byte {
	var (
		data []byte
	)

	// Create a directory for compaction test cases
	testSuitDir, err = ioutil.TempDir("/tmp", "compactor-test")
	Expect(err).ShouldNot(HaveOccurred())

	// Directory for the main ETCD process
	testEtcdDir := fmt.Sprintf("%s/etcd/default.etcd", testSuitDir)
	// Directory for storing the backups
	testSnapshotDir := fmt.Sprintf("%s/etcd/snapshotter.bkp", testSuitDir)

	logger.Infof("ETCD Directory is: %s", testEtcdDir)
	logger.Infof("Snapshot Directory is: %s", testSnapshotDir)

	// Start the main ETCD process that will run untill all compaction test cases are run
	etcd, err = utils.StartEmbeddedEtcd(testCtx, testEtcdDir, logger)
	Expect(err).ShouldNot(HaveOccurred())
	endpoints = []string{etcd.Clients[0].Addr().String()}
	logger.Infof("endpoints: %s", endpoints)

	// Populates data into the ETCD
	populatorCtx, cancelPopulator := context.WithTimeout(testCtx, 15*time.Second)
	defer cancelPopulator()
	resp := &utils.EtcdDataPopulationResponse{}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go utils.PopulateEtcdWithWaitGroup(populatorCtx, wg, logger, endpoints, resp)

	// Take snapshots (Full + Delta) of the ETCD database
	deltaSnapshotPeriod := time.Second
	ctx := utils.ContextWithWaitGroupFollwedByGracePeriod(populatorCtx, wg, deltaSnapshotPeriod+2*time.Second)
	compressionConfig := compressor.NewCompressorConfig()
	compressionConfig.Enabled = true
	compressionConfig.CompressionPolicy = "gzip"
	err = utils.RunSnapshotter(logger, testSnapshotDir, deltaSnapshotPeriod, endpoints, ctx.Done(), true, compressionConfig)
	Expect(err).ShouldNot(HaveOccurred())

	// Wait unitil the populator finishes with populating ETCD
	wg.Wait()

	keyTo = resp.KeyTo
	return data

}, func(data []byte) {})

var _ = SynchronizedAfterSuite(func() {}, cleanUp)

func cleanUp() {
	logger.Info("Stop the Embedded etcd server.")
	etcd.Server.Stop()
	etcd.Close()

	logger.Infof("All tests are done for compactor suite. %s is being removed.", testSuitDir)
	err = os.RemoveAll(testSuitDir)
	Expect(err).ShouldNot(HaveOccurred())
}
