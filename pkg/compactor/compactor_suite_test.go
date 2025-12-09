// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package compactor_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/compressor"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/gardener/etcd-backup-restore/test/utils"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/embed"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	embeddedEtcdPortNo = "9089"
)

var (
	testSuiteDir, testEtcdDir, testSnapshotDir string
	testCtx                                    = context.Background()
	logger                                     = logrus.New().WithField("suite", "compactor")
	etcd                                       *embed.Etcd
	err                                        error
	keyTo                                      int
	endpoints                                  []string
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
	testSuiteDir, err = os.MkdirTemp("/tmp", "compactor-test-")
	Expect(err).ShouldNot(HaveOccurred())

	// Directory for the main ETCD process
	testEtcdDir := fmt.Sprintf("%s/etcd/default.etcd", testSuiteDir)
	// Directory for storing the backups
	testSnapshotDir := fmt.Sprintf("%s/etcd/snapshotter.bkp", testSuiteDir)

	logger.Infof("ETCD Directory is: %s", testEtcdDir)
	logger.Infof("Snapshot Directory is: %s", testSnapshotDir)

	// Start the main ETCD process that will run untill all compaction test cases are run
	etcd, err = utils.StartEmbeddedEtcd(testCtx, testEtcdDir, logger, utils.DefaultEtcdName, embeddedEtcdPortNo)
	Expect(err).ShouldNot(HaveOccurred())
	endpoints = []string{etcd.Clients[0].Addr().String()}
	logger.Infof("endpoints: %s", endpoints)

	// Populates data into the ETCD
	populatorCtx, cancelPopulator := context.WithTimeout(testCtx, 15*time.Second)
	defer cancelPopulator()
	resp := &utils.EtcdDataPopulationResponse{}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go utils.PopulateEtcdWithWaitGroup(populatorCtx, wg, logger, endpoints, "", "", resp)

	// Take snapshots (Full + Delta) of the ETCD database
	deltaSnapshotPeriod := time.Second
	ctx := utils.ContextWithWaitGroupFollwedByGracePeriod(populatorCtx, wg, deltaSnapshotPeriod+2*time.Second)
	compressionConfig := compressor.NewCompressorConfig()
	compressionConfig.Enabled = true
	compressionConfig.CompressionPolicy = "gzip"
	snapstoreConfig := brtypes.SnapstoreConfig{Container: testSnapshotDir, Provider: "Local"}
	err = utils.RunSnapshotter(logger, snapstoreConfig, deltaSnapshotPeriod, endpoints, "", "", ctx.Done(), true, compressionConfig)
	Expect(err).ShouldNot(HaveOccurred())

	// Wait until the populator finishes with populating ETCD
	wg.Wait()

	keyTo = resp.KeyTo
	return data

}, func(_ []byte) {})

var _ = SynchronizedAfterSuite(func() {}, cleanUp)

func cleanUp() {
	logger.Info("Stop the Embedded etcd server.")
	etcd.Server.Stop()
	etcd.Close()

	logger.Infof("All tests are done for compactor suite. %s is being removed.", testSuiteDir)
	err = os.RemoveAll(testSuiteDir)
	Expect(err).ShouldNot(HaveOccurred())
}
