// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package restorer_test

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/compressor"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

	"github.com/gardener/etcd-backup-restore/test/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/embed"
)

const (
	outputDir          = "../../../test/output"
	embeddedEtcdPortNo = "9089"
)

var (
	etcdDir      = filepath.Join(outputDir, "default.etcd")
	tempDir      = filepath.Join(outputDir, "default.restore.tmp")
	snapstoreDir = filepath.Join(outputDir, "snapshotter.bkp")
	testCtx      = context.Background()
	logger       = logrus.New().WithField("suite", "restorer")
	etcd         *embed.Etcd
	err          error
	keyTo        int
	endpoints    []string
)

func TestRestorer(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Restorer Suite")
}

var _ = SynchronizedBeforeSuite(func() []byte {
	var (
		data []byte
	)

	err = os.RemoveAll(outputDir)
	Expect(err).ShouldNot(HaveOccurred())

	etcd, err = utils.StartEmbeddedEtcd(testCtx, etcdDir, logger, utils.DefaultEtcdName, embeddedEtcdPortNo)
	Expect(err).ShouldNot(HaveOccurred())
	defer func() {
		etcd.Server.Stop()
		etcd.Close()
	}()
	endpoints = []string{etcd.Clients[0].Addr().String()}
	logger.Infof("endpoints: %s", endpoints)
	populatorCtx, cancelPopulator := context.WithTimeout(testCtx, 15*time.Second)
	defer cancelPopulator()
	resp := &utils.EtcdDataPopulationResponse{}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go utils.PopulateEtcdWithWaitGroup(populatorCtx, wg, logger, endpoints, resp)

	deltaSnapshotPeriod := time.Second
	ctx := utils.ContextWithWaitGroupFollwedByGracePeriod(populatorCtx, wg, deltaSnapshotPeriod+2*time.Second)

	compressionConfig := compressor.NewCompressorConfig()
	snapstoreConfig := brtypes.SnapstoreConfig{Container: snapstoreDir, Provider: "Local"}
	err = utils.RunSnapshotter(logger, snapstoreConfig, deltaSnapshotPeriod, endpoints, ctx.Done(), true, compressionConfig)
	Expect(err).ShouldNot(HaveOccurred())

	keyTo = resp.KeyTo
	return data
}, func(data []byte) {})

var _ = SynchronizedAfterSuite(func() {}, cleanUp)

func cleanUp() {
	err = os.RemoveAll(etcdDir)
	Expect(err).ShouldNot(HaveOccurred())

	err = os.RemoveAll(snapstoreDir)
	Expect(err).ShouldNot(HaveOccurred())

	//for the negative scenario for invalid restoredir set to "" we need to cleanup the member folder in the working directory
	restoreDir := path.Clean("")
	err = os.RemoveAll(path.Join(restoreDir, "member"))
	Expect(err).ShouldNot(HaveOccurred())

}
