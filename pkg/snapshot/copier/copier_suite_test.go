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

package copier_test

import (
	"context"
	"os"
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
	etcdDir            = outputDir + "/default.etcd"
	snapstoreDir       = outputDir + "/snapshotter.bkp"
	targetSnapstoreDir = outputDir + "/snapshotter-target.bkp"
)

var (
	testCtx   = context.Background()
	logger    = logrus.New().WithField("suite", "copier")
	etcd      *embed.Etcd
	err       error
	keyTo     int
	endpoints []string
)

func TestRestorer(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Copier Suite")
}

var _ = SynchronizedBeforeSuite(func() []byte {
	var (
		data []byte
	)

	err = os.RemoveAll(outputDir)
	Expect(err).ShouldNot(HaveOccurred())

	etcd, err = utils.StartEmbeddedEtcd(testCtx, etcdDir, logger, utils.DefaultEtcdName, "")
	Expect(err).ShouldNot(HaveOccurred())
	endpoints = []string{etcd.Clients[0].Addr().String()}
	logger.Infof("endpoints: %s", endpoints)

	Expect(err).ShouldNot(HaveOccurred())
	defer func() {
		etcd.Server.Stop()
		etcd.Close()
	}()
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

	err = os.RemoveAll(targetSnapstoreDir)
	Expect(err).ShouldNot(HaveOccurred())
}
