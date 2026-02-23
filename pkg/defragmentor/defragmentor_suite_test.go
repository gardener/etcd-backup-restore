// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package defragmentor_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/gardener/etcd-backup-restore/test/utils"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/server/v3/embed"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	outputDir          = "../../test/output"
	etcdDir            = outputDir + "/default.etcd"
	etcdDialTimeout    = time.Second * 30
	embeddedEtcdPortNo = "9089"
)

var (
	testCtx   = context.Background()
	logger    = logrus.New().WithField("suite", "defragmentor")
	etcd      *embed.Etcd
	endpoints []string
	err       error
)

func TestDefragmentor(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Defragmentor Suite")
}

var _ = SynchronizedBeforeSuite(func() []byte {
	logger.Logger.Out = GinkgoWriter
	err = os.RemoveAll(outputDir)
	Expect(err).ShouldNot(HaveOccurred())

	etcd, err = utils.StartEmbeddedEtcd(testCtx, etcdDir, logger, utils.DefaultEtcdName, embeddedEtcdPortNo)
	Expect(err).ShouldNot(HaveOccurred())
	endpoints = []string{etcd.Clients[0].Addr().String()}
	logger.Infof("endpoints: %s", endpoints)
	var data []byte
	return data
}, func(_ []byte) {})

var _ = SynchronizedAfterSuite(func() {}, func() {
	etcd.Server.Stop()
	etcd.Close()
})
