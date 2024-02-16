// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapshotter_test

import (
	"context"
	"os"
	"testing"

	"github.com/gardener/etcd-backup-restore/test/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/embed"
)

var (
	testCtx = context.Background()
	logger  = logrus.New().WithField("suite", "snapshotter")
	etcd    *embed.Etcd
	err     error
	keyTo   int
)

const (
	outputDir          = "../../../test/output"
	etcdDir            = outputDir + "/default.etcd"
	embeddedEtcdPortNo = ""
)

func TestSnapshotter(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Snapshotter Suite")
}

var _ = SynchronizedBeforeSuite(func() []byte {
	err = os.RemoveAll(outputDir)
	Expect(err).ShouldNot(HaveOccurred())

	etcd, err = utils.StartEmbeddedEtcd(testCtx, etcdDir, logger, utils.DefaultEtcdName, embeddedEtcdPortNo)
	Expect(err).ShouldNot(HaveOccurred())
	var data []byte
	return data
}, func(data []byte) {})

var _ = SynchronizedAfterSuite(func() {}, func() {
	etcd.Server.Stop()
	etcd.Close()
})
