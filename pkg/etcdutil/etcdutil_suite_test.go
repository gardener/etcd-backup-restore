// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package etcdutil_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/gardener/etcd-backup-restore/test/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/embed"
)

const (
	outputDir = "../../test/output"
	etcdDir   = outputDir + "/default.etcd"
	podName   = "etcd-test-0"
)

var (
	logger      = logrus.New().WithField("suite", "etcdutil")
	err         error
	testCtx     = context.Background()
	etcd        *embed.Etcd
	mockTimeout = time.Second * 5
)

func TestEtcdUtil(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "EtcdUtil")
}

var _ = SynchronizedBeforeSuite(func() []byte {
	err = os.RemoveAll(outputDir)
	Expect(err).ShouldNot(HaveOccurred())

	etcd, err = utils.StartEmbeddedEtcd(testCtx, etcdDir, logger, podName, "")
	Expect(err).ShouldNot(HaveOccurred())
	var data []byte
	etcd.Server.Stop()
	etcd.Close()
	return data
}, func(data []byte) {})

var _ = SynchronizedAfterSuite(func() {}, func() {
	err = os.RemoveAll(outputDir)
	Expect(err).ShouldNot(HaveOccurred())
})
