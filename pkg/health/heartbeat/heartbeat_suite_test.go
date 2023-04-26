package heartbeat_test

import (
	"context"
	"os"
	"testing"

	"github.com/gardener/etcd-backup-restore/test/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/embed"
)

var (
	logger  = logrus.New().WithField("suite", "heartbeat")
	etcd    *embed.Etcd
	err     error
	testCtx = context.Background()
)

const (
	outputDir = "../../../test/output"
	etcdDir   = outputDir + "/default.etcd"
)

func TestHeartbeat(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Heartbeat Suite")
}

var _ = SynchronizedBeforeSuite(func() []byte {
	err = os.RemoveAll(outputDir)
	Expect(err).ShouldNot(HaveOccurred())

	etcd, err = utils.StartEmbeddedEtcd(testCtx, etcdDir, logger, utils.DefaultEtcdName, "")
	Expect(err).ShouldNot(HaveOccurred())
	var data []byte
	return data
}, func(data []byte) {})

var _ = SynchronizedAfterSuite(func() {}, func() {
	etcd.Server.Stop()
	etcd.Close()
})
