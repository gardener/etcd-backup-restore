package membergarbagecollector_test

import (
	"os"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
)

var (
	logger = logrus.New().WithField("suite", "snapshotter")
	err    error
)

const (
	outputDir = "../../../test/output"
	etcdDir   = outputDir + "/default.etcd"
)

func TestMembergarbagecollector(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Membergarbagecollector Suite")
}

var _ = SynchronizedBeforeSuite(func() []byte {
	err = os.RemoveAll(outputDir)
	Expect(err).ShouldNot(HaveOccurred())

	Expect(err).ShouldNot(HaveOccurred())
	var data []byte
	return data
}, func(data []byte) {})

var _ = SynchronizedAfterSuite(func() {}, func() {
})
