package membergarbagecollector_test

import (
	"io"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
)

var (
	logger = logrus.New().WithField("suite", "etcd-member-gc")
)

func TestMembergarbagecollector(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Membergarbagecollector Suite")
}

var _ = SynchronizedBeforeSuite(func() []byte {
	logger.Logger.Out = io.Discard
	return nil
}, func(data []byte) {})
