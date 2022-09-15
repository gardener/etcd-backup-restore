package server

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
)

var _ = Describe("backrestoreserver tests", func() {
	var (
		memberPeerURL string
		brServer      *BackupRestoreServer
		err           error
	)

	BeforeEach(func() {
		brServer, err = NewBackupRestoreServer(logrus.New(), &BackupRestoreComponentConfig{})
		Expect(err).ToNot(HaveOccurred())
	})

	Describe("testing isPeerURLTLSEnabled", func() {
		Context("testing with non-TLS enabled peer url", func() {
			BeforeEach(func() {
				memberPeerURL = "http://etcd-main-peer.default.svc:2380"
			})
			It("test", func() {
				enabled := brServer.isPeerURLTLSEnabled(memberPeerURL)
				Expect(err).To(BeNil())
				Expect(enabled).To(BeFalse())
			})

		})

		Context("testing with TLS enabled peer url", func() {
			BeforeEach(func() {
				memberPeerURL = "https://etcd-main-peer.default.svc:2380"
			})
			It("test", func() {
				enabled := brServer.isPeerURLTLSEnabled(memberPeerURL)
				Expect(err).To(BeNil())
				Expect(enabled).To(BeTrue())
			})
		})
	})

})
