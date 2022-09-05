package server

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backrestoreserver tests", func() {
	const podName = "etcd-test-pod-0"
	var (
		initialAdvertisePeerURLs string
	)

	Describe("testing isPeerURLTLSEnabled", func() {
		Context("testing with non-TLS enabled peer url", func() {
			BeforeEach(func() {
				initialAdvertisePeerURLs = "http@etcd-main-peer@default@2380"
			})
			It("test", func() {
				enabled, err := isPeerURLTLSEnabled(initialAdvertisePeerURLs, podName)
				Expect(err).To(BeNil())
				Expect(enabled).To(BeFalse())
			})

		})
		Context("testing with TLS enabled peer url", func() {
			BeforeEach(func() {
				initialAdvertisePeerURLs = "https@etcd-main-peer@default@2380"
			})
			It("test", func() {
				enabled, err := isPeerURLTLSEnabled(initialAdvertisePeerURLs, podName)
				Expect(err).To(BeNil())
				Expect(enabled).To(BeFalse())
			})
		})
	})

})
