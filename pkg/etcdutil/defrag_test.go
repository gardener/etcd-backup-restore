package etcdutil

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Defrag", func() {
	var (
		tlsConfig             *TLSConfig
		endpoints             = []string{"http://localhost:2379"}
		etcdConnectionTimeout = time.Duration(30 * time.Second)
		keyPrefix             = "/defrag/key-"
		valuePrefix           = "val"
	)
	tlsConfig = NewTLSConfig("", "", "", true, true, endpoints)
	Context("Defragmentation", func() {
		BeforeEach(func() {
			client, err := GetTLSClientForEtcd(tlsConfig)
			defer client.Close()
			Expect(err).ShouldNot(HaveOccurred())
			for index := 0; index <= 1000; index++ {
				ctx, cancel := context.WithTimeout(context.TODO(), etcdConnectionTimeout)
				client.Put(ctx, fmt.Sprintf("%s%d", keyPrefix, index), valuePrefix)
				cancel()
			}
			for index := 0; index <= 500; index++ {
				ctx, cancel := context.WithTimeout(context.TODO(), etcdConnectionTimeout)
				client.Delete(ctx, fmt.Sprintf("%s%d", keyPrefix, index))
				cancel()
			}
		})

		It("should defragment and reduce size of DB within time", func() {
			client, err := GetTLSClientForEtcd(tlsConfig)
			Expect(err).ShouldNot(HaveOccurred())
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.TODO(), etcdDialTimeout)
			oldStatus, err := client.Status(ctx, endpoints[0])
			cancel()
			Expect(err).ShouldNot(HaveOccurred())

			err = defragData(tlsConfig, etcdConnectionTimeout)
			Expect(err).ShouldNot(HaveOccurred())
			ctx, cancel = context.WithTimeout(context.TODO(), etcdDialTimeout)
			newStatus, err := client.Status(ctx, endpoints[0])
			cancel()
			Expect(err).ShouldNot(HaveOccurred())

			Expect(newStatus.DbSize).Should(BeNumerically("<", oldStatus.DbSize))
			Expect(newStatus.Header.GetRevision()).Should(BeNumerically("==", oldStatus.Header.GetRevision()))
		})

		It("should keep size of DB same in case of timeout", func() {
			etcdConnectionTimeout = time.Duration(time.Second)
			client, err := GetTLSClientForEtcd(tlsConfig)
			Expect(err).ShouldNot(HaveOccurred())
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.TODO(), etcdDialTimeout)
			oldStatus, err := client.Status(ctx, endpoints[0])
			cancel()
			Expect(err).ShouldNot(HaveOccurred())

			err = defragData(tlsConfig, time.Duration(time.Microsecond))
			Expect(err).Should(HaveOccurred())
			ctx, cancel = context.WithTimeout(context.TODO(), etcdDialTimeout)
			newStatus, err := client.Status(ctx, endpoints[0])
			cancel()
			Expect(err).ShouldNot(HaveOccurred())

			Expect(newStatus.Header.GetRevision()).Should(BeNumerically("==", oldStatus.Header.GetRevision()))
		})
	})
})
