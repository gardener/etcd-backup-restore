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
		etcdUsername          string
		etcdPassword          string
	)
	tlsConfig = NewTLSConfig("", "", "", true, true, endpoints, etcdUsername, etcdPassword)
	Context("Defragmentation", func() {
		BeforeEach(func() {
			now := time.Now().Unix()
			client, err := GetTLSClientForEtcd(tlsConfig)
			defer client.Close()
			Expect(err).ShouldNot(HaveOccurred())
			for index := 0; index <= 1000; index++ {
				ctx, cancel := context.WithTimeout(context.TODO(), etcdConnectionTimeout)
				client.Put(ctx, fmt.Sprintf("%s%d%d", keyPrefix, now, index), valuePrefix)
				cancel()
			}
			for index := 0; index <= 500; index++ {
				ctx, cancel := context.WithTimeout(context.TODO(), etcdConnectionTimeout)
				client.Delete(ctx, fmt.Sprintf("%s%d%d", keyPrefix, now, index))
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
			oldDBSize := oldStatus.DbSize
			oldRevision := oldStatus.Header.GetRevision()

			err = defragData(tlsConfig, etcdConnectionTimeout)
			Expect(err).ShouldNot(HaveOccurred())
			ctx, cancel = context.WithTimeout(context.TODO(), etcdDialTimeout)
			newStatus, err := client.Status(ctx, endpoints[0])
			cancel()
			Expect(err).ShouldNot(HaveOccurred())

			Expect(newStatus.DbSize).Should(BeNumerically("<", oldDBSize))
			Expect(newStatus.Header.GetRevision()).Should(BeNumerically("==", oldRevision))
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
			oldRevision := oldStatus.Header.GetRevision()

			err = defragData(tlsConfig, time.Duration(time.Microsecond))
			Expect(err).Should(HaveOccurred())
			ctx, cancel = context.WithTimeout(context.TODO(), etcdDialTimeout)
			newStatus, err := client.Status(ctx, endpoints[0])
			cancel()
			Expect(err).ShouldNot(HaveOccurred())

			Expect(newStatus.Header.GetRevision()).Should(BeNumerically("==", oldRevision))
		})

		It("should defrag periodically with callback", func() {
			defragCount := 0
			expectedDefragCount := 2
			defragmentationPeriod := time.Duration(30) * time.Second
			client, err := GetTLSClientForEtcd(tlsConfig)
			Expect(err).ShouldNot(HaveOccurred())
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.TODO(), etcdDialTimeout)
			oldStatus, err := client.Status(ctx, endpoints[0])
			cancel()
			Expect(err).ShouldNot(HaveOccurred())
			oldDBSize := oldStatus.DbSize
			oldRevision := oldStatus.Header.GetRevision()

			stopCh := make(chan struct{})
			time.AfterFunc(time.Second*time.Duration(75), func() {
				close(stopCh)
			})

			DefragDataPeriodically(stopCh, tlsConfig, defragmentationPeriod, etcdConnectionTimeout, func() error {
				defragCount++
				return nil
			})

			ctx, cancel = context.WithTimeout(context.TODO(), etcdDialTimeout)
			newStatus, err := client.Status(ctx, endpoints[0])
			cancel()
			Expect(err).ShouldNot(HaveOccurred())

			Expect(defragCount).Should(BeNumerically("==", expectedDefragCount))
			Expect(newStatus.DbSize).Should(BeNumerically("<", oldDBSize))
			Expect(newStatus.Header.GetRevision()).Should(BeNumerically("==", oldRevision))
		})
	})
})
