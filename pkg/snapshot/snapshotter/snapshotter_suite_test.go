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

package snapshotter_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
)

var (
	etcd  *embed.Etcd
	err   error
	keyTo int
)

const (
	outputDir   = "../../../test/output"
	etcdDir     = outputDir + "/default.etcd"
	keyPrefix   = "key-"
	valuePrefix = "val-"
	keyFrom     = 1
)

func TestSnapshotter(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Snapshotter Suite")
}

var _ = SynchronizedBeforeSuite(func() []byte {
	err = os.RemoveAll(outputDir)
	Expect(err).ShouldNot(HaveOccurred())

	etcd, err = startEmbeddedEtcd()
	Expect(err).ShouldNot(HaveOccurred())
	var data []byte
	return data
}, func(data []byte) {})

var _ = SynchronizedAfterSuite(func() {}, func() {
	etcd.Server.Stop()
	etcd.Close()
})

func startEmbeddedEtcd() (*embed.Etcd, error) {
	logger := logrus.New()
	logger.Infof("Starting embedded etcd")
	cfg := embed.NewConfig()
	cfg.Dir = etcdDir
	cfg.EnableV2 = false
	cfg.Debug = false
	cfg.GRPCKeepAliveTimeout = 0
	e, err := embed.StartEtcd(cfg)
	if err != nil {
		return nil, err
	}

	select {
	case <-e.Server.ReadyNotify():
		fmt.Printf("Embedded server is ready!\n")
	case <-time.After(60 * time.Second):
		e.Server.Stop() // trigger a shutdown
		e.Close()
		return nil, fmt.Errorf("server took too long to start")
	}
	return e, nil
}

// populateEtcd sequentially puts key-value pairs into the embedded etcd, until stopped
func populateEtcd(wg *sync.WaitGroup, logger *logrus.Logger, endpoints []string, errCh chan<- error, stopCh <-chan bool) {
	defer wg.Done()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 10 * time.Second,
	})
	if err != nil {
		errCh <- fmt.Errorf("unable to start etcd client: %v", err)
		return
	}
	defer cli.Close()

	var (
		key     string
		value   string
		currKey = 0
	)

	for {
		select {
		case _, more := <-stopCh:
			if !more {
				keyTo = currKey
				logger.Infof("Populated data till key %s into embedded etcd", keyPrefix+strconv.Itoa(currKey))
				return
			}
		default:
			currKey++
			key = keyPrefix + strconv.Itoa(currKey)
			value = valuePrefix + strconv.Itoa(currKey)
			_, err = cli.Put(context.TODO(), key, value)
			if err != nil {
				errCh <- fmt.Errorf("unable to put key-value pair (%s, %s) into embedded etcd: %v", key, value, err)
				return
			}
			time.Sleep(time.Second * 1)
		}
	}
}
