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

package restorer_test

import (
	"context"
	"fmt"
	"math"
	"os"
	"path"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/snapshotter"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
)

const (
	outputDir                  = "../../../test/output"
	etcdDir                    = outputDir + "/default.etcd"
	snapstoreDir               = outputDir + "/snapshotter.bkp"
	etcdEndpoint               = "http://localhost:2379"
	snapshotterDurationSeconds = 20
	keyPrefix                  = "key-"
	valuePrefix                = "val-"
	keyFrom                    = 1
)

var (
	etcd      *embed.Etcd
	err       error
	keyTo     int
	endpoints = []string{etcdEndpoint}
)

func TestRestorer(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Restorer Suite")
}

var _ = SynchronizedBeforeSuite(func() []byte {
	var (
		data            []byte
		errCh           = make(chan error)
		populatorStopCh = make(chan bool)
		ssrStopCh       = make(chan struct{})
	)

	err = os.RemoveAll(outputDir)
	Expect(err).ShouldNot(HaveOccurred())

	logger := logrus.New()

	etcd, err = startEmbeddedEtcd(etcdDir, logger)
	Expect(err).ShouldNot(HaveOccurred())
	wg := &sync.WaitGroup{}
	deltaSnapshotPeriod := 1
	wg.Add(1)
	go populateEtcd(wg, logger, endpoints, errCh, populatorStopCh)
	go func() {
		<-time.After(time.Duration(snapshotterDurationSeconds * time.Second))
		close(populatorStopCh)
		wg.Wait()
		time.Sleep(time.Duration(deltaSnapshotPeriod+2) * time.Second)
		close(ssrStopCh)
	}()

	err = runSnapshotter(logger, deltaSnapshotPeriod, endpoints, ssrStopCh, true)
	Expect(err).ShouldNot(HaveOccurred())

	etcd.Server.Stop()
	etcd.Close()
	return data

}, func(data []byte) {})

var _ = SynchronizedAfterSuite(func() {}, cleanUp)

func cleanUp() {
	err = os.RemoveAll(etcdDir)
	Expect(err).ShouldNot(HaveOccurred())

	err = os.RemoveAll(snapstoreDir)
	Expect(err).ShouldNot(HaveOccurred())

	//for the negative scenario for invalid restoredir set to "" we need to cleanup the member folder in the working directory
	restoreDir := path.Clean("")
	err = os.RemoveAll(path.Join(restoreDir, "member"))
	Expect(err).ShouldNot(HaveOccurred())

}

// startEmbeddedEtcd starts an embedded etcd server
func startEmbeddedEtcd(dir string, logger *logrus.Logger) (*embed.Etcd, error) {
	logger.Infof("Starting embedded etcd")
	cfg := embed.NewConfig()
	cfg.Dir = dir
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

// runSnapshotter creates a snapshotter object and runs it for a duration specified by 'snapshotterDurationSeconds'
func runSnapshotter(logger *logrus.Logger, deltaSnapshotPeriod int, endpoints []string, stopCh chan struct{}, startWithFullSnapshot bool) error {
	var (
		store                          snapstore.SnapStore
		certFile                       string
		keyFile                        string
		caFile                         string
		insecureTransport              bool
		insecureSkipVerify             bool
		maxBackups                     = 1
		etcdConnectionTimeout          = time.Duration(10)
		garbageCollectionPeriodSeconds = time.Duration(60)
		schedule                       = "0 0 1 1 *"
		garbageCollectionPolicy        = snapshotter.GarbageCollectionPolicyLimitBased
		etcdUsername                   string
		etcdPassword                   string
	)

	store, err = snapstore.GetSnapstore(&snapstore.Config{Container: snapstoreDir, Provider: "Local"})
	if err != nil {
		return err
	}

	tlsConfig := etcdutil.NewTLSConfig(
		certFile,
		keyFile,
		caFile,
		insecureTransport,
		insecureSkipVerify,
		endpoints,
		etcdUsername,
		etcdPassword,
	)

	snapshotterConfig, err := snapshotter.NewSnapshotterConfig(
		schedule,
		store,
		maxBackups,
		deltaSnapshotPeriod,
		snapshotter.DefaultDeltaSnapMemoryLimit,
		etcdConnectionTimeout,
		garbageCollectionPeriodSeconds,
		garbageCollectionPolicy,
		tlsConfig,
	)
	if err != nil {
		return err
	}

	ssr := snapshotter.NewSnapshotter(
		logger,
		snapshotterConfig,
	)

	return ssr.Run(stopCh, startWithFullSnapshot)
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
			//call a delete for every 10th Key after putting it in the store to check deletes in consistency check
			// handles deleted keys as every 10th key is deleted during populate etcd call
			// this handling is also done in the checkDataConsistency() in restorer_test.go file
			// also it assumes that the deltaSnapshotDuration is more than 10 --
			// if you change the constant please change the factor accordingly to have coverage of delete scenarios.
			if math.Mod(float64(currKey), 10) == 0 {
				_, err = cli.Delete(context.TODO(), key)
				if err != nil {
					errCh <- fmt.Errorf("unable to delete key  (%s) from embedded etcd: %v", key, err)
					return
				}
			}

		}
	}
}
