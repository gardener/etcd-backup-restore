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
	"io/ioutil"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	. "github.com/gardener/etcd-backup-restore/pkg/snapshot/snapshotter"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	"github.com/gardener/etcd-backup-restore/test/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Snapshotter", func() {
	var (
		endpoints                      []string
		store                          snapstore.SnapStore
		etcdConnectionTimeout          time.Duration
		garbageCollectionPeriodSeconds time.Duration
		maxBackups                     int
		schedule                       string
		certFile                       string
		keyFile                        string
		caFile                         string
		insecureTransport              bool
		insecureSkipVerify             bool
		etcdUsername                   string
		etcdPassword                   string
		err                            error
	)
	BeforeEach(func() {
		endpoints = []string{"http://localhost:2379"}
		etcdConnectionTimeout = 10
		garbageCollectionPeriodSeconds = 30
		schedule = "*/1 * * * *"
	})

	Describe("creating Snapshotter", func() {
		BeforeEach(func() {
			store, err = snapstore.GetSnapstore(&snapstore.Config{Container: path.Join(outputDir, "snapshotter_1.bkp")})
			Expect(err).ShouldNot(HaveOccurred())
		})
		Context("With invalid schedule", func() {
			It("should return error", func() {
				schedule = "65 * * * 5"
				tlsConfig := etcdutil.NewTLSConfig(
					certFile,
					keyFile,
					caFile,
					insecureTransport,
					insecureSkipVerify,
					endpoints,
					etcdUsername,
					etcdPassword)
				_, err := NewSnapshotterConfig(
					schedule,
					store,
					1,
					10,
					DefaultDeltaSnapMemoryLimit,
					etcdConnectionTimeout,
					garbageCollectionPeriodSeconds,
					GarbageCollectionPolicyExponential,
					tlsConfig)
				Expect(err).Should(HaveOccurred())
			})
		})

		Context("With valid schedule", func() {
			It("should create snapshotter config", func() {
				schedule = "*/5 * * * *"
				tlsConfig := etcdutil.NewTLSConfig(
					certFile,
					keyFile,
					caFile,
					insecureTransport,
					insecureSkipVerify,
					endpoints,
					etcdUsername,
					etcdPassword)
				_, err := NewSnapshotterConfig(
					schedule,
					store,
					1,
					10,
					DefaultDeltaSnapMemoryLimit,
					etcdConnectionTimeout,
					garbageCollectionPeriodSeconds,
					GarbageCollectionPolicyExponential,
					tlsConfig)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})
	})

	Describe("running snapshotter", func() {
		Context("with etcd not running at configured endpoint", func() {
			It("should timeout & not take any snapshot", func() {
				stopCh := make(chan struct{})
				endpoints = []string{"http://localhost:5000"}
				etcdConnectionTimeout = 5
				maxBackups = 2
				testTimeout := time.Duration(time.Minute * time.Duration(maxBackups+1))
				store, err = snapstore.GetSnapstore(&snapstore.Config{Container: path.Join(outputDir, "snapshotter_2.bkp")})
				Expect(err).ShouldNot(HaveOccurred())
				tlsConfig := etcdutil.NewTLSConfig(
					certFile,
					keyFile,
					caFile,
					insecureTransport,
					insecureSkipVerify,
					endpoints,
					etcdUsername,
					etcdPassword)
				snapshotterConfig, err := NewSnapshotterConfig(
					schedule,
					store,
					maxBackups,
					10,
					DefaultDeltaSnapMemoryLimit,
					etcdConnectionTimeout,
					garbageCollectionPeriodSeconds,
					GarbageCollectionPolicyExponential,
					tlsConfig)
				Expect(err).ShouldNot(HaveOccurred())

				ssr := NewSnapshotter(
					logger,
					snapshotterConfig)

				go func() {
					<-time.After(testTimeout)
					close(stopCh)
				}()
				err = ssr.Run(stopCh, true)
				Expect(err).Should(HaveOccurred())
				list, err := store.List()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(len(list)).Should(BeZero())
			})
		})

		Context("with etcd running at configured endpoint", func() {
			BeforeEach(func() {
				endpoints = []string{"http://localhost:2379"}
			})

			Context("with unreachable schedule", func() {
				var ssr *Snapshotter
				BeforeEach(func() {
					stopCh := make(chan struct{})
					schedule = "* * 31 2 *"
					etcdConnectionTimeout = 5
					maxBackups = 2
					testTimeout := time.Duration(time.Minute * time.Duration(maxBackups+1))
					store, err = snapstore.GetSnapstore(&snapstore.Config{Container: path.Join(outputDir, "snapshotter_3.bkp")})
					Expect(err).ShouldNot(HaveOccurred())
					tlsConfig := etcdutil.NewTLSConfig(
						certFile,
						keyFile,
						caFile,
						insecureTransport,
						insecureSkipVerify,
						endpoints,
						etcdUsername,
						etcdPassword)
					snapshotterConfig, err := NewSnapshotterConfig(
						schedule,
						store,
						maxBackups,
						10,
						DefaultDeltaSnapMemoryLimit,
						etcdConnectionTimeout,
						garbageCollectionPeriodSeconds,
						GarbageCollectionPolicyExponential,
						tlsConfig)
					Expect(err).ShouldNot(HaveOccurred())

					ssr = NewSnapshotter(
						logger,
						snapshotterConfig)
					go func() {
						<-time.After(testTimeout)
						close(stopCh)
					}()
					err = ssr.Run(stopCh, true)
					Expect(err).Should(HaveOccurred())
				})

				It("should not take any snapshot", func() {
					list, err := store.List()
					count := 0
					for _, snap := range list {
						if snap.Kind == snapstore.SnapshotKindFull {
							count++
						}
					}
					Expect(err).ShouldNot(HaveOccurred())
					Expect(count).Should(Equal(1))
				})
			})

			Context("with valid schedule", func() {
				var (
					ssr                          *Snapshotter
					schedule                     string
					maxBackups                   int
					testTimeout                  time.Duration
					deltaSnapshotIntervalSeconds int
				)
				BeforeEach(func() {
					endpoints = []string{"http://localhost:2379"}
					schedule = "*/1 * * * *"
					maxBackups = 2
					// We will wait for maxBackups+1 times schedule period
					testTimeout = time.Duration(time.Minute * time.Duration(maxBackups+1))
					etcdConnectionTimeout = 5
				})

				Context("with delta snapshot interval set to zero seconds", func() {
					BeforeEach(func() {
						deltaSnapshotIntervalSeconds = 0
						testTimeout = time.Duration(time.Minute * time.Duration(maxBackups))
					})
					It("should take periodic backups without delta snapshots", func() {
						stopCh := make(chan struct{})
						store, err = snapstore.GetSnapstore(&snapstore.Config{Container: path.Join(outputDir, "snapshotter_4.bkp")})
						Expect(err).ShouldNot(HaveOccurred())
						tlsConfig := etcdutil.NewTLSConfig(
							certFile,
							keyFile,
							caFile,
							insecureTransport,
							insecureSkipVerify,
							endpoints,
							etcdUsername,
							etcdPassword)
						snapshotterConfig, err := NewSnapshotterConfig(
							schedule,
							store,
							maxBackups,
							deltaSnapshotIntervalSeconds,
							DefaultDeltaSnapMemoryLimit,
							etcdConnectionTimeout,
							garbageCollectionPeriodSeconds,
							GarbageCollectionPolicyExponential,
							tlsConfig)
						Expect(err).ShouldNot(HaveOccurred())

						ssr = NewSnapshotter(
							logger,
							snapshotterConfig)

						go func() {
							<-time.After(testTimeout)
							close(stopCh)
						}()
						err = ssr.Run(stopCh, true)
						Expect(err).ShouldNot(HaveOccurred())
						list, err := store.List()
						Expect(err).ShouldNot(HaveOccurred())
						Expect(len(list)).ShouldNot(BeZero())
						for _, snapshot := range list {
							Expect(snapshot.Kind).ShouldNot(Equal(snapstore.SnapshotKindDelta))
						}
					})
				})

				Context("with delta snapshots enabled", func() {
					BeforeEach(func() {
						deltaSnapshotIntervalSeconds = 10
					})

					Context("with snapshotter starting without first full snapshot", func() {
						It("first snapshot should be a delta snapshot", func() {
							store, err = snapstore.GetSnapstore(&snapstore.Config{Container: path.Join(outputDir, "snapshotter_5.bkp")})
							Expect(err).ShouldNot(HaveOccurred())
							tlsConfig := etcdutil.NewTLSConfig(
								certFile,
								keyFile,
								caFile,
								insecureTransport,
								insecureSkipVerify,
								endpoints,
								etcdUsername,
								etcdPassword)
							snapshotterConfig, err := NewSnapshotterConfig(
								schedule,
								store,
								maxBackups,
								deltaSnapshotIntervalSeconds,
								DefaultDeltaSnapMemoryLimit,
								etcdConnectionTimeout,
								garbageCollectionPeriodSeconds,
								GarbageCollectionPolicyExponential,
								tlsConfig)
							Expect(err).ShouldNot(HaveOccurred())

							ssr = NewSnapshotter(
								logger,
								snapshotterConfig)
							populatorCtx, cancelPopulator := context.WithTimeout(testCtx, testTimeout)
							defer cancelPopulator()
							wg := &sync.WaitGroup{}
							wg.Add(1)
							// populating etcd so that snapshots will be taken
							go utils.PopulateEtcdWithWaitGroup(populatorCtx, wg, logger, endpoints, nil)
							ssrCtx := utils.ContextWithWaitGroup(testCtx, wg)
							err = ssr.Run(ssrCtx.Done(), false)
							Expect(err).ShouldNot(HaveOccurred())
							list, err := store.List()
							Expect(err).ShouldNot(HaveOccurred())
							Expect(len(list)).ShouldNot(BeZero())
							Expect(list[0].Kind).Should(Equal(snapstore.SnapshotKindDelta))
						})
					})

					Context("with snapshotter starting with full snapshot", func() {
						It("should take periodic backups", func() {
							store, err = snapstore.GetSnapstore(&snapstore.Config{Container: path.Join(outputDir, "snapshotter_6.bkp")})
							Expect(err).ShouldNot(HaveOccurred())
							tlsConfig := etcdutil.NewTLSConfig(
								certFile,
								keyFile,
								caFile,
								insecureTransport,
								insecureSkipVerify,
								endpoints,
								etcdUsername,
								etcdPassword)
							snapshotterConfig, err := NewSnapshotterConfig(
								schedule,
								store,
								maxBackups,
								deltaSnapshotIntervalSeconds,
								DefaultDeltaSnapMemoryLimit,
								etcdConnectionTimeout,
								garbageCollectionPeriodSeconds,
								GarbageCollectionPolicyExponential,
								tlsConfig)
							Expect(err).ShouldNot(HaveOccurred())

							populatorCtx, cancelPopulator := context.WithTimeout(testCtx, testTimeout)
							defer cancelPopulator()
							wg := &sync.WaitGroup{}
							wg.Add(1)
							// populating etcd so that snapshots will be taken
							go utils.PopulateEtcdWithWaitGroup(populatorCtx, wg, logger, endpoints, nil)

							ssr = NewSnapshotter(
								logger,
								snapshotterConfig)
							ssrCtx := utils.ContextWithWaitGroup(testCtx, wg)
							err = ssr.Run(ssrCtx.Done(), true)

							Expect(err).ShouldNot(HaveOccurred())
							list, err := store.List()
							Expect(err).ShouldNot(HaveOccurred())
							Expect(len(list)).ShouldNot(BeZero())
							Expect(list[0].Kind).Should(Equal(snapstore.SnapshotKindFull))
						})
					})
				})
			})
		})

		Context("##GarbageCollector", func() {
			var (
				testTimeout time.Duration
			)
			BeforeEach(func() {
				endpoints = []string{"http://localhost:2379"}
				schedule = "*/1 * * * *"
				maxBackups = 2
				garbageCollectionPeriodSeconds = 5
				testTimeout = time.Duration(time.Second * time.Duration(garbageCollectionPeriodSeconds*2))
				etcdConnectionTimeout = 5
			})

			It("should garbage collect exponentially", func() {
				logger.Infoln("creating expected output")

				// Prepare expected resultant snapshot list
				var (
					now              = time.Now().UTC()
					store            = prepareStoreForGarbageCollection(now, "garbagecollector_exponential.bkp")
					snapTime         = time.Date(now.Year(), now.Month(), now.Day()-35, 0, -30, 0, 0, now.Location())
					expectedSnapList = snapstore.SnapList{}
				)

				// weekly snapshot
				for i := 1; i <= 4; i++ {
					snapTime = snapTime.Add(time.Duration(time.Hour * 24 * 7))
					snap := &snapstore.Snapshot{
						Kind:          snapstore.SnapshotKindFull,
						CreatedOn:     snapTime,
						StartRevision: 0,
						LastRevision:  1001,
					}
					snap.GenerateSnapshotDirectory()
					snap.GenerateSnapshotName()
					expectedSnapList = append(expectedSnapList, snap)
				}
				fmt.Println("Weekly snapshot list prepared")

				// daily snapshot
				for i := 1; i <= 7; i++ {
					snapTime = snapTime.Add(time.Duration(time.Hour * 24))
					snap := &snapstore.Snapshot{
						Kind:          snapstore.SnapshotKindFull,
						CreatedOn:     snapTime,
						StartRevision: 0,
						LastRevision:  1001,
					}
					snap.GenerateSnapshotDirectory()
					snap.GenerateSnapshotName()
					expectedSnapList = append(expectedSnapList, snap)
				}
				fmt.Println("Daily snapshot list prepared")

				// hourly snapshot
				snapTime = snapTime.Add(time.Duration(time.Hour))
				for now.Truncate(time.Hour).Sub(snapTime) > 0 {
					snap := &snapstore.Snapshot{
						Kind:          snapstore.SnapshotKindFull,
						CreatedOn:     snapTime,
						StartRevision: 0,
						LastRevision:  1001,
					}
					snap.GenerateSnapshotDirectory()
					snap.GenerateSnapshotName()
					expectedSnapList = append(expectedSnapList, snap)
					snapTime = snapTime.Add(time.Duration(time.Hour))
				}
				fmt.Println("Hourly snapshot list prepared")

				// current hour
				snapTime = now.Truncate(time.Hour)
				snap := &snapstore.Snapshot{
					Kind:          snapstore.SnapshotKindFull,
					CreatedOn:     snapTime,
					StartRevision: 0,
					LastRevision:  1001,
				}
				snap.GenerateSnapshotDirectory()
				snap.GenerateSnapshotName()
				expectedSnapList = append(expectedSnapList, snap)
				snapTime = snapTime.Add(time.Duration(time.Minute * 30))
				for now.Sub(snapTime) >= 0 {
					snap := &snapstore.Snapshot{
						Kind:          snapstore.SnapshotKindFull,
						CreatedOn:     snapTime,
						StartRevision: 0,
						LastRevision:  1001,
					}
					snap.GenerateSnapshotDirectory()
					snap.GenerateSnapshotName()
					expectedSnapList = append(expectedSnapList, snap)
					snapTime = snapTime.Add(time.Duration(time.Minute * 30))
				}
				fmt.Println("Current hour full snapshot list prepared")

				// delta snapshots
				snapTime = snapTime.Add(time.Duration(-time.Minute * 30))
				snapTime = snapTime.Add(time.Duration(time.Minute * 10))
				for now.Sub(snapTime) >= 0 {
					snap := &snapstore.Snapshot{
						Kind:          snapstore.SnapshotKindDelta,
						CreatedOn:     snapTime,
						StartRevision: 0,
						LastRevision:  1001,
					}
					snap.GenerateSnapshotDirectory()
					snap.GenerateSnapshotName()
					expectedSnapList = append(expectedSnapList, snap)
					snapTime = snapTime.Add(time.Duration(time.Minute * 10))
				}
				fmt.Println("Incremental snapshot list prepared")

				//start test
				tlsConfig := etcdutil.NewTLSConfig(
					certFile,
					keyFile,
					caFile,
					insecureTransport,
					insecureSkipVerify,
					endpoints,
					etcdUsername,
					etcdPassword)
				snapshotterConfig, err := NewSnapshotterConfig(
					schedule,
					store,
					maxBackups,
					10,
					DefaultDeltaSnapMemoryLimit,
					etcdConnectionTimeout,
					garbageCollectionPeriodSeconds,
					GarbageCollectionPolicyExponential,
					tlsConfig)
				Expect(err).ShouldNot(HaveOccurred())
				ssr := NewSnapshotter(
					logger,
					snapshotterConfig)

				gcStopCh := make(chan struct{})

				go func() {
					<-time.After(testTimeout)
					close(gcStopCh)
				}()
				ssr.RunGarbageCollector(gcStopCh)

				list, err := store.List()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(len(list)).Should(Equal(len(expectedSnapList)))

				for index, snap := range list {
					if snap.CreatedOn != expectedSnapList[index].CreatedOn || snap.Kind != expectedSnapList[index].Kind {
						Fail("Expected snap list doesn't match with output snap list")
					}
				}
			})

			It("should garbage collect limitBased", func() {
				now := time.Now().UTC()
				store := prepareStoreForGarbageCollection(now, "garbagecollector_limit_based.bkp")
				tlsConfig := etcdutil.NewTLSConfig(
					certFile,
					keyFile,
					caFile,
					insecureTransport,
					insecureSkipVerify,
					endpoints,
					etcdUsername,
					etcdPassword)
				snapshotterConfig, err := NewSnapshotterConfig(
					schedule,
					store,
					maxBackups,
					10,
					DefaultDeltaSnapMemoryLimit,
					etcdConnectionTimeout,
					garbageCollectionPeriodSeconds,
					GarbageCollectionPolicyLimitBased,
					tlsConfig)
				Expect(err).ShouldNot(HaveOccurred())

				ssr := NewSnapshotter(
					logger,
					snapshotterConfig)

				gcCtx, cancel := context.WithTimeout(testCtx, testTimeout)
				defer cancel()
				ssr.RunGarbageCollector(gcCtx.Done())

				list, err := store.List()
				Expect(err).ShouldNot(HaveOccurred())

				incr := false
				fullSnapCount := 0
				for _, snap := range list {
					if incr == false {
						if snap.Kind == snapstore.SnapshotKindDelta {
							incr = true
						} else {
							fullSnapCount++
							Expect(fullSnapCount).Should(BeNumerically("<=", maxBackups))
						}
					} else {
						Expect(snap.Kind).Should(Equal(snapstore.SnapshotKindDelta))
					}
				}
			})
		})
	})
})

// prepareStoreForGarbageCollection populates the store with dummy snapshots for garbage collection tests
func prepareStoreForGarbageCollection(forTime time.Time, storeContainer string) snapstore.SnapStore {
	var (
		snapTime           = time.Date(forTime.Year(), forTime.Month(), forTime.Day()-36, 0, 0, 0, 0, forTime.Location())
		count              = 0
		noOfDeltaSnapshots = 3
	)
	fmt.Println("setting up garbage collection test")
	// Prepare snapshot directory
	store, err := snapstore.GetSnapstore(&snapstore.Config{Container: path.Join(outputDir, storeContainer)})
	Expect(err).ShouldNot(HaveOccurred())
	for forTime.Sub(snapTime) >= 0 {
		var kind = snapstore.SnapshotKindDelta
		if count == 0 {
			kind = snapstore.SnapshotKindFull
		}
		count = (count + 1) % noOfDeltaSnapshots
		snap := snapstore.Snapshot{
			Kind:          kind,
			CreatedOn:     snapTime,
			StartRevision: 0,
			LastRevision:  1001,
		}
		snap.GenerateSnapshotDirectory()
		snap.GenerateSnapshotName()
		snapTime = snapTime.Add(time.Duration(time.Minute * 10))
		store.Save(snap, ioutil.NopCloser(strings.NewReader(fmt.Sprintf("dummy-snapshot-content for snap created on %s", snap.CreatedOn))))
	}
	return store
}
