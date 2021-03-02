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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/compressor"
	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	. "github.com/gardener/etcd-backup-restore/pkg/snapshot/snapshotter"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/gardener/etcd-backup-restore/pkg/wrappers"
	"github.com/gardener/etcd-backup-restore/test/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Snapshotter", func() {
	var (
		store                   brtypes.SnapStore
		garbageCollectionPeriod time.Duration
		maxBackups              uint
		schedule                string
		etcdConnectionConfig    *etcdutil.EtcdConnectionConfig
		compressionConfig       *compressor.CompressionConfig
		err                     error
	)
	BeforeEach(func() {
		etcdConnectionConfig = etcdutil.NewEtcdConnectionConfig()
		compressionConfig = compressor.NewCompressorConfig()
		etcdConnectionConfig.Endpoints = []string{etcd.Clients[0].Addr().String()}
		etcdConnectionConfig.ConnectionTimeout.Duration = 5 * time.Second
		garbageCollectionPeriod = 30 * time.Second
		schedule = "*/1 * * * *"
	})

	Describe("creating Snapshotter", func() {
		BeforeEach(func() {
			store, err = snapstore.GetSnapstore(&brtypes.SnapstoreConfig{Container: path.Join(outputDir, "snapshotter_1.bkp")})
			Expect(err).ShouldNot(HaveOccurred())
		})
		Context("With invalid schedule", func() {
			It("should return error", func() {
				schedule = "65 * * * 5"
				snapshotterConfig := &brtypes.SnapshotterConfig{
					FullSnapshotSchedule:     schedule,
					DeltaSnapshotPeriod:      wrappers.Duration{Duration: 10},
					DeltaSnapshotMemoryLimit: brtypes.DefaultDeltaSnapMemoryLimit,
					GarbageCollectionPeriod:  wrappers.Duration{Duration: garbageCollectionPeriod},
					GarbageCollectionPolicy:  brtypes.GarbageCollectionPolicyExponential,
					MaxBackups:               1,
				}

				_, err := NewSnapshotter(logger, snapshotterConfig, store, etcdConnectionConfig, compressionConfig)
				Expect(err).Should(HaveOccurred())
			})
		})

		Context("With valid schedule", func() {
			It("should create snapshotter config", func() {
				schedule = "*/5 * * * *"
				snapshotterConfig := &brtypes.SnapshotterConfig{
					FullSnapshotSchedule:     schedule,
					DeltaSnapshotPeriod:      wrappers.Duration{Duration: 10},
					DeltaSnapshotMemoryLimit: brtypes.DefaultDeltaSnapMemoryLimit,
					GarbageCollectionPeriod:  wrappers.Duration{Duration: garbageCollectionPeriod},
					GarbageCollectionPolicy:  brtypes.GarbageCollectionPolicyExponential,
					MaxBackups:               1,
				}

				_, err := NewSnapshotter(logger, snapshotterConfig, store, etcdConnectionConfig, compressionConfig)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})
	})

	Describe("running snapshotter", func() {
		Context("with etcd not running at configured endpoint", func() {
			BeforeEach(func() {
				validEndpoint := etcd.Clients[0].Addr().String()
				tokens := strings.Split(validEndpoint, ":")
				Expect(len(tokens)).Should(BeNumerically(">=", 2))
				i, err := strconv.Atoi(tokens[len(tokens)-1])
				Expect(err).ShouldNot(HaveOccurred())
				invalidEndpoint := fmt.Sprintf("%s:%d", strings.Join(tokens[:len(tokens)-1], ":"), i+12)
				etcdConnectionConfig.Endpoints = []string{invalidEndpoint}
			})

			It("should timeout & not take any snapshot", func() {
				maxBackups = 2
				testTimeout := time.Duration(time.Minute * time.Duration(maxBackups+1))
				store, err = snapstore.GetSnapstore(&brtypes.SnapstoreConfig{Container: path.Join(outputDir, "snapshotter_2.bkp")})
				Expect(err).ShouldNot(HaveOccurred())
				snapshotterConfig := &brtypes.SnapshotterConfig{
					FullSnapshotSchedule:     schedule,
					DeltaSnapshotPeriod:      wrappers.Duration{Duration: 10},
					DeltaSnapshotMemoryLimit: brtypes.DefaultDeltaSnapMemoryLimit,
					GarbageCollectionPeriod:  wrappers.Duration{Duration: garbageCollectionPeriod},
					GarbageCollectionPolicy:  brtypes.GarbageCollectionPolicyExponential,
					MaxBackups:               maxBackups,
				}

				ssr, err := NewSnapshotter(logger, snapshotterConfig, store, etcdConnectionConfig, compressionConfig)
				Expect(err).ShouldNot(HaveOccurred())

				ctx, cancel := context.WithTimeout(testCtx, testTimeout)
				defer cancel()
				err = ssr.Run(ctx.Done(), true)
				Expect(err).Should(HaveOccurred())
				list, err := store.List()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(len(list)).Should(BeZero())
			})
		})

		Context("with etcd running at configured endpoint", func() {
			BeforeEach(func() {
				etcdConnectionConfig.Endpoints = []string{etcd.Clients[0].Addr().String()}
			})

			Context("with unreachable schedule", func() {
				var ssr *Snapshotter
				BeforeEach(func() {
					schedule = "* * 31 2 *"
					maxBackups = 2
					testTimeout := time.Duration(time.Minute * time.Duration(maxBackups+1))
					store, err = snapstore.GetSnapstore(&brtypes.SnapstoreConfig{Container: path.Join(outputDir, "snapshotter_3.bkp")})
					Expect(err).ShouldNot(HaveOccurred())
					snapshotterConfig := &brtypes.SnapshotterConfig{
						FullSnapshotSchedule:     schedule,
						DeltaSnapshotPeriod:      wrappers.Duration{Duration: 10 * time.Second},
						DeltaSnapshotMemoryLimit: brtypes.DefaultDeltaSnapMemoryLimit,
						GarbageCollectionPeriod:  wrappers.Duration{Duration: garbageCollectionPeriod},
						GarbageCollectionPolicy:  brtypes.GarbageCollectionPolicyExponential,
						MaxBackups:               maxBackups,
					}

					ssr, err = NewSnapshotter(logger, snapshotterConfig, store, etcdConnectionConfig, compressionConfig)
					Expect(err).ShouldNot(HaveOccurred())
					ctx, cancel := context.WithTimeout(testCtx, testTimeout)
					defer cancel()
					err = ssr.Run(ctx.Done(), true)
					Expect(err).Should(HaveOccurred())
				})

				It("should not take any snapshot", func() {
					list, err := store.List()
					count := 0
					for _, snap := range list {
						if snap.Kind == brtypes.SnapshotKindFull {
							count++
						}
					}
					Expect(err).ShouldNot(HaveOccurred())
					Expect(count).Should(Equal(1))
				})
			})

			Context("with valid schedule", func() {
				var (
					ssr                   *Snapshotter
					schedule              string
					maxBackups            uint
					testTimeout           time.Duration
					deltaSnapshotInterval time.Duration
				)
				BeforeEach(func() {
					schedule = "*/1 * * * *"
					maxBackups = 2
					// We will wait for maxBackups+1 times schedule period
					testTimeout = time.Duration(time.Minute * time.Duration(maxBackups+1))
				})

				Context("with delta snapshot interval set to zero seconds", func() {
					BeforeEach(func() {
						deltaSnapshotInterval = 0
						testTimeout = time.Duration(time.Minute * time.Duration(maxBackups))
					})

					It("should take periodic backups without delta snapshots", func() {
						store, err = snapstore.GetSnapstore(&brtypes.SnapstoreConfig{Container: path.Join(outputDir, "snapshotter_4.bkp")})
						Expect(err).ShouldNot(HaveOccurred())
						snapshotterConfig := &brtypes.SnapshotterConfig{
							FullSnapshotSchedule:     schedule,
							DeltaSnapshotPeriod:      wrappers.Duration{Duration: deltaSnapshotInterval},
							DeltaSnapshotMemoryLimit: brtypes.DefaultDeltaSnapMemoryLimit,
							GarbageCollectionPeriod:  wrappers.Duration{Duration: garbageCollectionPeriod},
							GarbageCollectionPolicy:  brtypes.GarbageCollectionPolicyExponential,
							MaxBackups:               maxBackups,
						}

						ssr, err = NewSnapshotter(logger, snapshotterConfig, store, etcdConnectionConfig, compressionConfig)
						Expect(err).ShouldNot(HaveOccurred())

						ctx, cancel := context.WithTimeout(testCtx, testTimeout)
						defer cancel()
						err = ssr.Run(ctx.Done(), true)
						Expect(err).ShouldNot(HaveOccurred())
						list, err := store.List()
						Expect(err).ShouldNot(HaveOccurred())
						Expect(len(list)).ShouldNot(BeZero())
						for _, snapshot := range list {
							Expect(snapshot.Kind).ShouldNot(Equal(brtypes.SnapshotKindDelta))
						}
					})

					It("should fail on triggering out-of-schedule delta snapshot", func() {
						store, err = snapstore.GetSnapstore(&brtypes.SnapstoreConfig{Container: path.Join(outputDir, "snapshotter_4.bkp")})
						Expect(err).ShouldNot(HaveOccurred())
						snapshotterConfig := &brtypes.SnapshotterConfig{
							FullSnapshotSchedule:     schedule,
							DeltaSnapshotPeriod:      wrappers.Duration{Duration: deltaSnapshotInterval},
							DeltaSnapshotMemoryLimit: brtypes.DefaultDeltaSnapMemoryLimit,
							GarbageCollectionPeriod:  wrappers.Duration{Duration: garbageCollectionPeriod},
							GarbageCollectionPolicy:  brtypes.GarbageCollectionPolicyExponential,
							MaxBackups:               maxBackups,
						}

						ssr, err = NewSnapshotter(logger, snapshotterConfig, store, etcdConnectionConfig, compressionConfig)
						Expect(err).ShouldNot(HaveOccurred())

						_, err = ssr.TriggerDeltaSnapshot()
						Expect(err).Should(HaveOccurred())
					})
				})

				Context("with delta snapshots enabled", func() {
					BeforeEach(func() {
						deltaSnapshotInterval = 10 * time.Second
						testTimeout = time.Duration(time.Minute * time.Duration(maxBackups+1))
					})

					Context("with snapshotter starting without first full snapshot", func() {
						It("first snapshot should be a delta snapshot", func() {
							currentHour := time.Now().Hour()
							store, err = snapstore.GetSnapstore(&brtypes.SnapstoreConfig{Container: path.Join(outputDir, "snapshotter_5.bkp")})
							Expect(err).ShouldNot(HaveOccurred())
							snapshotterConfig := &brtypes.SnapshotterConfig{
								FullSnapshotSchedule:     fmt.Sprintf("59 %d * * *", (currentHour+1)%24), // This make sure that full snapshot timer doesn't trigger full snapshot.
								DeltaSnapshotPeriod:      wrappers.Duration{Duration: deltaSnapshotInterval},
								DeltaSnapshotMemoryLimit: brtypes.DefaultDeltaSnapMemoryLimit,
								GarbageCollectionPeriod:  wrappers.Duration{Duration: garbageCollectionPeriod},
								GarbageCollectionPolicy:  brtypes.GarbageCollectionPolicyExponential,
								MaxBackups:               maxBackups,
							}

							ssr, err = NewSnapshotter(logger, snapshotterConfig, store, etcdConnectionConfig, compressionConfig)
							Expect(err).ShouldNot(HaveOccurred())
							populatorCtx, cancelPopulator := context.WithTimeout(testCtx, testTimeout)
							defer cancelPopulator()
							wg := &sync.WaitGroup{}
							wg.Add(1)
							// populating etcd so that snapshots will be taken
							go utils.PopulateEtcdWithWaitGroup(populatorCtx, wg, logger, etcdConnectionConfig.Endpoints, nil)
							ssrCtx := utils.ContextWithWaitGroup(testCtx, wg)
							err = ssr.Run(ssrCtx.Done(), false)
							Expect(err).ShouldNot(HaveOccurred())
							list, err := store.List()
							Expect(err).ShouldNot(HaveOccurred())
							Expect(len(list)).ShouldNot(BeZero())
							Expect(list[0].Kind).Should(Equal(brtypes.SnapshotKindDelta))
						})
					})

					Context("with snapshotter starting with full snapshot", func() {
						It("should take periodic backups", func() {
							store, err = snapstore.GetSnapstore(&brtypes.SnapstoreConfig{Container: path.Join(outputDir, "snapshotter_6.bkp")})
							Expect(err).ShouldNot(HaveOccurred())
							snapshotterConfig := &brtypes.SnapshotterConfig{
								FullSnapshotSchedule:     schedule,
								DeltaSnapshotPeriod:      wrappers.Duration{Duration: deltaSnapshotInterval},
								DeltaSnapshotMemoryLimit: brtypes.DefaultDeltaSnapMemoryLimit,
								GarbageCollectionPeriod:  wrappers.Duration{Duration: garbageCollectionPeriod},
								GarbageCollectionPolicy:  brtypes.GarbageCollectionPolicyExponential,
								MaxBackups:               maxBackups,
							}

							populatorCtx, cancelPopulator := context.WithTimeout(testCtx, testTimeout)
							defer cancelPopulator()
							wg := &sync.WaitGroup{}
							wg.Add(1)
							// populating etcd so that snapshots will be taken
							go utils.PopulateEtcdWithWaitGroup(populatorCtx, wg, logger, etcdConnectionConfig.Endpoints, nil)

							ssr, err = NewSnapshotter(logger, snapshotterConfig, store, etcdConnectionConfig, compressionConfig)
							Expect(err).ShouldNot(HaveOccurred())
							ssrCtx := utils.ContextWithWaitGroup(testCtx, wg)
							err = ssr.Run(ssrCtx.Done(), true)

							Expect(err).ShouldNot(HaveOccurred())
							list, err := store.List()
							Expect(err).ShouldNot(HaveOccurred())
							Expect(len(list)).ShouldNot(BeZero())
							Expect(list[0].Kind).Should(Equal(brtypes.SnapshotKindFull))
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
				etcdConnectionConfig.Endpoints = []string{etcd.Clients[0].Addr().String()}
				schedule = "*/1 * * * *"
				maxBackups = 2
				garbageCollectionPeriod = 5 * time.Second
				testTimeout = garbageCollectionPeriod * 2
			})

			It("should garbage collect exponentially", func() {
				logger.Infoln("creating expected output")

				// Prepare expected resultant snapshot list
				var (
					now              = time.Now().UTC()
					store            = prepareStoreForGarbageCollection(now, "garbagecollector_exponential.bkp")
					snapTime         = time.Date(now.Year(), now.Month(), now.Day()-35, 0, -30, 0, 0, now.Location())
					expectedSnapList = brtypes.SnapList{}
				)

				// weekly snapshot
				for i := 1; i <= 4; i++ {
					snapTime = snapTime.Add(time.Duration(time.Hour * 24 * 7))
					snap := &brtypes.Snapshot{
						Kind:          brtypes.SnapshotKindFull,
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
					snap := &brtypes.Snapshot{
						Kind:          brtypes.SnapshotKindFull,
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
					snap := &brtypes.Snapshot{
						Kind:          brtypes.SnapshotKindFull,
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
				snap := &brtypes.Snapshot{
					Kind:          brtypes.SnapshotKindFull,
					CreatedOn:     snapTime,
					StartRevision: 0,
					LastRevision:  1001,
				}
				snap.GenerateSnapshotDirectory()
				snap.GenerateSnapshotName()
				expectedSnapList = append(expectedSnapList, snap)
				snapTime = snapTime.Add(time.Duration(time.Minute * 30))
				for now.Sub(snapTime) >= 0 {
					snap := &brtypes.Snapshot{
						Kind:          brtypes.SnapshotKindFull,
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
					snap := &brtypes.Snapshot{
						Kind:          brtypes.SnapshotKindDelta,
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
				snapshotterConfig := &brtypes.SnapshotterConfig{
					FullSnapshotSchedule:     schedule,
					DeltaSnapshotPeriod:      wrappers.Duration{Duration: 10 * time.Second},
					DeltaSnapshotMemoryLimit: brtypes.DefaultDeltaSnapMemoryLimit,
					GarbageCollectionPeriod:  wrappers.Duration{Duration: garbageCollectionPeriod},
					GarbageCollectionPolicy:  brtypes.GarbageCollectionPolicyExponential,
					MaxBackups:               maxBackups,
				}

				ssr, err := NewSnapshotter(logger, snapshotterConfig, store, etcdConnectionConfig, compressionConfig)
				Expect(err).ShouldNot(HaveOccurred())

				gcCtx, cancel := context.WithTimeout(testCtx, testTimeout)
				defer cancel()
				ssr.RunGarbageCollector(gcCtx.Done())

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
				snapshotterConfig := &brtypes.SnapshotterConfig{
					FullSnapshotSchedule:     schedule,
					DeltaSnapshotPeriod:      wrappers.Duration{Duration: 10 * time.Second},
					DeltaSnapshotMemoryLimit: brtypes.DefaultDeltaSnapMemoryLimit,
					GarbageCollectionPeriod:  wrappers.Duration{Duration: garbageCollectionPeriod},
					GarbageCollectionPolicy:  brtypes.GarbageCollectionPolicyLimitBased,
					MaxBackups:               maxBackups,
				}

				ssr, err := NewSnapshotter(logger, snapshotterConfig, store, etcdConnectionConfig, compressionConfig)
				Expect(err).ShouldNot(HaveOccurred())

				gcCtx, cancel := context.WithTimeout(testCtx, testTimeout)
				defer cancel()
				ssr.RunGarbageCollector(gcCtx.Done())

				list, err := store.List()
				Expect(err).ShouldNot(HaveOccurred())

				incr := false
				fullSnapCount := 0
				for _, snap := range list {
					if incr == false {
						if snap.Kind == brtypes.SnapshotKindDelta {
							incr = true
						} else {
							fullSnapCount++
							Expect(fullSnapCount).Should(BeNumerically("<=", maxBackups))
						}
					} else {
						Expect(snap.Kind).Should(Equal(brtypes.SnapshotKindDelta))
					}
				}
			})
		})
	})
})

// prepareStoreForGarbageCollection populates the store with dummy snapshots for garbage collection tests
func prepareStoreForGarbageCollection(forTime time.Time, storeContainer string) brtypes.SnapStore {
	var (
		snapTime           = time.Date(forTime.Year(), forTime.Month(), forTime.Day()-36, 0, 0, 0, 0, forTime.Location())
		count              = 0
		noOfDeltaSnapshots = 3
	)
	fmt.Println("setting up garbage collection test")
	// Prepare snapshot directory
	store, err := snapstore.GetSnapstore(&brtypes.SnapstoreConfig{Container: path.Join(outputDir, storeContainer)})
	Expect(err).ShouldNot(HaveOccurred())
	for forTime.Sub(snapTime) >= 0 {
		var kind = brtypes.SnapshotKindDelta
		if count == 0 {
			kind = brtypes.SnapshotKindFull
		}
		count = (count + 1) % noOfDeltaSnapshots
		snap := brtypes.Snapshot{
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
