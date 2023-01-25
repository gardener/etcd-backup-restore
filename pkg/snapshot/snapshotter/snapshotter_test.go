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
	"io"
	"path"
	"strconv"
	"sync"

	"strings"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/compressor"
	. "github.com/gardener/etcd-backup-restore/pkg/snapshot/snapshotter"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/gardener/etcd-backup-restore/pkg/wrappers"

	"github.com/gardener/etcd-backup-restore/test/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	mixed     = "mixed"
	snapsInV1 = "v1"
	snapsInV2 = "v2"
)

var _ = Describe("Snapshotter", func() {
	var (
		store                   brtypes.SnapStore
		garbageCollectionPeriod time.Duration
		maxBackups              uint
		schedule                string
		etcdConnectionConfig    *brtypes.EtcdConnectionConfig
		compressionConfig       *compressor.CompressionConfig
		healthConfig            *brtypes.HealthConfig
		snapstoreConfig         *brtypes.SnapstoreConfig
		err                     error
	)
	BeforeEach(func() {
		etcdConnectionConfig = brtypes.NewEtcdConnectionConfig()
		compressionConfig = compressor.NewCompressorConfig()
		healthConfig = brtypes.NewHealthConfig()
		etcdConnectionConfig.Endpoints = []string{etcd.Clients[0].Addr().String()}
		etcdConnectionConfig.ConnectionTimeout.Duration = 5 * time.Second
		garbageCollectionPeriod = 30 * time.Second
		schedule = "*/1 * * * *"
	})

	Describe("creating Snapshotter", func() {
		BeforeEach(func() {
			snapstoreConfig = &brtypes.SnapstoreConfig{Container: path.Join(outputDir, "snapshotter_1.bkp")}
			store, err = snapstore.GetSnapstore(snapstoreConfig)
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

				_, err := NewSnapshotter(logger, snapshotterConfig, store, etcdConnectionConfig, compressionConfig, healthConfig, snapstoreConfig)
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

				_, err := NewSnapshotter(logger, snapshotterConfig, store, etcdConnectionConfig, compressionConfig, healthConfig, snapstoreConfig)
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
				snapstoreConfig = &brtypes.SnapstoreConfig{Container: path.Join(outputDir, "snapshotter_2.bkp")}
				store, err = snapstore.GetSnapstore(snapstoreConfig)
				Expect(err).ShouldNot(HaveOccurred())
				snapshotterConfig := &brtypes.SnapshotterConfig{
					FullSnapshotSchedule:     schedule,
					DeltaSnapshotPeriod:      wrappers.Duration{Duration: 10},
					DeltaSnapshotMemoryLimit: brtypes.DefaultDeltaSnapMemoryLimit,
					GarbageCollectionPeriod:  wrappers.Duration{Duration: garbageCollectionPeriod},
					GarbageCollectionPolicy:  brtypes.GarbageCollectionPolicyExponential,
					MaxBackups:               maxBackups,
				}

				ssr, err := NewSnapshotter(logger, snapshotterConfig, store, etcdConnectionConfig, compressionConfig, healthConfig, snapstoreConfig)
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
					snapstoreConfig = &brtypes.SnapstoreConfig{Container: path.Join(outputDir, "snapshotter_3.bkp")}
					store, err = snapstore.GetSnapstore(snapstoreConfig)
					Expect(err).ShouldNot(HaveOccurred())
					snapshotterConfig := &brtypes.SnapshotterConfig{
						FullSnapshotSchedule:     schedule,
						DeltaSnapshotPeriod:      wrappers.Duration{Duration: 10 * time.Second},
						DeltaSnapshotMemoryLimit: brtypes.DefaultDeltaSnapMemoryLimit,
						GarbageCollectionPeriod:  wrappers.Duration{Duration: garbageCollectionPeriod},
						GarbageCollectionPolicy:  brtypes.GarbageCollectionPolicyExponential,
						MaxBackups:               maxBackups,
					}

					ssr, err = NewSnapshotter(logger, snapshotterConfig, store, etcdConnectionConfig, compressionConfig, healthConfig, snapstoreConfig)
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
						snapstoreConfig := &brtypes.SnapstoreConfig{Container: path.Join(outputDir, "snapshotter_4.bkp")}
						store, err = snapstore.GetSnapstore(snapstoreConfig)
						Expect(err).ShouldNot(HaveOccurred())
						snapshotterConfig := &brtypes.SnapshotterConfig{
							FullSnapshotSchedule:     schedule,
							DeltaSnapshotPeriod:      wrappers.Duration{Duration: deltaSnapshotInterval},
							DeltaSnapshotMemoryLimit: brtypes.DefaultDeltaSnapMemoryLimit,
							GarbageCollectionPeriod:  wrappers.Duration{Duration: garbageCollectionPeriod},
							GarbageCollectionPolicy:  brtypes.GarbageCollectionPolicyExponential,
							MaxBackups:               maxBackups,
						}

						ssr, err = NewSnapshotter(logger, snapshotterConfig, store, etcdConnectionConfig, compressionConfig, healthConfig, snapstoreConfig)
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
						snapstoreConfig = &brtypes.SnapstoreConfig{Container: path.Join(outputDir, "snapshotter_4.bkp")}
						store, err = snapstore.GetSnapstore(snapstoreConfig)
						Expect(err).ShouldNot(HaveOccurred())
						snapshotterConfig := &brtypes.SnapshotterConfig{
							FullSnapshotSchedule:     schedule,
							DeltaSnapshotPeriod:      wrappers.Duration{Duration: deltaSnapshotInterval},
							DeltaSnapshotMemoryLimit: brtypes.DefaultDeltaSnapMemoryLimit,
							GarbageCollectionPeriod:  wrappers.Duration{Duration: garbageCollectionPeriod},
							GarbageCollectionPolicy:  brtypes.GarbageCollectionPolicyExponential,
							MaxBackups:               maxBackups,
						}

						ssr, err = NewSnapshotter(logger, snapshotterConfig, store, etcdConnectionConfig, compressionConfig, healthConfig, snapstoreConfig)
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
							snapstoreConfig = &brtypes.SnapstoreConfig{Container: path.Join(outputDir, "snapshotter_5.bkp")}
							store, err = snapstore.GetSnapstore(snapstoreConfig)
							Expect(err).ShouldNot(HaveOccurred())
							snapshotterConfig := &brtypes.SnapshotterConfig{
								FullSnapshotSchedule:     fmt.Sprintf("59 %d * * *", (currentHour+1)%24), // This make sure that full snapshot timer doesn't trigger full snapshot.
								DeltaSnapshotPeriod:      wrappers.Duration{Duration: deltaSnapshotInterval},
								DeltaSnapshotMemoryLimit: brtypes.DefaultDeltaSnapMemoryLimit,
								GarbageCollectionPeriod:  wrappers.Duration{Duration: garbageCollectionPeriod},
								GarbageCollectionPolicy:  brtypes.GarbageCollectionPolicyExponential,
								MaxBackups:               maxBackups,
							}

							ssr, err = NewSnapshotter(logger, snapshotterConfig, store, etcdConnectionConfig, compressionConfig, healthConfig, snapstoreConfig)
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
							snapstoreConfig = &brtypes.SnapstoreConfig{Container: path.Join(outputDir, "snapshotter_6.bkp")}
							store, err = snapstore.GetSnapstore(snapstoreConfig)
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

							ssr, err = NewSnapshotter(logger, snapshotterConfig, store, etcdConnectionConfig, compressionConfig, healthConfig, snapstoreConfig)
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
				now         time.Time
			)
			BeforeEach(func() {
				etcdConnectionConfig.Endpoints = []string{etcd.Clients[0].Addr().String()}
				schedule = "*/1 * * * *"
				maxBackups = 2
				garbageCollectionPeriod = 5 * time.Second
				testTimeout = garbageCollectionPeriod * 2
				now = time.Now().UTC()
			})

			It("should garbage collect exponentially", func() {
				logger.Infoln("creating expected output")

				// Prepare expected resultant snapshot list
				var (
					store, snapstoreConfig = prepareStoreForGarbageCollection(now, "garbagecollector_exponential.bkp", "v2")
					snapTime               = time.Date(now.Year(), now.Month(), now.Day()-35, 0, -30, 0, 0, now.Location())
					expectedSnapList       = brtypes.SnapList{}
				)

				expectedSnapList = prepareExpectedSnapshotsList(snapTime, now, expectedSnapList, snapsInV2)

				//start test
				snapshotterConfig := &brtypes.SnapshotterConfig{
					FullSnapshotSchedule:     schedule,
					DeltaSnapshotPeriod:      wrappers.Duration{Duration: 10 * time.Second},
					DeltaSnapshotMemoryLimit: brtypes.DefaultDeltaSnapMemoryLimit,
					GarbageCollectionPeriod:  wrappers.Duration{Duration: garbageCollectionPeriod},
					GarbageCollectionPolicy:  brtypes.GarbageCollectionPolicyExponential,
					MaxBackups:               maxBackups,
				}

				ssr, err := NewSnapshotter(logger, snapshotterConfig, store, etcdConnectionConfig, compressionConfig, healthConfig, snapstoreConfig)
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

			//Test to check backward compatibility of garbage collector
			//Checks garbage collector behaviour (in exponential config) when both v1 and v2 directories are present
			//TODO: Consider removing when backward compatibility no longer needed
			It("should garbage collect exponentially from both v1 and v2 dir structures (backward compatible)", func() {
				logger.Infoln("creating expected output")

				// Prepare expected resultant snapshot list
				var (
					store, snapstoreConfig = prepareStoreForBackwardCompatibleGC(now, "gc_exponential_backward_compatible.bkp")
					snapTime               = time.Date(now.Year(), now.Month(), now.Day()-35, 0, -30, 0, 0, now.Location())
					expectedSnapList       = brtypes.SnapList{}
				)

				expectedSnapList = prepareExpectedSnapshotsList(snapTime, now, expectedSnapList, mixed)

				//start test
				snapshotterConfig := &brtypes.SnapshotterConfig{
					FullSnapshotSchedule:     schedule,
					DeltaSnapshotPeriod:      wrappers.Duration{Duration: 10 * time.Second},
					DeltaSnapshotMemoryLimit: brtypes.DefaultDeltaSnapMemoryLimit,
					GarbageCollectionPeriod:  wrappers.Duration{Duration: garbageCollectionPeriod},
					GarbageCollectionPolicy:  brtypes.GarbageCollectionPolicyExponential,
					MaxBackups:               maxBackups,
				}

				ssr, err := NewSnapshotter(logger, snapshotterConfig, store, etcdConnectionConfig, compressionConfig, healthConfig, snapstoreConfig)
				Expect(err).ShouldNot(HaveOccurred())

				gcCtx, cancel := context.WithTimeout(testCtx, testTimeout)
				defer cancel()
				ssr.RunGarbageCollector(gcCtx.Done())

				list, err := store.List()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(len(list)).Should(Equal(len(expectedSnapList)))

				for index, snap := range list {
					fmt.Println("Snap day: ", snap.CreatedOn.Day())
					fmt.Println("Expected snap day: ", expectedSnapList[index].CreatedOn.Day())
					Expect(snap.CreatedOn).Should(Equal(expectedSnapList[index].CreatedOn))
					Expect(snap.Kind).Should(Equal(expectedSnapList[index].Kind))
					Expect(snap.SnapDir).Should(Equal(expectedSnapList[index].SnapDir))
				}
			})

			//Test to check backward compatibility of garbage collector
			//Tests garbage collector behaviour (in exponential config) when only v1 directory is present
			//TODO: Consider removing when backward compatibility no longer needed
			It("should garbage collect exponentially with only v1 dir structure present (backward compatible test)", func() {
				logger.Infoln("creating expected output")

				// Prepare expected resultant snapshot list
				var (
					store, snapstoreConfig = prepareStoreForGarbageCollection(now, "gc_exponential_backward_compatiblev1.bkp", "v1")
					snapTime               = time.Date(now.Year(), now.Month(), now.Day()-35, 0, -30, 0, 0, now.Location())
					expectedSnapList       = brtypes.SnapList{}
				)

				expectedSnapList = prepareExpectedSnapshotsList(snapTime, now, expectedSnapList, snapsInV1)

				//start test
				snapshotterConfig := &brtypes.SnapshotterConfig{
					FullSnapshotSchedule:     schedule,
					DeltaSnapshotPeriod:      wrappers.Duration{Duration: 10 * time.Second},
					DeltaSnapshotMemoryLimit: brtypes.DefaultDeltaSnapMemoryLimit,
					GarbageCollectionPeriod:  wrappers.Duration{Duration: garbageCollectionPeriod},
					GarbageCollectionPolicy:  brtypes.GarbageCollectionPolicyExponential,
					MaxBackups:               maxBackups,
				}

				ssr, err := NewSnapshotter(logger, snapshotterConfig, store, etcdConnectionConfig, compressionConfig, healthConfig, snapstoreConfig)
				Expect(err).ShouldNot(HaveOccurred())

				gcCtx, cancel := context.WithTimeout(testCtx, testTimeout)
				defer cancel()
				ssr.RunGarbageCollector(gcCtx.Done())

				list, err := store.List()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(len(list)).Should(Equal(len(expectedSnapList)))

				for index, snap := range list {
					Expect(snap.CreatedOn).Should(Equal(expectedSnapList[index].CreatedOn))
					Expect(snap.Kind).Should(Equal(expectedSnapList[index].Kind))
					Expect(snap.SnapDir).Should(Equal(expectedSnapList[index].SnapDir))
				}
			})

			It("should garbage collect limitBased", func() {
				now := time.Now().UTC()
				store, snapstoreConfig := prepareStoreForGarbageCollection(now, "garbagecollector_limit_based.bkp", "v2")
				snapshotterConfig := &brtypes.SnapshotterConfig{
					FullSnapshotSchedule:     schedule,
					DeltaSnapshotPeriod:      wrappers.Duration{Duration: 10 * time.Second},
					DeltaSnapshotMemoryLimit: brtypes.DefaultDeltaSnapMemoryLimit,
					GarbageCollectionPeriod:  wrappers.Duration{Duration: garbageCollectionPeriod},
					GarbageCollectionPolicy:  brtypes.GarbageCollectionPolicyLimitBased,
					MaxBackups:               maxBackups,
				}

				ssr, err := NewSnapshotter(logger, snapshotterConfig, store, etcdConnectionConfig, compressionConfig, healthConfig, snapstoreConfig)
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

			// Test to check backward compatibility of garbage collector
			// Tests garbage collector behaviour (in limit based config) when both v1 and v2 directories are present
			// TODO: Consider removing when backward compatibility no longer needed
			/*It("should garbage collect limitBased from both v1 and v2 dir structures (backward compatibility test)", func() {
				now := time.Now().UTC()
				store := prepareStoreForBackwardCompatibleGC(now, "gc_limit_based_backward_compatible.bkp")
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

				validateLimitBasedSnapshots(list, maxBackups, snapsInV2)
			})*/

			//Test to check backward compatibility of garbage collector
			//Tests garbage collector behaviour (in limit based config) when only v1 directory is present
			//TODO: Consider removing when backward compatibility no longer needed
			It("should garbage collect limitBased with only v1 dir structure present (backward compatible test)", func() {
				now := time.Now().UTC()
				store, snapstoreConfig := prepareStoreForGarbageCollection(now, "gc_limit_based_backward_compatiblev1.bkp", "v1")
				snapshotterConfig := &brtypes.SnapshotterConfig{
					FullSnapshotSchedule:     schedule,
					DeltaSnapshotPeriod:      wrappers.Duration{Duration: 10 * time.Second},
					DeltaSnapshotMemoryLimit: brtypes.DefaultDeltaSnapMemoryLimit,
					GarbageCollectionPeriod:  wrappers.Duration{Duration: garbageCollectionPeriod},
					GarbageCollectionPolicy:  brtypes.GarbageCollectionPolicyLimitBased,
					MaxBackups:               maxBackups,
				}

				ssr, err := NewSnapshotter(logger, snapshotterConfig, store, etcdConnectionConfig, compressionConfig, healthConfig, snapstoreConfig)
				Expect(err).ShouldNot(HaveOccurred())

				gcCtx, cancel := context.WithTimeout(testCtx, testTimeout)
				defer cancel()
				ssr.RunGarbageCollector(gcCtx.Done())

				list, err := store.List()
				Expect(err).ShouldNot(HaveOccurred())

				validateLimitBasedSnapshots(list, maxBackups, snapsInV1)
			})
		})
	})

	Describe("Scenarios to take full-snapshot during startup", func() {
		var (
			ssr                    *Snapshotter
			currentMin             int
			currentHour            int
			fullSnapshotTimeWindow float64
		)
		BeforeEach(func() {
			fullSnapshotTimeWindow = 24
			currentHour = time.Now().Hour()
			currentMin = time.Now().Minute()
			snapstoreConfig = &brtypes.SnapstoreConfig{Container: path.Join(outputDir, "default.bkp")}
			store, err = snapstore.GetSnapstore(snapstoreConfig)
			Expect(err).ShouldNot(HaveOccurred())
		})
		Context("Previous full snapshot was taken more than 24hrs before", func() {
			It("should return true", func() {
				snapshotterConfig := &brtypes.SnapshotterConfig{
					FullSnapshotSchedule: fmt.Sprintf("%d %d * * *", (currentMin+1)%60, (currentHour+2)%24),
				}

				ssr, err = NewSnapshotter(logger, snapshotterConfig, store, etcdConnectionConfig, compressionConfig, healthConfig, snapstoreConfig)
				Expect(err).ShouldNot(HaveOccurred())

				// Previous full snapshot was taken 2 days before
				ssr.PrevFullSnapshot = &brtypes.Snapshot{
					CreatedOn: time.Date(time.Now().Year(), time.Now().Month(), time.Now().Day()-2, currentHour, currentMin, 0, 0, time.Local),
				}
				isFullSnapMissed := ssr.IsTakingFullSnapRequiresAtStartup(fullSnapshotTimeWindow)
				Expect(isFullSnapMissed).Should(BeTrue())
			})
		})

		Context("Previous full snapshot was taken exactly at scheduled snapshot time, no FullSnapshot was missed", func() {
			It("should return false", func() {
				snapshotterConfig := &brtypes.SnapshotterConfig{
					FullSnapshotSchedule: fmt.Sprintf("%d %d * * *", (currentMin+1)%60, (currentHour+2)%24),
				}

				ssr, err = NewSnapshotter(logger, snapshotterConfig, store, etcdConnectionConfig, compressionConfig, healthConfig, snapstoreConfig)
				Expect(err).ShouldNot(HaveOccurred())

				// Previous full snapshot was taken 1 days before at exactly at scheduled time
				ssr.PrevFullSnapshot = &brtypes.Snapshot{
					CreatedOn: time.Date(time.Now().Year(), time.Now().Month(), time.Now().Day()-1, (currentHour+2)%24, (currentMin+1)%60, 0, 0, time.Local),
				}

				isFullSnapMissed := ssr.IsTakingFullSnapRequiresAtStartup(fullSnapshotTimeWindow)
				Expect(isFullSnapMissed).Should(BeFalse())
			})
		})

		Context("Previous snapshot was taken within 24hrs and next schedule full-snapshot will be taken within 24hs of time window", func() {
			It("should return false", func() {
				scheduleHour := (currentHour + 4) % 24
				snapshotterConfig := &brtypes.SnapshotterConfig{
					FullSnapshotSchedule: fmt.Sprintf("%d %d * * *", currentMin, scheduleHour),
				}

				ssr, err = NewSnapshotter(logger, snapshotterConfig, store, etcdConnectionConfig, compressionConfig, healthConfig, snapstoreConfig)
				Expect(err).ShouldNot(HaveOccurred())

				// Previous full snapshot was taken 4hrs 10 mins before startup of backup-restore
				ssr.PrevFullSnapshot = &brtypes.Snapshot{
					CreatedOn: time.Date(time.Now().Year(), time.Now().Month(), time.Now().Day(), (currentHour-4)%24, (currentMin-10)%60, 0, 0, time.Local),
				}
				isFullSnapCanBeMissed := ssr.IsTakingFullSnapRequiresAtStartup(fullSnapshotTimeWindow)
				Expect(isFullSnapCanBeMissed).Should(BeFalse())
			})
		})

		Context("Previous snapshot was taken within 24hrs and next schedule full-snapshot likely to cross 24hs of time window", func() {
			It("should return true", func() {
				scheduleHour := (currentHour + 8) % 24
				snapshotterConfig := &brtypes.SnapshotterConfig{
					FullSnapshotSchedule: fmt.Sprintf("%d %d * * *", currentMin, scheduleHour),
				}

				ssr, err = NewSnapshotter(logger, snapshotterConfig, store, etcdConnectionConfig, compressionConfig, healthConfig, snapstoreConfig)
				Expect(err).ShouldNot(HaveOccurred())

				// Previous full snapshot was taken 18hrs(<24hrs) before startup of backup-restore
				ssr.PrevFullSnapshot = &brtypes.Snapshot{
					CreatedOn: time.Date(time.Now().Year(), time.Now().Month(), time.Now().Day(), (currentHour-18)%24, (currentMin)%60, 0, 0, time.Local),
				}
				isFullSnapCanBeMissed := ssr.IsTakingFullSnapRequiresAtStartup(fullSnapshotTimeWindow)
				Expect(isFullSnapCanBeMissed).Should(BeTrue())
			})
		})
	})
})

// prepareExpectedSnapshotsList prepares the expected snapshotlist based on directory structure
func prepareExpectedSnapshotsList(snapTime time.Time, now time.Time, expectedSnapList brtypes.SnapList, directoryStruct string) brtypes.SnapList {
	// weekly snapshot
	for i := 1; i <= 4; i++ {
		snapTime = snapTime.Add(time.Duration(time.Hour * 24 * 7))
		snap := &brtypes.Snapshot{
			Kind:          brtypes.SnapshotKindFull,
			CreatedOn:     snapTime,
			StartRevision: 0,
			LastRevision:  1001,
		}
		snap.GenerateSnapshotName()
		if directoryStruct != snapsInV2 {
			snap.GenerateSnapshotDirectory()
		}

		expectedSnapList = append(expectedSnapList, snap)
	}
	fmt.Println("Weekly snapshot list prepared")

	// daily snapshot
	// in case of mixed directory structure, we would list snaps of first 3 days from v1 structure and rest of all snaps from v2 structure
	for i := 1; i <= 7; i++ {
		snapTime = snapTime.Add(time.Duration(time.Hour * 24))
		snap := &brtypes.Snapshot{
			Kind:          brtypes.SnapshotKindFull,
			CreatedOn:     snapTime,
			StartRevision: 0,
			LastRevision:  1001,
		}
		snap.GenerateSnapshotName()
		if directoryStruct == "mixed" {
			if i <= 3 {
				snap.GenerateSnapshotDirectory()
			}
		}
		if directoryStruct == snapsInV1 {
			snap.GenerateSnapshotDirectory()
		}
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
		snap.GenerateSnapshotName()
		if directoryStruct == snapsInV1 {
			snap.GenerateSnapshotDirectory()
		}
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
	snap.GenerateSnapshotName()
	if directoryStruct == snapsInV1 {
		snap.GenerateSnapshotDirectory()
	}
	expectedSnapList = append(expectedSnapList, snap)
	snapTime = snapTime.Add(time.Duration(time.Minute * 30))
	for now.Sub(snapTime) >= 0 {
		snap := &brtypes.Snapshot{
			Kind:          brtypes.SnapshotKindFull,
			CreatedOn:     snapTime,
			StartRevision: 0,
			LastRevision:  1001,
		}
		snap.GenerateSnapshotName()
		if directoryStruct == snapsInV1 {
			snap.GenerateSnapshotDirectory()
		}
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
		snap.GenerateSnapshotName()
		if directoryStruct == snapsInV1 {
			snap.GenerateSnapshotDirectory()
		}
		expectedSnapList = append(expectedSnapList, snap)
		snapTime = snapTime.Add(time.Duration(time.Minute * 10))
	}
	fmt.Println("Incremental snapshot list prepared")
	return expectedSnapList
}

/*func prepareExpectedSnapshotsList(snapTime time.Time, now time.Time, mode string) brtypes.SnapList {
	var expectedSnapList brtypes.SnapList
	var dir string

	// weekly snapshot
	for i := 1; i <= 4; i++ {
		snapTime = snapTime.Add(time.Duration(time.Hour * 24 * 7))
		if (mode == snapsInV1) || (mode == mixed && (i == 1 || i == 2)) {
			dir = fmt.Sprintf("Backup-%d", snapTime.Unix())
		} else {
			dir = ""
		}
		snap := &brtypes.Snapshot{
			Kind:          brtypes.SnapshotKindFull,
			CreatedOn:     snapTime,
			StartRevision: 0,
			LastRevision:  1001,
			SnapDir:       dir,
		}
		snap.GenerateSnapshotName()
		expectedSnapList = append(expectedSnapList, snap)
	}
	fmt.Println("Weekly snapshot list prepared")
	fmt.Printf("len: %d", len(expectedSnapList))

	// daily snapshot
	for i := 1; i <= 7; i++ {
		snapTime = snapTime.Add(time.Duration(time.Hour * 24))
		if mode == snapsInV1 {
			dir = fmt.Sprintf("Backup-%d", snapTime.Unix())
		} else {
			dir = ""
		}
		snap := &brtypes.Snapshot{
			Kind:          brtypes.SnapshotKindFull,
			CreatedOn:     snapTime,
			StartRevision: 0,
			LastRevision:  1001,
			SnapDir:       dir,
		}
		snap.GenerateSnapshotName()
		expectedSnapList = append(expectedSnapList, snap)
	}
	fmt.Println("Daily snapshot list prepared")

	// hourly snapshot
	snapTime = snapTime.Add(time.Duration(time.Hour))
	for now.Truncate(time.Hour).Sub(snapTime) > 0 {
		if mode == snapsInV1 {
			dir = fmt.Sprintf("Backup-%d", snapTime.Unix())
		} else {
			dir = ""
		}
		snap := &brtypes.Snapshot{
			Kind:          brtypes.SnapshotKindFull,
			CreatedOn:     snapTime,
			StartRevision: 0,
			LastRevision:  1001,
			SnapDir:       dir,
		}
		snap.GenerateSnapshotName()
		expectedSnapList = append(expectedSnapList, snap)
		snapTime = snapTime.Add(time.Duration(time.Hour))
	}
	fmt.Println("Hourly snapshot list prepared")

	// current hour
	snapTime = now.Truncate(time.Hour)
	if mode == snapsInV1 {
		dir = fmt.Sprintf("Backup-%d", snapTime.Unix())
	} else {
		dir = ""
	}
	snap := &brtypes.Snapshot{
		Kind:          brtypes.SnapshotKindFull,
		CreatedOn:     snapTime,
		StartRevision: 0,
		LastRevision:  1001,
		SnapDir:       dir,
	}
	snap.GenerateSnapshotName()
	expectedSnapList = append(expectedSnapList, snap)
	snapTime = snapTime.Add(time.Duration(time.Minute * 30))
	for now.Sub(snapTime) >= 0 {
		if mode == snapsInV1 {
			dir = fmt.Sprintf("Backup-%d", snapTime.Unix())
		} else {
			dir = ""
		}
		snap := &brtypes.Snapshot{
			Kind:          brtypes.SnapshotKindFull,
			CreatedOn:     snapTime,
			StartRevision: 0,
			LastRevision:  1001,
			SnapDir:       dir,
		}
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
			SnapDir:       dir,
		}
		snap.GenerateSnapshotName()
		expectedSnapList = append(expectedSnapList, snap)
		snapTime = snapTime.Add(time.Duration(time.Minute * 10))
	}
	fmt.Println("Incremental snapshot list prepared")

	return expectedSnapList
}
*/

// prepareStoreForGarbageCollection populates the store with dummy snapshots for garbage collection tests
func prepareStoreForGarbageCollection(forTime time.Time, storeContainer string, storePrefix string) (brtypes.SnapStore, *brtypes.SnapstoreConfig) {
	var (
		snapTime           = time.Date(forTime.Year(), forTime.Month(), forTime.Day()-36, 0, 0, 0, 0, forTime.Location())
		count              = 0
		noOfDeltaSnapshots = 3
	)
	fmt.Println("setting up garbage collection test")
	// Prepare snapshot directory
	var snapstoreConf *brtypes.SnapstoreConfig
	if storePrefix == "v1" {
		snapstoreConf = &brtypes.SnapstoreConfig{Container: path.Join(outputDir, storeContainer), Prefix: "v1"}
	}

	if storePrefix == "v2" {
		snapstoreConf = &brtypes.SnapstoreConfig{Container: path.Join(outputDir, storeContainer), Prefix: "v2"}
	}
	store, err := snapstore.GetSnapstore(snapstoreConf)
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
		snap.GenerateSnapshotName()
		if storePrefix == "v1" {
			snap.GenerateSnapshotDirectory()
		}
		snapTime = snapTime.Add(time.Duration(time.Minute * 10))
		store.Save(snap, io.NopCloser(strings.NewReader(fmt.Sprintf("dummy-snapshot-content for snap created on %s", snap.CreatedOn))))
	}
	return store, snapstoreConf
}

// prepareStoreForBackwardCompatibleGC populates the store with dummy snapshots in both v1 and v2 drectory structures for backward compatible garbage collection tests
// Tied up with backward compatibility tests
// TODO: Consider removing when backward compatibility no longer needed
func prepareStoreForBackwardCompatibleGC(forTime time.Time, storeContainer string) (brtypes.SnapStore, *brtypes.SnapstoreConfig) {
	var (
		// Divide the forTime into two period. First period is during when snapshots would be collected in v1 and second period is when snapshots would be collected in v2.
		snapTimev1         = time.Date(forTime.Year(), forTime.Month(), forTime.Day()-36, 0, 0, 0, 0, forTime.Location())
		snapTimev2         = time.Date(forTime.Year(), forTime.Month(), forTime.Day()-4, 0, 0, 0, 0, forTime.Location())
		count              = 0
		noOfDeltaSnapshots = 3
	)
	fmt.Println("setting up garbage collection test")
	// Prepare store
	snapstoreConfig := &brtypes.SnapstoreConfig{Container: path.Join(outputDir, storeContainer), Prefix: "v1"}
	store, err := snapstore.GetSnapstore(snapstoreConfig)
	Expect(err).ShouldNot(HaveOccurred())

	for snapTimev2.Sub(snapTimev1) >= 0 {
		var kind = brtypes.SnapshotKindDelta
		if count == 0 {
			kind = brtypes.SnapshotKindFull
		}
		count = (count + 1) % noOfDeltaSnapshots
		snap := brtypes.Snapshot{
			Kind:          kind,
			CreatedOn:     snapTimev1,
			StartRevision: 0,
			LastRevision:  1001,
		}
		snap.GenerateSnapshotName()
		snap.GenerateSnapshotDirectory()
		snapTimev1 = snapTimev1.Add(time.Duration(time.Minute * 10))
		store.Save(snap, io.NopCloser(strings.NewReader(fmt.Sprintf("dummy-snapshot-content for snap created on %s", snap.CreatedOn))))
	}

	count = 0
	// Prepare store
	snapstoreConfig = &brtypes.SnapstoreConfig{Container: path.Join(outputDir, storeContainer), Prefix: "v2"}
	store, err = snapstore.GetSnapstore(snapstoreConfig)
	Expect(err).ShouldNot(HaveOccurred())

	for forTime.Sub(snapTimev2) >= 0 {
		var kind = brtypes.SnapshotKindDelta
		if count == 0 {
			kind = brtypes.SnapshotKindFull
		}
		count = (count + 1) % noOfDeltaSnapshots
		snapv2 := brtypes.Snapshot{
			Kind:          kind,
			CreatedOn:     snapTimev2,
			StartRevision: 0,
			LastRevision:  1001,
		}
		snapv2.GenerateSnapshotName()
		snapTimev2 = snapTimev2.Add(time.Duration(time.Minute * 10))
		store.Save(snapv2, io.NopCloser(strings.NewReader(fmt.Sprintf("dummy-snapshot-content for snap created on %s", snapv2.CreatedOn))))
	}
	return store, snapstoreConfig
}

// validateLimitBasedSnapshots verifies whether the snapshot list after being garbage collected using the limit-based configuration is a valid snapshot list
func validateLimitBasedSnapshots(list brtypes.SnapList, maxBackups uint, mode string) {
	incr := false
	fullSnapCount := 0
	for _, snap := range list {
		if incr == false {
			if snap.Kind == brtypes.SnapshotKindDelta {
				//Indicates that no full snapshot can occur after a incr snapshot in an already garbage collected list
				incr = true
			} else {
				//Number of full snapshots in garbage collected list cannot be more than the maxBackups configuration
				fullSnapCount++
				Expect(fullSnapCount).Should(BeNumerically("<=", maxBackups))
			}
			if mode == snapsInV2 {
				Expect(snap.SnapDir).Should(Equal(""))
			} else if mode == snapsInV1 {
				Expect(snap.SnapDir).Should(ContainSubstring("Backup"))
			}
		} else {
			Expect(snap.Kind).Should(Equal(brtypes.SnapshotKindDelta))
			if mode == snapsInV2 {
				Expect(snap.SnapDir).Should(Equal(""))
			} else if mode == snapsInV1 {
				Expect(snap.SnapDir).Should(ContainSubstring("Backup"))
			}
		}
	}
}
