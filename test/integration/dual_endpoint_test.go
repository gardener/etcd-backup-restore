// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("DualEndpoint", func() {
	var (
		testTempDir                        string
		primaryContainer, primaryStore     string
		secondaryContainer, secondaryStore string
		primaryConfig, secondaryConfig     *brtypes.SnapstoreConfig
		cleanupEnvVars                     []string
	)

	BeforeEach(func() {

		// Create temporary directory for test files
		var err error
		testTempDir, err = os.MkdirTemp("", "dual-endpoint-test-*")
		Expect(err).ShouldNot(HaveOccurred())

		// Setup unique container names for this test
		testID := strings.ReplaceAll(testTempDir, "/", "_")
		primaryContainer = fmt.Sprintf("primary-%s", testID)
		secondaryContainer = fmt.Sprintf("secondary-%s", testID)
		primaryStore = filepath.Join(testTempDir, "primary")
		secondaryStore = filepath.Join(testTempDir, "secondary")

		// Create directories for local snapstores
		Expect(os.MkdirAll(primaryStore, 0755)).To(Succeed())
		Expect(os.MkdirAll(secondaryStore, 0755)).To(Succeed())

		// Configure primary snapstore
		primaryConfig = &brtypes.SnapstoreConfig{
			Provider:  brtypes.SnapstoreProviderLocal,
			Container: primaryStore,
			Prefix:    "v2",
			TempDir:   testTempDir,
		}

		// Configure secondary snapstore
		secondaryConfig = &brtypes.SnapstoreConfig{
			Provider:  brtypes.SnapstoreProviderLocal,
			Container: secondaryStore,
			Prefix:    "v2",
			TempDir:   testTempDir,
		}

		// Track environment variables we set for cleanup
		cleanupEnvVars = []string{}
	})

	AfterEach(func() {
		// Cleanup environment variables
		for _, envVar := range cleanupEnvVars {
			os.Unsetenv(envVar)
		}

		// Cleanup temporary directory
		if testTempDir != "" {
			os.RemoveAll(testTempDir)
		}
	})

	setEnvVar := func(key, value string) {
		os.Setenv(key, value)
		cleanupEnvVars = append(cleanupEnvVars, key)
	}

	Context("Single Endpoint Configuration", func() {
		It("should create snapstore without copier for single endpoint", func() {
			swc, err := snapstore.GetSnapstoreWithCopier(primaryConfig)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(swc).ShouldNot(BeNil())
			Expect(swc.Store).ShouldNot(BeNil())
			Expect(swc.Copier).Should(BeNil()) // No copier for single endpoint
		})

		It("should handle single endpoint with GetSnapstoreWithCopier", func() {
			swc, err := snapstore.GetSnapstoreWithCopier(primaryConfig)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(swc).ShouldNot(BeNil())
			Expect(swc.Store).ShouldNot(BeNil())
			Expect(swc.Copier).Should(BeNil()) // No copier for single endpoint
		})
	})

	Context("Dual Endpoint Configuration via Config Fields", func() {
		var dualConfig *brtypes.SnapstoreConfig

		BeforeEach(func() {
			dualConfig = &brtypes.SnapstoreConfig{
				Provider:           brtypes.SnapstoreProviderLocal,
				Container:          primaryStore,
				Prefix:             "v2",
				TempDir:            testTempDir,
				SecondaryProvider:  brtypes.SnapstoreProviderLocal,
				SecondaryContainer: secondaryStore,
				SecondaryPrefix:    "v2",
			}
		})

		It("should create snapstore with copier for dual endpoints", func() {
			swc, err := snapstore.GetSnapstoreWithCopier(dualConfig)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(swc).ShouldNot(BeNil())
			Expect(swc.Store).ShouldNot(BeNil())
			Expect(swc.Copier).ShouldNot(BeNil()) // Should have copier for dual endpoints
		})

		It("should create snapstore with copier using resilient function", func() {
			swc, err := snapstore.GetSnapstoreWithCopier(dualConfig)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(swc).ShouldNot(BeNil())
			Expect(swc.Store).ShouldNot(BeNil())
			Expect(swc.Copier).ShouldNot(BeNil()) // Should have copier for dual endpoints
		})

		It("should perform basic backup operations to both endpoints", func() {
			swc, err := snapstore.GetSnapstoreWithCopier(dualConfig)
			Expect(err).ShouldNot(HaveOccurred())

			// Create a test snapshot
			snapshot := &brtypes.Snapshot{
				Kind:          brtypes.SnapshotKindFull,
				StartRevision: 0,
				LastRevision:  1000,
				CreatedOn:     time.Now(),
				Prefix:        "v2",
			}
			snapshot.GenerateSnapshotName()

			// Create test data
			testData := "test snapshot data for dual endpoint integration test"
			testFile := filepath.Join(testTempDir, "test-snapshot.db")
			err = os.WriteFile(testFile, []byte(testData), 0644)
			Expect(err).ShouldNot(HaveOccurred())

			file, err := os.Open(testFile)
			Expect(err).ShouldNot(HaveOccurred())
			defer file.Close()

			// Save to primary store
			err = swc.Store.Save(*snapshot, file)
			Expect(err).ShouldNot(HaveOccurred())

			// Verify the snapshot exists in primary store
			snapList, err := swc.Store.List(false)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(snapList).ShouldNot(BeEmpty())

			// Start the copier to sync to secondary
			if swc.Copier != nil {
				ctx := context.Background()
				swc.Copier.Start(ctx)
				defer swc.Copier.Stop()

				// Wait for background sync to complete (giving it reasonable time)
				time.Sleep(2 * time.Second)

				// Verify synchronization occurred
				secondaryStore, err := snapstore.GetSnapstore(secondaryConfig)
				Expect(err).ShouldNot(HaveOccurred())

				secondarySnapList, err := secondaryStore.List(false)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(secondarySnapList).ShouldNot(BeEmpty())
				Expect(len(secondarySnapList)).Should(Equal(len(snapList)))
			}
		})
	})

	Context("Dual Endpoint Configuration via Environment Variables", func() {
		BeforeEach(func() {
			// Configure primary endpoint
			setEnvVar("STORAGE_CONTAINER", primaryContainer)

			// Configure secondary endpoint via environment variables
			setEnvVar("SECONDARY_STORAGE_CONTAINER", secondaryContainer)
			setEnvVar("SECONDARY_STORAGE_PROVIDER", brtypes.SnapstoreProviderLocal)
		})

		It("should detect dual endpoint configuration from environment variables", func() {
			config := &brtypes.SnapstoreConfig{
				Provider:  brtypes.SnapstoreProviderLocal,
				Container: primaryStore, // Override env var for local testing
				Prefix:    "v2",
				TempDir:   testTempDir,
			}

			// Override the secondary container to use local path instead of env var
			config.SecondaryContainer = secondaryStore

			swc, err := snapstore.GetSnapstoreWithCopier(config)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(swc).ShouldNot(BeNil())
			Expect(swc.Store).ShouldNot(BeNil())
			Expect(swc.Copier).ShouldNot(BeNil()) // Should have copier due to secondary config
		})

		It("should use resilient snapstore creation with environment configuration", func() {
			config := &brtypes.SnapstoreConfig{
				Provider:  brtypes.SnapstoreProviderLocal,
				Container: primaryStore, // Override env var for local testing
				Prefix:    "v2",
				TempDir:   testTempDir,
			}

			// Override the secondary container to use local path instead of env var
			config.SecondaryContainer = secondaryStore

			swc, err := snapstore.GetSnapstoreWithCopier(config)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(swc).ShouldNot(BeNil())
			Expect(swc.Store).ShouldNot(BeNil())
			Expect(swc.Copier).ShouldNot(BeNil()) // Should have copier due to secondary config
		})
	})

	Context("Error Handling and Resilience", func() {
		It("should handle primary store failure gracefully", func() {
			// Configure with invalid primary but valid secondary
			config := &brtypes.SnapstoreConfig{
				Provider:           "INVALID_PROVIDER",
				Container:          "invalid-container",
				Prefix:             "v2",
				TempDir:            testTempDir,
				SecondaryProvider:  brtypes.SnapstoreProviderLocal,
				SecondaryContainer: secondaryStore,
				SecondaryPrefix:    "v2",
			}

			swc, err := snapstore.GetSnapstoreWithCopier(config)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(swc).ShouldNot(BeNil())
			Expect(swc.Store).ShouldNot(BeNil()) // Should fallback to secondary as primary
			Expect(swc.Copier).Should(BeNil())   // No copier since only one store available
		})

		It("should handle secondary store failure gracefully", func() {
			// Configure with valid primary but invalid secondary
			config := &brtypes.SnapstoreConfig{
				Provider:           brtypes.SnapstoreProviderLocal,
				Container:          primaryStore,
				Prefix:             "v2",
				TempDir:            testTempDir,
				SecondaryProvider:  "INVALID_PROVIDER",
				SecondaryContainer: "invalid-secondary",
				SecondaryPrefix:    "v2",
			}

			swc, err := snapstore.GetSnapstoreWithCopier(config)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(swc).ShouldNot(BeNil())
			Expect(swc.Store).ShouldNot(BeNil()) // Should use primary store
			Expect(swc.Copier).Should(BeNil())   // No copier since secondary failed
		})

		It("should fail when both stores are invalid", func() {
			// Configure with both invalid stores
			config := &brtypes.SnapstoreConfig{
				Provider:           "INVALID_PROVIDER",
				Container:          "invalid-primary",
				Prefix:             "v2",
				TempDir:            testTempDir,
				SecondaryProvider:  "INVALID_PROVIDER",
				SecondaryContainer: "invalid-secondary",
				SecondaryPrefix:    "v2",
			}

			_, err := snapstore.GetSnapstoreWithCopier(config)
			Expect(err).Should(HaveOccurred())
			Expect(err.Error()).Should(ContainSubstring("failed to create both primary and secondary snapstores"))
		})
	})

	Context("BackupCopier Integration", func() {
		var dualConfig *brtypes.SnapstoreConfig

		BeforeEach(func() {
			dualConfig = &brtypes.SnapstoreConfig{
				Provider:           brtypes.SnapstoreProviderLocal,
				Container:          primaryStore,
				Prefix:             "v2",
				TempDir:            testTempDir,
				SecondaryProvider:  brtypes.SnapstoreProviderLocal,
				SecondaryContainer: secondaryStore,
				SecondaryPrefix:    "v2",
				BackupSyncEnabled:  true,
				BackupSyncPeriod:   1 * time.Second, // Fast sync for testing
			}
		})

		It("should synchronize snapshots in background", func() {
			swc, err := snapstore.GetSnapstoreWithCopier(dualConfig)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(swc.Copier).ShouldNot(BeNil())

			// Create multiple test snapshots BEFORE starting the copier
			createdSnapshots := make([]*brtypes.Snapshot, 3)
			for i := 0; i < 3; i++ {
				snapshot := &brtypes.Snapshot{
					Kind:          brtypes.SnapshotKindFull,
					StartRevision: int64(i * 1000),
					LastRevision:  int64((i + 1) * 1000),
					CreatedOn:     time.Now().Add(time.Duration(i) * time.Second),
					Prefix:        "v2",
				}
				snapshot.GenerateSnapshotName()
				// Don't call GenerateSnapshotDirectory() for v2 snapshots
				createdSnapshots[i] = snapshot

				// Create test data
				testData := fmt.Sprintf("test snapshot data %d", i)
				testFile := filepath.Join(testTempDir, fmt.Sprintf("test-snapshot-%d.db", i))
				err = os.WriteFile(testFile, []byte(testData), 0644)
				Expect(err).ShouldNot(HaveOccurred())

				file, err := os.Open(testFile)
				Expect(err).ShouldNot(HaveOccurred())

				// Save to primary store
				err = swc.Store.Save(*snapshot, file)
				file.Close()
				Expect(err).ShouldNot(HaveOccurred())
			}

			// Verify primary store has all snapshots before starting copier
			primarySnapList, err := swc.Store.List(false)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(len(primarySnapList)).Should(Equal(3))

			// Now start the copier - it should sync existing snapshots
			ctx := context.Background()
			swc.Copier.Start(ctx)
			defer swc.Copier.Stop()

			// Wait for background synchronization (initial sync should happen immediately)
			time.Sleep(2 * time.Second)

			// Verify secondary store has synchronized snapshots
			secondaryStore, err := snapstore.GetSnapstore(secondaryConfig)
			Expect(err).ShouldNot(HaveOccurred())

			secondarySnapList, err := secondaryStore.List(false)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(len(secondarySnapList)).Should(Equal(3))

			// Verify snapshot content matches
			for i, primarySnap := range primarySnapList {
				secondarySnap := secondarySnapList[i]
				Expect(secondarySnap.SnapName).Should(Equal(primarySnap.SnapName))
				Expect(secondarySnap.LastRevision).Should(Equal(primarySnap.LastRevision))
			}
		})

		It("should handle copier lifecycle correctly", func() {
			swc, err := snapstore.GetSnapstoreWithCopier(dualConfig)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(swc.Copier).ShouldNot(BeNil())

			// Create test snapshot BEFORE starting copier lifecycle tests
			snapshot := &brtypes.Snapshot{
				Kind:          brtypes.SnapshotKindFull,
				StartRevision: 0,
				LastRevision:  500,
				CreatedOn:     time.Now(),
				Prefix:        "v2",
			}
			snapshot.GenerateSnapshotName()
			// Don't call GenerateSnapshotDirectory() for v2 snapshots

			testData := "lifecycle test data"
			testFile := filepath.Join(testTempDir, "lifecycle-test.db")
			err = os.WriteFile(testFile, []byte(testData), 0644)
			Expect(err).ShouldNot(HaveOccurred())

			file, err := os.Open(testFile)
			Expect(err).ShouldNot(HaveOccurred())

			// Save to primary store BEFORE starting copier
			err = swc.Store.Save(*snapshot, file)
			file.Close()
			Expect(err).ShouldNot(HaveOccurred())

			// Test start/stop multiple times
			ctx := context.Background()
			for i := 0; i < 3; i++ {
				swc.Copier.Start(ctx)
				time.Sleep(100 * time.Millisecond) // Brief pause
				swc.Copier.Stop()
			}

			// Final start for actual testing
			swc.Copier.Start(ctx)
			defer swc.Copier.Stop()

			// Wait for sync (initial sync should happen on start)
			time.Sleep(2 * time.Second)

			// Verify synchronization still works
			secondaryStore, err := snapstore.GetSnapstore(secondaryConfig)
			Expect(err).ShouldNot(HaveOccurred())

			secondarySnapList, err := secondaryStore.List(false)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(secondarySnapList).ShouldNot(BeEmpty())
		})
	})

	Context("Configuration Validation", func() {
		It("should validate HasSecondaryEndpoint correctly", func() {
			// Test config without secondary endpoint
			singleConfig := &brtypes.SnapstoreConfig{
				Provider:  brtypes.SnapstoreProviderLocal,
				Container: primaryStore,
			}
			Expect(singleConfig.HasSecondaryEndpoint()).Should(BeFalse())

			// Test config with secondary container
			dualConfig := &brtypes.SnapstoreConfig{
				Provider:           brtypes.SnapstoreProviderLocal,
				Container:          primaryStore,
				SecondaryContainer: secondaryStore,
			}
			Expect(dualConfig.HasSecondaryEndpoint()).Should(BeTrue())

			// Test config using environment variable
			setEnvVar("SECONDARY_STORAGE_CONTAINER", "test-secondary")
			envConfig := &brtypes.SnapstoreConfig{
				Provider:  brtypes.SnapstoreProviderLocal,
				Container: primaryStore,
			}
			Expect(envConfig.HasSecondaryEndpoint()).Should(BeTrue())
		})

		It("should create correct secondary config", func() {
			config := &brtypes.SnapstoreConfig{
				Provider:                         brtypes.SnapstoreProviderLocal,
				Container:                        primaryStore,
				Prefix:                           "v2",
				TempDir:                          testTempDir,
				MinChunkSize:                     5 * 1024 * 1024,
				MaxParallelChunkUploads:          4,
				SecondaryProvider:                brtypes.SnapstoreProviderLocal,
				SecondaryContainer:               secondaryStore,
				SecondaryPrefix:                  "v2-secondary",
				SecondaryMinChunkSize:            10 * 1024 * 1024,
				SecondaryMaxParallelChunkUploads: 8,
			}

			secondaryConfig := config.GetSecondaryConfig()
			Expect(secondaryConfig).ShouldNot(BeNil())
			Expect(secondaryConfig.Provider).Should(Equal(brtypes.SnapstoreProviderLocal))
			Expect(secondaryConfig.Container).Should(Equal(secondaryStore))
			Expect(secondaryConfig.Prefix).Should(Equal("v2-secondary"))
			Expect(secondaryConfig.MinChunkSize).Should(Equal(int64(10 * 1024 * 1024)))
			Expect(secondaryConfig.MaxParallelChunkUploads).Should(Equal(uint(8)))
			Expect(secondaryConfig.IsSecondary).Should(BeTrue())
			Expect(secondaryConfig.IsSource).Should(BeFalse())
			Expect(secondaryConfig.TempDir).Should(Equal(testTempDir))
		})
	})
})
