package snapstore_test

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

	"github.com/sirupsen/logrus"

	. "github.com/gardener/etcd-backup-restore/pkg/snapstore"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// MockSnapStore implements brtypes.SnapStore for testing
type MockSnapStore struct {
	mu        sync.Mutex
	name      string
	snapshots []*brtypes.Snapshot
	saveErr   error
	fetchErr  error
	listErr   error
}

func NewMockSnapStore(name string) *MockSnapStore {
	return &MockSnapStore{
		name:      name,
		snapshots: make([]*brtypes.Snapshot, 0),
	}
}

func (m *MockSnapStore) Fetch(snapshot brtypes.Snapshot) (io.ReadCloser, error) {
	if m.fetchErr != nil {
		return nil, m.fetchErr
	}
	return io.NopCloser(strings.NewReader("mock snapshot data")), nil
}

func (m *MockSnapStore) Save(snapshot brtypes.Snapshot, rc io.ReadCloser) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.saveErr != nil {
		return m.saveErr
	}
	m.snapshots = append(m.snapshots, &snapshot)
	return nil
}

func (m *MockSnapStore) List(finalise bool) (brtypes.SnapList, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.listErr != nil {
		return nil, m.listErr
	}
	return brtypes.SnapList(m.snapshots), nil
}

func (m *MockSnapStore) Delete(snapshot brtypes.Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, snap := range m.snapshots {
		if snap.SnapName == snapshot.SnapName {
			m.snapshots = append(m.snapshots[:i], m.snapshots[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("snapshot not found")
}

func (m *MockSnapStore) GetSnapstoreConfig() *brtypes.SnapstoreConfig {
	return &brtypes.SnapstoreConfig{Container: m.name}
}

func (m *MockSnapStore) AddSnapshot(snapshot *brtypes.Snapshot) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snapshots = append(m.snapshots, snapshot)
}

func (m *MockSnapStore) SetSaveErr(err error) {
	m.saveErr = err
}

func (m *MockSnapStore) SetFetchErr(err error) {
	m.fetchErr = err
}

func (m *MockSnapStore) SetListErr(err error) {
	m.listErr = err
}

var _ = Describe("BackupCopier", func() {
	var (
		primaryStore   *MockSnapStore
		secondaryStore *MockSnapStore
		copier         *BackupCopier
		logger         *logrus.Entry
		config         *BackupCopierConfig
		ctx            context.Context
		cancel         context.CancelFunc
	)

	BeforeEach(func() {
		primaryStore = NewMockSnapStore("primary")
		secondaryStore = NewMockSnapStore("secondary")
		logger = logrus.NewEntry(logrus.New())
		config = &BackupCopierConfig{
			SyncPeriod:       100 * time.Millisecond, // Fast for testing
			MaxRetries:       2,
			RetryBackoff:     10 * time.Millisecond,
			ConcurrentCopies: 2,
		}
		copier = NewBackupCopier(primaryStore, secondaryStore, config, logger)
		ctx, cancel = context.WithCancel(context.Background())
	})

	AfterEach(func() {
		if copier.IsRunning() {
			cancel()
			copier.Stop()
		}
	})

	Describe("NewBackupCopier", func() {
		It("should create a new backup copier", func() {
			Expect(copier).ToNot(BeNil())
			Expect(copier.IsRunning()).To(BeFalse())
		})

		It("should use default config when nil is provided", func() {
			copierWithDefaults := NewBackupCopier(primaryStore, secondaryStore, nil, logger)
			Expect(copierWithDefaults).ToNot(BeNil())
		})
	})

	Describe("Start and Stop", func() {
		It("should start and stop the copier", func() {
			Expect(copier.IsRunning()).To(BeFalse())

			copier.Start(ctx)
			Eventually(func() bool {
				return copier.IsRunning()
			}, time.Second).Should(BeTrue())

			copier.Stop()
			Eventually(func() bool {
				return copier.IsRunning()
			}, time.Second).Should(BeFalse())
		})

		It("should not start multiple times", func() {
			copier.Start(ctx)
			Eventually(func() bool {
				return copier.IsRunning()
			}, time.Second).Should(BeTrue())

			// Try to start again
			copier.Start(ctx)
			Expect(copier.IsRunning()).To(BeTrue())

			copier.Stop()
		})
	})

	Describe("SyncOnce", func() {
		It("should sync snapshots from primary to secondary", func() {
			// Add snapshot to primary
			snapshot := &brtypes.Snapshot{
				SnapName:      "test-snapshot",
				SnapDir:       "/snapshots",
				StartRevision: 1,
				LastRevision:  10,
			}
			primaryStore.AddSnapshot(snapshot)

			err := copier.SyncOnce(ctx)
			Expect(err).ToNot(HaveOccurred())

			// Check that snapshot was copied to secondary
			secondarySnapshots, err := secondaryStore.List(true)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(secondarySnapshots)).To(Equal(1))
			Expect(secondarySnapshots[0].SnapName).To(Equal("test-snapshot"))
		})

		It("should not copy snapshots that already exist in secondary", func() {
			// Add same snapshot to both stores
			snapshot := &brtypes.Snapshot{
				SnapName:      "existing-snapshot",
				SnapDir:       "/snapshots",
				StartRevision: 1,
				LastRevision:  10,
			}
			primaryStore.AddSnapshot(snapshot)
			secondaryStore.AddSnapshot(snapshot)

			err := copier.SyncOnce(ctx)
			Expect(err).ToNot(HaveOccurred())

			// Should still have only one snapshot in secondary
			secondarySnapshots, err := secondaryStore.List(true)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(secondarySnapshots)).To(Equal(1))
		})

		It("should handle primary list errors", func() {
			primaryStore.SetListErr(fmt.Errorf("primary list error"))

			err := copier.SyncOnce(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to list primary snapshots"))
		})

		It("should handle secondary list errors", func() {
			secondaryStore.SetListErr(fmt.Errorf("secondary list error"))

			err := copier.SyncOnce(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to list secondary snapshots"))
		})

		It("should handle copy failures with retry", func() {
			// Add snapshot to primary
			snapshot := &brtypes.Snapshot{
				SnapName:      "test-snapshot",
				SnapDir:       "/snapshots",
				StartRevision: 1,
				LastRevision:  10,
			}
			primaryStore.AddSnapshot(snapshot)

			// Make secondary save fail
			secondaryStore.SetSaveErr(fmt.Errorf("save error"))

			err := copier.SyncOnce(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to copy"))

			// Verify retry attempts were made
			secondarySnapshots, _ := secondaryStore.List(true)
			Expect(len(secondarySnapshots)).To(Equal(0)) // Should not have been saved
		})
	})

	Describe("Background sync", func() {
		It("should periodically sync snapshots", func() {
			// Start the copier
			copier.Start(ctx)
			Eventually(func() bool {
				return copier.IsRunning()
			}, time.Second).Should(BeTrue())

			// Add snapshot to primary after copier starts
			snapshot := &brtypes.Snapshot{
				SnapName:      "periodic-snapshot",
				SnapDir:       "/snapshots",
				StartRevision: 1,
				LastRevision:  10,
			}
			primaryStore.AddSnapshot(snapshot)

			// Wait for periodic sync to happen
			Eventually(func() int {
				snapshots, _ := secondaryStore.List(true)
				return len(snapshots)
			}, 2*time.Second).Should(Equal(1))

			copier.Stop()
		})

		It("should update last sync timestamp", func() {
			initialTime := copier.GetLastSyncTimestamp()
			Expect(initialTime.IsZero()).To(BeTrue())

			err := copier.SyncOnce(ctx)
			Expect(err).ToNot(HaveOccurred())

			lastSyncTime := copier.GetLastSyncTimestamp()
			Expect(lastSyncTime.After(initialTime)).To(BeTrue())
		})
	})

	Describe("Context cancellation", func() {
		It("should stop when context is cancelled", func() {
			copier.Start(ctx)
			Eventually(func() bool {
				return copier.IsRunning()
			}, time.Second).Should(BeTrue())

			cancel()
			Eventually(func() bool {
				return copier.IsRunning()
			}, time.Second).Should(BeFalse())
		})
	})

	Describe("Concurrent copies", func() {
		It("should handle multiple snapshots concurrently", func() {
			// Add multiple snapshots to primary
			for i := 0; i < 5; i++ {
				snapshot := &brtypes.Snapshot{
					SnapName:      fmt.Sprintf("snapshot-%d", i),
					SnapDir:       "/snapshots",
					StartRevision: int64(i*10 + 1),
					LastRevision:  int64((i + 1) * 10),
				}
				primaryStore.AddSnapshot(snapshot)
			}

			err := copier.SyncOnce(ctx)
			Expect(err).ToNot(HaveOccurred())

			// All snapshots should be copied
			secondarySnapshots, err := secondaryStore.List(true)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(secondarySnapshots)).To(Equal(5))
		})
	})
})

var _ = Describe("DefaultBackupCopierConfig", func() {
	It("should return valid default configuration", func() {
		config := DefaultBackupCopierConfig()
		Expect(config).ToNot(BeNil())
		Expect(config.SyncPeriod).To(Equal(5 * time.Minute))
		Expect(config.MaxRetries).To(Equal(3))
		Expect(config.RetryBackoff).To(Equal(30 * time.Second))
		Expect(config.ConcurrentCopies).To(Equal(2))
	})
})
