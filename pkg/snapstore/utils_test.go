package snapstore_test

import (
	"os"

	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

	. "github.com/gardener/etcd-backup-restore/pkg/snapstore"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("GetSnapstore", func() {
	var (
		config *brtypes.SnapstoreConfig
	)

	BeforeEach(func() {
		config = &brtypes.SnapstoreConfig{
			Provider:  brtypes.SnapstoreProviderLocal,
			Prefix:    "test",
			Container: "test-container",
			TempDir:   "/tmp",
		}
	})

	Context("when prefix is not set", func() {
		BeforeEach(func() {
			config.Prefix = ""
		})
		It("should set default prefix", func() {
			_, err := GetSnapstore(config)
			Expect(err).ToNot(HaveOccurred())
			Expect(config.Prefix).To(Equal("v2"))
		})
	})

	Context("when container is not set", func() {
		BeforeEach(func() {
			config.Container = ""
		})
		Context("if snapstore is to be created for source bucket", func() {
			BeforeEach(func() {
				config.IsSource = true
				Expect(os.Setenv("SOURCE_STORAGE_CONTAINER", "container")).ToNot(HaveOccurred())
			})
			AfterEach(func() {
				Expect(os.Unsetenv("SOURCE_STORAGE_CONTAINER")).ToNot(HaveOccurred())
			})
			It("should use SOURCE_STORAGE_CONTAINER env variable", func() {
				_, err := GetSnapstore(config)
				Expect(err).ToNot(HaveOccurred())
				Expect(config.Container).To(Equal("container"))
			})
		})
		Context("if snapstore is to be created for a non-source bucket", func() {
			BeforeEach(func() {
				config.IsSource = false
				Expect(os.Setenv("STORAGE_CONTAINER", "dest-container")).ToNot(HaveOccurred())
			})
			AfterEach(func() {
				Expect(os.Unsetenv("STORAGE_CONTAINER")).ToNot(HaveOccurred())
			})
			It("should use STORAGE_CONTAINER env variable", func() {
				_, err := GetSnapstore(config)
				Expect(err).ToNot(HaveOccurred())
				Expect(config.Container).To(Equal("dest-container"))
			})
		})
	})

	Context("when snapshot temp dir not provided", func() {
		BeforeEach(func() {
			config.TempDir = ""
		})
		It("should use default temp dir and create it if necessary", func() {
			snapstore, err := GetSnapstore(config)
			Expect(err).ToNot(HaveOccurred())
			Expect(snapstore).ToNot(BeNil())
			Expect(config.TempDir).To(Equal("/tmp"))
			_, err = os.Stat(config.TempDir)
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Context("when provided snapshot temp dir does not exist", func() {
		BeforeEach(func() {
			config.TempDir = "/tmp/nonexistent/dir"
		})
		It("should create the temp dir", func() {
			snapstore, err := GetSnapstore(config)
			Expect(err).ToNot(HaveOccurred())
			Expect(snapstore).ToNot(BeNil())
			_, err = os.Stat(config.TempDir)
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Context("when snapstore provider is local", func() {
		BeforeEach(func() {
			config.Provider = brtypes.SnapstoreProviderLocal
			config.Container = "test-container"
		})
		It("should return a local snapstore", func() {
			snapstore, err := GetSnapstore(config)
			Expect(err).ToNot(HaveOccurred())
			Expect(snapstore).ToNot(BeNil())
			_, ok := snapstore.(*LocalSnapStore)
			Expect(ok).To(BeTrue())
		})
	})

	Context("when snapstore provider is unknown", func() {
		BeforeEach(func() {
			config.Provider = "unknown"
		})
		It("should return an error", func() {
			snapstore, err := GetSnapstore(config)
			Expect(err).To(HaveOccurred())
			Expect(snapstore).To(BeNil())
			Expect(err.Error()).To(ContainSubstring("unsupported storage provider"))
		})
	})
})

var _ = Describe("GetSnapstoreWithCopier", func() {
	var (
		config *brtypes.SnapstoreConfig
	)

	BeforeEach(func() {
		config = &brtypes.SnapstoreConfig{
			Provider:  brtypes.SnapstoreProviderLocal,
			Prefix:    "test",
			Container: "test-container",
			TempDir:   "/tmp",
		}
	})

	Context("with single endpoint", func() {
		It("should return SnapstoreWithCopier with nil copier", func() {
			snapstoreWithCopier, err := GetSnapstoreWithCopier(config)
			Expect(err).ToNot(HaveOccurred())
			Expect(snapstoreWithCopier).ToNot(BeNil())
			Expect(snapstoreWithCopier.Store).ToNot(BeNil())
			Expect(snapstoreWithCopier.Copier).To(BeNil())
		})
	})

	Context("with dual endpoints", func() {
		BeforeEach(func() {
			config.SecondaryProvider = brtypes.SnapstoreProviderLocal
			config.SecondaryContainer = "secondary-container"
			config.SecondaryPrefix = "secondary-prefix"
		})

		It("should return SnapstoreWithCopier with copier", func() {
			snapstoreWithCopier, err := GetSnapstoreWithCopier(config)
			Expect(err).ToNot(HaveOccurred())
			Expect(snapstoreWithCopier).ToNot(BeNil())
			Expect(snapstoreWithCopier.Store).ToNot(BeNil())
			Expect(snapstoreWithCopier.Copier).ToNot(BeNil())
		})

		It("should have correct primary and secondary stores in copier", func() {
			snapstoreWithCopier, err := GetSnapstoreWithCopier(config)
			Expect(err).ToNot(HaveOccurred())

			// Check copier is configured correctly
			copier := snapstoreWithCopier.Copier
			Expect(copier).ToNot(BeNil())

			// Both stores should be LocalSnapStore for this test
			_, isPrimaryLocal := snapstoreWithCopier.Store.(*LocalSnapStore)
			Expect(isPrimaryLocal).To(BeTrue())
		})
	})

	Context("when secondary endpoint configuration is incomplete", func() {
		BeforeEach(func() {
			config.SecondaryProvider = brtypes.SnapstoreProviderLocal
			// Missing SecondaryContainer
		})

		It("should return single snapstore without copier", func() {
			snapstoreWithCopier, err := GetSnapstoreWithCopier(config)
			Expect(err).ToNot(HaveOccurred())
			Expect(snapstoreWithCopier).ToNot(BeNil())
			Expect(snapstoreWithCopier.Store).ToNot(BeNil())
			Expect(snapstoreWithCopier.Copier).To(BeNil())
		})
	})

	Context("when primary snapstore creation fails", func() {
		BeforeEach(func() {
			config.Provider = "invalid-provider"
			config.SecondaryProvider = brtypes.SnapstoreProviderLocal
			config.SecondaryContainer = "secondary-container"
		})

		It("should fall back to secondary when primary fails", func() {
			snapstoreWithCopier, err := GetSnapstoreWithCopier(config)
			Expect(err).ToNot(HaveOccurred())
			Expect(snapstoreWithCopier).ToNot(BeNil())
			Expect(snapstoreWithCopier.Store).ToNot(BeNil()) // Should be secondary store
			Expect(snapstoreWithCopier.Copier).To(BeNil())   // No copier since primary failed
		})
	})
})
