package initializer

import (
	"fmt"
	"os"
	"path"
	"path/filepath"

	"github.com/gardener/etcd-backup-restore/pkg/initializer/validator"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	"github.com/sirupsen/logrus"
)

const (
	envStorageContainer = "STORAGE_CONTAINER"
	defaultLocalStore   = "default.etcd.bkp"
	backupFormatVersion = "v1"
)

// Initialize has the following steps:
//   * Check if data directory exists.
//     - If data directory exists
//       * Check for data corruption.
//			- If data directory is in corrupted state, clear the data directory.
//     - If data directory does not exist.
//       * Check if Latest snapshot available.
//		   - Try to perform an Etcd data restoration from the latest snapshot.
//		   - No snapshots are available, start etcd as a fresh installation.
func (e *EtcdInitializer) Initialize() error {
	dataDirStatus, err := e.Validator.Validate()
	if err != nil {
		e.Logger.Errorf("error while initializing: %v", err)
		return err
	}
	if dataDirStatus != validator.DataDirectoryValid {
		err = e.restoreCorruptData()
		e.Logger.Errorf("error while intializing: %v", err)
	}
	return err
}

//NewInitializer creates an etcd initializer object.
func NewInitializer(dataDir, storageProvider string, logger *logrus.Logger) *EtcdInitializer {

	etcdInit := &EtcdInitializer{
		Config: &InitializerConfig{
			DataDir:         dataDir,
			StorageProvider: storageProvider,
		},
		Validator: &validator.DataValidator{
			Config: &validator.ValidatorConfig{
				DataDir: dataDir,
			},
			Logger: logger,
		},
		Logger: logger,
	}

	return etcdInit
}

func (e *EtcdInitializer) restoreCorruptData() error {
	logger := e.Logger
	dataDir := e.Config.DataDir
	storageProvider := e.Config.StorageProvider
	logger.Infof("Removing data directory(%s) for snapshot restoration.", e.Config.DataDir)
	err := os.RemoveAll(filepath.Join(dataDir))
	if err != nil {
		logger.Errorf("failed to delete the Data directory: %v", err)
		return err
	}
	store, err := getSnapstore(storageProvider)
	if err != nil {
		logger.Errorf("failed to create snapstore from configured storage provider: %v", err)
		return err
	}
	logger.Infoln("Finding latest snapshot...")
	snap, err := store.GetLatest()
	if err != nil {
		logger.Errorf("failed to get latest snapshot: %v", err)
		return err
	}
	if snap == nil {
		logger.Infof("No snapshot found. Will do nothing.")
		return err
	}
	return err
}

// getSnapstore returns the snapstore object for give storageProvider with specified container
func getSnapstore(storageProvider string) (snapstore.SnapStore, error) {
	switch storageProvider {
	case snapstore.SnapstoreProviderLocal, "":
		container := os.Getenv(envStorageContainer)
		if container == "" {
			container = defaultLocalStore
		}
		return snapstore.NewLocalSnapStore(path.Join(container, backupFormatVersion))
	case snapstore.SnapstoreProviderS3:
		container := os.Getenv(envStorageContainer)
		if container == "" {
			return nil, fmt.Errorf("storage container name not specified")
		}
		return snapstore.NewS3SnapStore(container, backupFormatVersion)
	default:
		return nil, fmt.Errorf("unsupported storage provider : %s", storageProvider)

	}
}
