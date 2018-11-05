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

package initializer

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/gardener/etcd-backup-restore/pkg/initializer/validator"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/restorer"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	"github.com/sirupsen/logrus"
)

const (
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
	if err != nil && dataDirStatus != validator.DataDirectoryNotExist {
		err = fmt.Errorf("error while initializing: %v", err)
		return err
	}
	if dataDirStatus != validator.DataDirectoryValid {
		if err := e.restoreCorruptData(); err != nil {
			return fmt.Errorf("error while restoring corrupt data: %v", err)
		}
	}
	return err
}

//NewInitializer creates an etcd initializer object.
func NewInitializer(options *restorer.RestoreOptions, snapstoreConfig *snapstore.Config, logger *logrus.Logger) *EtcdInitializer {

	etcdInit := &EtcdInitializer{
		Config: &Config{
			SnapstoreConfig: snapstoreConfig,
			RestoreOptions:  options,
		},
		Validator: &validator.DataValidator{
			Config: &validator.Config{
				DataDir: options.RestoreDataDir,
			},
			Logger: logger,
		},
		Logger: logger,
	}

	return etcdInit
}

func (e *EtcdInitializer) restoreCorruptData() error {
	logger := e.Logger
	dataDir := e.Config.RestoreOptions.RestoreDataDir
	logger.Infof("Removing data directory(%s) for snapshot restoration.", dataDir)
	err := removeContents(filepath.Join(dataDir))
	if err != nil {
		err = fmt.Errorf("failed to delete the Data directory: %v", err)
		return err
	}
	if e.Config.SnapstoreConfig == nil {
		logger.Warnf("No snapstore storage provider configured. Will only clean the directory.")
		return nil
	}
	store, err := snapstore.GetSnapstore(e.Config.SnapstoreConfig)
	if err != nil {
		err = fmt.Errorf("failed to create snapstore from configured storage provider: %v", err)
		return err
	}
	logger.Info("Finding latest set of snapshot to recover from...")
	baseSnap, deltaSnapList, err := miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
	if err != nil {
		logger.Errorf("failed to get latest set of snapshot: %v", err)
		return err
	}
	if baseSnap == nil {
		logger.Infof("No snapshot found. Will do nothing.")
		return nil
	}

	e.Config.RestoreOptions.BaseSnapshot = *baseSnap
	e.Config.RestoreOptions.DeltaSnapList = deltaSnapList

	rs := restorer.NewRestorer(store, logger)

	if err := rs.Restore(*e.Config.RestoreOptions); err != nil {
		err = fmt.Errorf("Failed to restore snapshot: %v", err)
		return err
	}
	logger.Info("Successfully restored the etcd data directory.")
	return err
}

func removeContents(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		err = os.RemoveAll(filepath.Join(dir, name))
		if err != nil {
			return err
		}
	}
	return nil
}
