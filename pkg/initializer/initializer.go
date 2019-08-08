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
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/initializer/validator"
	"github.com/gardener/etcd-backup-restore/pkg/metrics"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/restorer"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	"github.com/prometheus/client_golang/prometheus"
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
func (e *EtcdInitializer) Initialize(mode validator.Mode, failBelowRevision int64) error {
	start := time.Now()
	dataDirStatus, err := e.Validator.Validate(mode, failBelowRevision)
	if err != nil && dataDirStatus != validator.DataDirectoryNotExist {
		metrics.ValidationDurationSeconds.With(prometheus.Labels{metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Observe(time.Now().Sub(start).Seconds())
		return fmt.Errorf("error while initializing: %v", err)
	}

	if dataDirStatus == validator.FailBelowRevisionConsistencyError {
		metrics.ValidationDurationSeconds.With(prometheus.Labels{metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Observe(time.Now().Sub(start).Seconds())
		return fmt.Errorf("failed to initialize since fail below revision check failed")
	}
	metrics.ValidationDurationSeconds.With(prometheus.Labels{metrics.LabelSucceeded: metrics.ValueSucceededTrue}).Observe(time.Now().Sub(start).Seconds())

	if dataDirStatus != validator.DataDirectoryValid {
		start := time.Now()
		if err := e.restoreCorruptData(); err != nil {
			metrics.RestorationDurationSeconds.With(prometheus.Labels{metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Observe(time.Now().Sub(start).Seconds())
			return fmt.Errorf("error while restoring corrupt data: %v", err)
		}
		metrics.RestorationDurationSeconds.With(prometheus.Labels{metrics.LabelSucceeded: metrics.ValueSucceededTrue}).Observe(time.Now().Sub(start).Seconds())
	}
	return nil
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
				DataDir:         options.RestoreDataDir,
				SnapstoreConfig: snapstoreConfig,
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

	if e.Config.SnapstoreConfig == nil {
		logger.Warnf("No snapstore storage provider configured.")
		logger.Infof("Removing data directory(%s) for snapshot restoration.", dataDir)
		if err := os.RemoveAll(filepath.Join(dataDir)); err != nil {
			return fmt.Errorf("failed to delete data directory %s with err: %v", dataDir, err)
		}
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
	tempRestoreOptions := *e.Config.RestoreOptions
	tempRestoreOptions.RestoreDataDir = fmt.Sprintf("%s.%s", tempRestoreOptions.RestoreDataDir, "part")

	logger.Infof("Removing data directory(%s) for snapshot restoration.", tempRestoreOptions.RestoreDataDir)
	if err := os.RemoveAll(filepath.Join(tempRestoreOptions.RestoreDataDir)); err != nil {
		return fmt.Errorf("failed to delete previous temporary data directory %s with err: %v", tempRestoreOptions.RestoreDataDir, err)
	}

	rs := restorer.NewRestorer(store, logger)
	if err := rs.Restore(tempRestoreOptions); err != nil {
		err = fmt.Errorf("Failed to restore snapshot: %v", err)
		return err
	}

	if err := e.removeContents(dataDir); err != nil {
		return fmt.Errorf("failed to remove corrupt contents with restored snapshot: %v", err)
	}
	logger.Infoln("Successfully restored the etcd data directory.")
	return nil
}

func (e *EtcdInitializer) removeContents(dataDir string) error {
	logger := e.Logger
	logger.Infof("Removing data directory(%s) for snapshot restoration.", dataDir)
	if err := os.RemoveAll(filepath.Join(dataDir)); err != nil {
		return fmt.Errorf("failed to delete data directory %s with err: %v", dataDir, err)
	}

	if err := os.Rename(filepath.Join(fmt.Sprintf("%s.%s", dataDir, "part")), filepath.Join(dataDir)); err != nil {
		return fmt.Errorf("Failed to rename temp restore directory %s to data directory %s with err: %v", filepath.Join(fmt.Sprintf("%s.%s", dataDir, "part")), dataDir, err)
	}
	return nil
}
