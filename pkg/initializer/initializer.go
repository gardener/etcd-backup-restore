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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/errors"
	"github.com/gardener/etcd-backup-restore/pkg/initializer/validator"
	"github.com/gardener/etcd-backup-restore/pkg/member"
	"github.com/gardener/etcd-backup-restore/pkg/metrics"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/restorer"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"go.uber.org/zap"
	"k8s.io/client-go/util/retry"
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
	isEtcdMemberPresent := false
	ctx := context.Background()
	var err error

	//Etcd cluster scale-up case
	if miscellaneous.IsMultiNode(e.Logger.WithField("actor", "initializer")) {
		m := member.NewMemberControl(e.Config.EtcdConnectionConfig)
		isEtcdMemberPresent, err = m.IsMemberInCluster(ctx)
		if !isEtcdMemberPresent && err == nil {
			retry.OnError(retry.DefaultBackoff, func(err error) bool {
				return err != nil
			}, func() error {
				return m.AddMemberAsLearner(ctx)
			})
			// return here after adding member as no restoration or validation needed
			return nil
		}
	}

	dataDirStatus, err := e.Validator.Validate(mode, failBelowRevision)
	if dataDirStatus == validator.WrongVolumeMounted {
		metrics.ValidationDurationSeconds.With(prometheus.Labels{metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Observe(time.Since(start).Seconds())
		return fmt.Errorf("won't initialize ETCD because wrong ETCD volume is mounted: %v", err)
	}

	if dataDirStatus == validator.FailToOpenBoltDBError {
		metrics.ValidationDurationSeconds.With(prometheus.Labels{metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Observe(time.Since(start).Seconds())
		return fmt.Errorf("failed to initialize since another process still holds the file lock")
	}

	if dataDirStatus == validator.DataDirectoryStatusUnknown {
		metrics.ValidationDurationSeconds.With(prometheus.Labels{metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Observe(time.Since(start).Seconds())
		return fmt.Errorf("error while initializing: %v", err)
	}

	if dataDirStatus == validator.FailBelowRevisionConsistencyError {
		metrics.ValidationDurationSeconds.With(prometheus.Labels{metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Observe(time.Since(start).Seconds())
		return fmt.Errorf("failed to initialize since fail below revision check failed")
	}

	metrics.ValidationDurationSeconds.With(prometheus.Labels{metrics.LabelSucceeded: metrics.ValueSucceededTrue}).Observe(time.Since(start).Seconds())

	if dataDirStatus != validator.DataDirectoryValid {
		if dataDirStatus == validator.DataDirStatusInvalidInMultiNode || (e.Validator.OriginalClusterSize > 1 && dataDirStatus == validator.DataDirectoryCorrupt) {
			if err := e.restoreInMultiNode(ctx); err != nil {
				return err
			}
		} else if e.Validator.OriginalClusterSize > 1 && isEtcdMemberPresent {
			if err := e.restoreInMultiNode(ctx); err != nil {
				return err
			}
		} else {
			// For case: ClusterSize=1 or when multi-node cluster(ClusterSize>1) is bootstrapped
			start := time.Now()
			restored, err := e.restoreCorruptData()
			if err != nil {
				metrics.RestorationDurationSeconds.With(prometheus.Labels{metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Observe(time.Since(start).Seconds())
				return fmt.Errorf("error while restoring corrupt data: %v", err)
			}
			if restored {
				metrics.RestorationDurationSeconds.With(prometheus.Labels{metrics.LabelSucceeded: metrics.ValueSucceededTrue}).Observe(time.Since(start).Seconds())
			}
		}
	}
	return nil
}

//NewInitializer creates an etcd initializer object.
func NewInitializer(options *brtypes.RestoreOptions, snapstoreConfig *brtypes.SnapstoreConfig, etcdConnectionConfig *brtypes.EtcdConnectionConfig, logger *logrus.Logger) *EtcdInitializer {
	zapLogger, _ := zap.NewProduction()
	etcdInit := &EtcdInitializer{
		Config: &Config{
			SnapstoreConfig:      snapstoreConfig,
			RestoreOptions:       options,
			EtcdConnectionConfig: etcdConnectionConfig,
		},
		Validator: &validator.DataValidator{
			Config: &validator.Config{
				DataDir:                options.Config.RestoreDataDir,
				EmbeddedEtcdQuotaBytes: options.Config.EmbeddedEtcdQuotaBytes,
				SnapstoreConfig:        snapstoreConfig,
			},
			OriginalClusterSize: options.OriginalClusterSize,
			Logger:              logger,
			ZapLogger:           zapLogger,
		},
		Logger: logger,
	}

	return etcdInit
}

// restoreCorruptData attempts to restore a corrupted data directory.
// It returns true only if restoration was successful, and false when
// bootstrapping a new data directory or if restoration failed
func (e *EtcdInitializer) restoreCorruptData() (bool, error) {
	logger := e.Logger
	tempRestoreOptions := *(e.Config.RestoreOptions.DeepCopy())
	dataDir := tempRestoreOptions.Config.RestoreDataDir

	if e.Config.SnapstoreConfig == nil || len(e.Config.SnapstoreConfig.Provider) == 0 {
		logger.Warnf("No snapstore storage provider configured.")
		return e.restoreWithEmptySnapstore()
	}
	store, err := snapstore.GetSnapstore(e.Config.SnapstoreConfig)
	if err != nil {
		err = fmt.Errorf("failed to create snapstore from configured storage provider: %v", err)
		return false, err
	}
	logger.Info("Finding latest set of snapshot to recover from...")
	baseSnap, deltaSnapList, err := miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
	if err != nil {
		logger.Errorf("failed to get latest set of snapshot: %v", err)
		return false, err
	}
	if baseSnap == nil && (deltaSnapList == nil || len(deltaSnapList) == 0) {
		// Snapstore is considered to be the source of truth. Thus, if
		// snapstore exists but is empty, data directory should be cleared.
		logger.Infof("No snapshot found. Will remove the data directory.")
		return e.restoreWithEmptySnapstore()
	}

	tempRestoreOptions.BaseSnapshot = baseSnap
	tempRestoreOptions.DeltaSnapList = deltaSnapList
	tempRestoreOptions.Config.RestoreDataDir = fmt.Sprintf("%s.%s", tempRestoreOptions.Config.RestoreDataDir, "part")

	if err := e.removeDir(tempRestoreOptions.Config.RestoreDataDir); err != nil {
		return false, fmt.Errorf("failed to delete previous temporary data directory: %v", err)
	}

	rs := restorer.NewRestorer(store, logrus.NewEntry(logger))
	m := member.NewMemberControl(e.Config.EtcdConnectionConfig)
	if err := rs.RestoreAndStopEtcd(tempRestoreOptions, m); err != nil {
		err = fmt.Errorf("failed to restore snapshot: %v", err)
		return false, err
	}

	if err := e.removeContents(dataDir); err != nil {
		return false, fmt.Errorf("failed to remove corrupt contents with restored snapshot: %v", err)
	}
	logger.Infoln("Successfully restored the etcd data directory.")
	return true, nil
}

// restoreWithEmptySnapstore removes the data directory as
// part of restoration process for empty snapstore case.
// It returns true if data directory removal is successful,
// and false if directory removal failed or if directory
// never existed (bootstrap case)
func (e *EtcdInitializer) restoreWithEmptySnapstore() (bool, error) {
	dataDir := e.Config.RestoreOptions.Config.RestoreDataDir
	e.Logger.Infof("Removing directory(%s) since snapstore is empty.", dataDir)

	// If data directory doesn't exist, it means we are bootstrapping
	// a new data directory, so no restoration occurs
	if _, err := os.Stat(dataDir); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}

	// If data directory already exists, then we remove it.
	// This is considered an act of restoration because we
	// act on the corrupted data directory by removing it
	if err := e.removeDir(dataDir); err != nil {
		return false, err
	}
	return true, nil
}

func (e *EtcdInitializer) removeContents(dataDir string) error {
	if err := e.removeDir(dataDir); err != nil {
		return err
	}

	if err := os.Rename(filepath.Join(fmt.Sprintf("%s.%s", dataDir, "part")), filepath.Join(dataDir)); err != nil {
		return fmt.Errorf("failed to rename temp restore directory %s to data directory %s with err: %v", filepath.Join(fmt.Sprintf("%s.%s", dataDir, "part")), dataDir, err)
	}
	return nil
}

func (e *EtcdInitializer) removeDir(dirname string) error {
	e.Logger.Infof("Removing directory(%s).", dirname)
	if err := os.RemoveAll(filepath.Join(dirname)); err != nil {
		return fmt.Errorf("failed to remove directory %s with err: %v", dirname, err)
	}
	return nil
}

// restoreInMultiNode
// * Remove the member from the cluster
// * Clean the data-dir of member that needs to be restored.
// * Add a new member as a learner(non-voting member)
func (e *EtcdInitializer) restoreInMultiNode(ctx context.Context) error {
	m := member.NewMemberControl(e.Config.EtcdConnectionConfig)
	if err := retry.OnError(retry.DefaultBackoff, errors.AnyError, func() error {
		return m.RemoveMember(ctx)
	}); err != nil {
		return fmt.Errorf("unable to remove the member %v", err)
	}

	if err := e.removeDir(e.Config.RestoreOptions.Config.RestoreDataDir); err != nil {
		return fmt.Errorf("unable to remove the data-dir %v", err)
	}

	if err := retry.OnError(retry.DefaultBackoff, errors.AnyError, func() error {
		return m.AddMemberAsLearner(ctx)
	}); err != nil {
		return fmt.Errorf("unable to add the member as learner %v", err)
	}
	return nil
}
