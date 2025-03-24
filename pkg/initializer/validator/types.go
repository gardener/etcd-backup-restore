// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package validator

import (
	"time"

	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

	"github.com/sirupsen/logrus"
	"go.uber.org/zap"
)

// DataDirStatus represents the status of the etcd data directory.
type DataDirStatus int

const (
	// DataDirectoryValid indicates data directory is valid.
	DataDirectoryValid = iota
	// WrongVolumeMounted indicates wrong volume is attached to the ETCD container
	WrongVolumeMounted
	// DataDirectoryNotExist indicates data directory is non-existent.
	DataDirectoryNotExist
	// DataDirectoryInvStruct indicates data directory has invalid structure.
	DataDirectoryInvStruct
	// DataDirectoryCorrupt indicates data directory is corrupt.
	DataDirectoryCorrupt
	// BoltDBCorrupt indicates Bolt database is corrupt.
	BoltDBCorrupt
	// DataDirectoryStatusUnknown indicates validator failed to check the data directory status.
	DataDirectoryStatusUnknown
	// DataDirStatusInvalidInMultiNode indicates validator failed to check the data directory status in multi-node etcd cluster.
	DataDirStatusInvalidInMultiNode
	// RevisionConsistencyError indicates current etcd revision is inconsistent with latest snapshot revision.
	RevisionConsistencyError
	// FailBelowRevisionConsistencyError indicates the current etcd revision is inconsistent with failBelowRevision.
	FailBelowRevisionConsistencyError
	// FailToOpenBoltDBError indicates that backup-restore is unable to open boltDB as it is failed to acquire lock over database.
	FailToOpenBoltDBError
)

const (
	snapSuffix                    = ".snap"
	connectionTimeout             = time.Duration(10 * time.Second)
	embeddedEtcdPingLimitDuration = 60 * time.Second
	timeoutToOpenBoltDB           = 120 * time.Second
	podNamespace                  = "POD_NAMESPACE"
	safeGuard                     = "safe_guard"
)

// Mode is the Validation mode passed on to the DataValidator
type Mode string

const (
	// Full Mode does complete validation including the data directory contents for corruption.
	Full Mode = "full"
	// Sanity Mode does a quick, partial validation of data directory using time-efficient checks.
	Sanity Mode = "sanity"
)

// Config store configuration for DataValidator.
type Config struct {
	DataDir                string
	EmbeddedEtcdQuotaBytes int64
	SnapstoreConfig        *brtypes.SnapstoreConfig
}

// DataValidator contains implements Validator interface to perform data validation.
type DataValidator struct {
	Config              *Config
	OriginalClusterSize int
	Logger              *logrus.Logger
	ZapLogger           *zap.Logger
}

// Validator is the interface for data validation actions.
type Validator interface {
	Validate(Mode, int64) error
}
