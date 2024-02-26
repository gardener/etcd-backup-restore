// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package initializer

import (
	"github.com/gardener/etcd-backup-restore/pkg/initializer/validator"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/sirupsen/logrus"
)

// Config holds etcd related configuration required for initialization
// checks and snapshot restoration in case of corruption.
type Config struct {
	SnapstoreConfig      *brtypes.SnapstoreConfig
	RestoreOptions       *brtypes.RestoreOptions
	EtcdConnectionConfig *brtypes.EtcdConnectionConfig
}

// EtcdInitializer implements Initializer interface to perform validation and
// data restore if required.
type EtcdInitializer struct {
	Validator *validator.DataValidator
	Config    *Config
	Logger    *logrus.Logger
}

// Initializer is the interface for etcd initialization actions.
type Initializer interface {
	Initialize(validator.Mode, int64) error
}
