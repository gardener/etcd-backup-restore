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

	"github.com/gardener/etcd-backup-restore/pkg/initializer/validator"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/restorer"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	"github.com/sirupsen/logrus"
)

// Config holds etcd related configuration required for initialization
// checks and snapshot restoration in case of corruption.
type Config struct {
	SnapstoreConfig *snapstore.Config
	RestoreOptions  *restorer.RestoreOptions
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
	Initialize(context.Context, validator.Mode, int64) error
}
