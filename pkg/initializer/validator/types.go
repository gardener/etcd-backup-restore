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

package validator

import (
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	"github.com/sirupsen/logrus"
)

// DataDirStatus represents the status of the etcd data directory.
type DataDirStatus int

const (
	// DataDirectoryValid indicates data directory is valid.
	DataDirectoryValid = iota
	// DataDirectoryNotExist indicates data directory is non-existent.
	DataDirectoryNotExist
	// DataDirectoryInvStruct indicates data directory has invalid structure.
	DataDirectoryInvStruct
	// DataDirectoryCorrupt indicates data directory is corrupt.
	DataDirectoryCorrupt
	// DataDirectoryError indicates unknown error while validation.
	DataDirectoryError
	// RevisionConsistencyError indicates current etcd revision is inconsistent with latest snapshot revision.
	RevisionConsistencyError
)

const (
	snapSuffix = ".snap"
)

// Mode is the Validation mode passed on to the DataValidator
type Mode string

const (
	// Full Mode does complete validation including the data directory contents for corruption.
	Full Mode = "full"
	// Sanity Mode validates only the data revision against the revision in the backup store.
	Sanity Mode = "sanity"
)

// Config store configuration for DataValidator.
type Config struct {
	DataDir         string
	SnapstoreConfig *snapstore.Config
}

// DataValidator contains implements Validator interface to perform data validation.
type DataValidator struct {
	Config *Config
	Logger *logrus.Logger
}

// Validator is the interface for data validation actions.
type Validator interface {
	Validate(Mode) error
}
