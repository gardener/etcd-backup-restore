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

package cmd

import (
	"github.com/sirupsen/logrus"
)

const (
	backupFormatVersion             = "v1"
	defaultServerPort               = 8080
	defaultName                     = "default"
	defaultInitialAdvertisePeerURLs = "http://localhost:2380"
)

var (
	logger  = logrus.New()
	version bool
	//snapshotter flags
	fullSnapshotSchedule           string
	etcdEndpoints                  []string
	etcdUsername                   string
	etcdPassword                   string
	deltaSnapshotIntervalSeconds   int
	deltaSnapshotMemoryLimit       int
	maxBackups                     int
	etcdConnectionTimeout          int
	garbageCollectionPeriodSeconds int
	garbageCollectionPolicy        string
	insecureTransport              bool
	insecureSkipVerify             bool
	certFile                       string
	keyFile                        string
	caFile                         string
	defragmentationSchedule        string

	//server flags
	port            int
	enableProfiling bool

	//restore flags
	restoreCluster         string
	restoreClusterToken    string
	restoreDataDir         string
	restorePeerURLs        []string
	restoreName            string
	skipHashCheck          bool
	restoreMaxFetchers     int
	embeddedEtcdQuotaBytes int64

	//snapstore flags
	storageProvider         string
	storageContainer        string
	storagePrefix           string
	maxParallelChunkUploads int
	snapstoreTempDir        string

	//initializer flags
	validationMode    string
	failBelowRevision int64
)

var emptyStruct struct{}
