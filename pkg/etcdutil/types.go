// Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
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

package etcdutil

import (
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/wrappers"
)

const (
	defaultEtcdConnectionEndpoint string = "127.0.0.1:2379"

	// DefaultDefragConnectionTimeout for timeout during ETCD defrag call
	DefaultDefragConnectionTimeout time.Duration = 30 * time.Second
)

// EtcdConnectionConfig holds the etcd connection config.
type EtcdConnectionConfig struct {
	// Endpoints are the endpoints from which the backup will be take or defragmentation will be called.
	// This need not be necessary match the entire etcd cluster.
	Endpoints          []string          `json:"endpoints"`
	Username           string            `json:"username,omitempty"`
	Password           string            `json:"password,omitempty"`
	ConnectionTimeout  wrappers.Duration `json:"connectionTimeout,omitempty"`
	InsecureTransport  bool              `json:"insecureTransport,omitempty"`
	InsecureSkipVerify bool              `json:"insecureSkipVerify,omitempty"`
	CertFile           string            `json:"certFile,omitempty"`
	KeyFile            string            `json:"keyFile,omitempty"`
	CaFile             string            `json:"caFile,omitempty"`
}
