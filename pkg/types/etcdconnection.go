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

package types

import (
	"fmt"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/wrappers"
	flag "github.com/spf13/pflag"
)

const (
	defaultEtcdConnectionEndpoint string = "127.0.0.1:2379"

	// DefaultEtcdConnectionTimeout defines default timeout duration for etcd client connection.
	DefaultEtcdConnectionTimeout time.Duration = 30 * time.Second
	// DefaultDefragConnectionTimeout defines default timeout duration for ETCD defrag call.
	DefaultDefragConnectionTimeout time.Duration = 8 * time.Minute
	// DefaultSnapshotTimeout defines default timeout duration for taking FullSnapshot.
	DefaultSnapshotTimeout time.Duration = 15 * time.Minute

	// DefragRetryPeriod is used as the duration after which a defragmentation is retried.
	DefragRetryPeriod time.Duration = 1 * time.Minute
)

// EtcdConnectionConfig holds the etcd connection config.
type EtcdConnectionConfig struct {
	// Endpoints are the endpoints from which the backup will be take or defragmentation will be called.
	// This need not be necessary match the entire etcd cluster.
	Endpoints          []string          `json:"endpoints"`
	ServiceEndpoints   []string          `json:"serviceEndpoints,omitempty"`
	Username           string            `json:"username,omitempty"`
	Password           string            `json:"password,omitempty"`
	ConnectionTimeout  wrappers.Duration `json:"connectionTimeout,omitempty"`
	SnapshotTimeout    wrappers.Duration `json:"snapshotTimeout,omitempty"`
	DefragTimeout      wrappers.Duration `json:"defragTimeout,omitempty"`
	InsecureTransport  bool              `json:"insecureTransport,omitempty"`
	InsecureSkipVerify bool              `json:"insecureSkipVerify,omitempty"`
	CertFile           string            `json:"certFile,omitempty"`
	KeyFile            string            `json:"keyFile,omitempty"`
	CaFile             string            `json:"caFile,omitempty"`
	MaxCallSendMsgSize int               `json:"maxCallSendMsgSize,omitempty"`
}

// NewEtcdConnectionConfig returns etcd connection config.
func NewEtcdConnectionConfig() *EtcdConnectionConfig {
	return &EtcdConnectionConfig{
		Endpoints:          []string{defaultEtcdConnectionEndpoint},
		ConnectionTimeout:  wrappers.Duration{Duration: DefaultEtcdConnectionTimeout},
		SnapshotTimeout:    wrappers.Duration{Duration: DefaultSnapshotTimeout},
		DefragTimeout:      wrappers.Duration{Duration: DefaultDefragConnectionTimeout},
		InsecureTransport:  true,
		InsecureSkipVerify: false,
	}
}

// AddFlags adds the flags to flagset.
func (c *EtcdConnectionConfig) AddFlags(fs *flag.FlagSet) {
	fs.StringSliceVarP(&c.Endpoints, "endpoints", "e", c.Endpoints, "comma separated list of etcd endpoints")
	fs.StringSliceVar(&c.ServiceEndpoints, "service-endpoints", c.ServiceEndpoints, "comma separated list of etcd endpoints that are used for etcd-backup-restore to connect to etcd through a (Kubernetes) service")
	fs.StringVar(&c.Username, "etcd-username", c.Username, "etcd server username, if one is required")
	fs.StringVar(&c.Password, "etcd-password", c.Password, "etcd server password, if one is required")
	fs.DurationVar(&c.ConnectionTimeout.Duration, "etcd-connection-timeout", c.ConnectionTimeout.Duration, "etcd client connection timeout")
	fs.DurationVar(&c.SnapshotTimeout.Duration, "etcd-snapshot-timeout", c.SnapshotTimeout.Duration, "timeout duration for taking etcd snapshots")
	fs.DurationVar(&c.DefragTimeout.Duration, "etcd-defrag-timeout", c.DefragTimeout.Duration, "timeout duration for etcd defrag call")
	fs.BoolVar(&c.InsecureTransport, "insecure-transport", c.InsecureTransport, "disable transport security for client connections")
	fs.BoolVar(&c.InsecureSkipVerify, "insecure-skip-tls-verify", c.InsecureTransport, "skip server certificate verification")
	fs.StringVar(&c.CertFile, "cert", c.CertFile, "identify secure client using this TLS certificate file")
	fs.StringVar(&c.KeyFile, "key", c.KeyFile, "identify secure client using this TLS key file")
	fs.StringVar(&c.CaFile, "cacert", c.CaFile, "verify certificates of TLS-enabled secure servers using this CA bundle")
}

// Validate validates the config.
func (c *EtcdConnectionConfig) Validate() error {
	if c.ConnectionTimeout.Duration <= 0 {
		return fmt.Errorf("connection timeout should be greater than zero")
	}
	if c.SnapshotTimeout.Duration <= 0 {
		return fmt.Errorf("snapshot timeout should be greater than zero")
	}
	if c.SnapshotTimeout.Duration < c.ConnectionTimeout.Duration {
		return fmt.Errorf("snapshot timeout should be greater than or equal to connection timeout")
	}
	if c.DefragTimeout.Duration <= 0 {
		return fmt.Errorf("etcd defrag timeout should be greater than zero")
	}
	return nil
}
