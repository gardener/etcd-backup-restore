// SPDX-FileCopyrightText: Contributors to the Gardener project
//
// SPDX-License-Identifier: Apache-2.0

package types

import (
	"fmt"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/wrappers"

	flag "github.com/spf13/pflag"
)

const (
	defaultEtcdConnectionEndpoint string = "http://127.0.0.1:2379"

	// DefaultEtcdConnectionTimeout defines default timeout duration for etcd client connection.
	DefaultEtcdConnectionTimeout time.Duration = 30 * time.Second
	// DefaultSnapshotTimeout defines default timeout duration for taking FullSnapshot.
	DefaultSnapshotTimeout time.Duration = 15 * time.Minute
	// DefaultEndpointsRefreshEnabled is the default value for the endpoints-refresh feature.
	DefaultEndpointsRefreshEnabled = false
	// DefaultEndpointsRefreshInterval is the default interval for refreshing the ENDPOINTS file.
	DefaultEndpointsRefreshInterval time.Duration = 30 * time.Second
)

// EtcdConnectionConfig holds the etcd connection config.
type EtcdConnectionConfig struct {
	CertFile         string   `json:"certFile,omitempty"`
	Username         string   `json:"username,omitempty"`
	Password         string   `json:"password,omitempty"`
	KeyFile          string   `json:"keyFile,omitempty"`
	CaFile           string   `json:"caFile,omitempty"`
	ServiceEndpoints []string `json:"serviceEndpoints,omitempty"`
	// Endpoints are the endpoints from which the backup will be take or defragmentation will be called.
	// This need not be necessary match the entire etcd cluster.
	Endpoints                []string          `json:"endpoints"`
	ConnectionTimeout        wrappers.Duration `json:"connectionTimeout,omitempty"`
	SnapshotTimeout          wrappers.Duration `json:"snapshotTimeout,omitempty"`
	MaxCallSendMsgSize       int               `json:"maxCallSendMsgSize,omitempty"`
	InsecureTransport        bool              `json:"insecureTransport,omitempty"`
	InsecureSkipVerify       bool              `json:"insecureSkipVerify,omitempty"`
	EndpointsRefreshEnabled  bool              `json:"endpointsRefreshEnabled,omitempty"`
	EndpointsRefreshInterval wrappers.Duration `json:"endpointsRefreshInterval,omitempty"`
}

// NewEtcdConnectionConfig returns etcd connection config.
func NewEtcdConnectionConfig() *EtcdConnectionConfig {
	return &EtcdConnectionConfig{
		Endpoints:                []string{defaultEtcdConnectionEndpoint},
		ConnectionTimeout:        wrappers.Duration{Duration: DefaultEtcdConnectionTimeout},
		SnapshotTimeout:          wrappers.Duration{Duration: DefaultSnapshotTimeout},
		InsecureTransport:        true,
		InsecureSkipVerify:       false,
		EndpointsRefreshEnabled:  DefaultEndpointsRefreshEnabled,
		EndpointsRefreshInterval: wrappers.Duration{Duration: DefaultEndpointsRefreshInterval},
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
	fs.BoolVar(&c.InsecureTransport, "insecure-transport", c.InsecureTransport, "disable transport security for client connections")
	fs.BoolVar(&c.InsecureSkipVerify, "insecure-skip-tls-verify", c.InsecureTransport, "skip server certificate verification")
	fs.StringVar(&c.CertFile, "cert", c.CertFile, "identify secure client using this TLS certificate file")
	fs.StringVar(&c.KeyFile, "key", c.KeyFile, "identify secure client using this TLS key file")
	fs.StringVar(&c.CaFile, "cacert", c.CaFile, "verify certificates of TLS-enabled secure servers using this CA bundle")
	fs.BoolVar(&c.EndpointsRefreshEnabled, "enable-endpoints-refresh", c.EndpointsRefreshEnabled, "enable periodic refresh of etcd endpoints from the ENDPOINTS file")
	fs.DurationVar(&c.EndpointsRefreshInterval.Duration, "endpoints-refresh-interval", c.EndpointsRefreshInterval.Duration, "interval at which the ENDPOINTS file is refreshed from the live etcd member list")
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
	if c.EndpointsRefreshEnabled && c.EndpointsRefreshInterval.Duration <= 0 {
		return fmt.Errorf("endpoints-refresh-interval must be greater than zero when endpoints refresh is enabled")
	}
	return nil
}
