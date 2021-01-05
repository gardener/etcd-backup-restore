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

package server

import (
	"fmt"
	"os"

	"github.com/gardener/etcd-backup-restore/pkg/compressor"
	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

	"github.com/gardener/etcd-backup-restore/pkg/snapshot/snapshotter"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	"github.com/robfig/cron/v3"
	flag "github.com/spf13/pflag"
)

// NewBackupRestoreComponentConfig returns the backup-restore componenet config.
func NewBackupRestoreComponentConfig() *BackupRestoreComponentConfig {
	return &BackupRestoreComponentConfig{
		EtcdConnectionConfig:    etcdutil.NewEtcdConnectionConfig(),
		ServerConfig:            NewHTTPServerConfig(),
		SnapshotterConfig:       snapshotter.NewSnapshotterConfig(),
		SnapstoreConfig:         snapstore.NewSnapstoreConfig(),
		CompressionConfig:       compressor.NewCompressorConfig(),
		RestorationConfig:       brtypes.NewRestorationConfig(),
		DefragmentationSchedule: defaultDefragmentationSchedule,
	}
}

// AddFlags adds the flags to flagset.
func (c *BackupRestoreComponentConfig) AddFlags(fs *flag.FlagSet) {
	c.EtcdConnectionConfig.AddFlags(fs)
	c.ServerConfig.AddFlags(fs)
	c.SnapshotterConfig.AddFlags(fs)
	c.SnapstoreConfig.AddFlags(fs)
	c.RestorationConfig.AddFlags(fs)
	c.CompressionConfig.AddFlags(fs)

	// Miscellaneous
	fs.StringVar(&c.DefragmentationSchedule, "defragmentation-schedule", c.DefragmentationSchedule, "schedule to defragment etcd data directory")
}

// Validate validates the config.
func (c *BackupRestoreComponentConfig) Validate() error {
	if err := c.EtcdConnectionConfig.Validate(); err != nil {
		return err
	}
	if err := c.ServerConfig.Validate(); err != nil {
		return err
	}
	if err := c.SnapshotterConfig.Validate(); err != nil {
		return err
	}
	if err := c.SnapstoreConfig.Validate(); err != nil {
		return err
	}
	if err := c.RestorationConfig.Validate(); err != nil {
		return err
	}
	if err := c.CompressionConfig.Validate(); err != nil {
		return err
	}
	if _, err := cron.ParseStandard(c.DefragmentationSchedule); err != nil {
		return err
	}
	return nil
}

// Complete completes the config.
func (c *BackupRestoreComponentConfig) Complete() {
	c.SnapstoreConfig.Complete()
}

// HTTPServerConfig holds the server config.
type HTTPServerConfig struct {
	Port            uint   `json:"port,omitempty"`
	EnableProfiling bool   `json:"enableProfiling,omitempty"`
	TLSCertFile     string `json:"server-cert,omitempty"`
	TLSKeyFile      string `json:"server-key,omitempty"`
}

// NewHTTPServerConfig returns the config for http server
func NewHTTPServerConfig() *HTTPServerConfig {
	return &HTTPServerConfig{
		Port:            defaultServerPort,
		EnableProfiling: false,
	}
}

// AddFlags adds the flags to flagset.
func (c *HTTPServerConfig) AddFlags(fs *flag.FlagSet) {
	fs.UintVarP(&c.Port, "server-port", "p", c.Port, "port on which server should listen")
	fs.BoolVar(&c.EnableProfiling, "enable-profiling", c.EnableProfiling, "enable profiling")
	fs.StringVar(&c.TLSCertFile, "server-cert", "", "TLS certificate file for backup-restore server")
	fs.StringVar(&c.TLSKeyFile, "server-key", "", "TLS key file for backup-restore server")
}

// Validate validates the config.E
func (c *HTTPServerConfig) Validate() error {
	enableTLS := c.TLSCertFile != "" && c.TLSKeyFile != ""
	if enableTLS {
		// Check for existence of server cert and key files before proceeding
		if _, err := os.Stat(c.TLSCertFile); err != nil {
			return fmt.Errorf("TLS enabled but server TLS cert file is invalid. Will not start HTTPS server: %v", err)
		}
		if _, err := os.Stat(c.TLSKeyFile); err != nil {
			return fmt.Errorf("TLS enabled but server TLS key file is invalid. Will not start HTTPS server: %v", err)
		}
	}
	return nil
}
