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

package cmd

import (
	"context"
	"io/ioutil"

	"github.com/gardener/etcd-backup-restore/pkg/compressor"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/snapshotter"

	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"

	"github.com/gardener/etcd-backup-restore/pkg/snapstore"

	"github.com/gardener/etcd-backup-restore/pkg/initializer/validator"

	"github.com/gardener/etcd-backup-restore/pkg/server"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/ghodss/yaml"
	"github.com/sirupsen/logrus"
	flag "github.com/spf13/pflag"
)

type serverOptions struct {
	ConfigFile string
	Version    bool
	LogLevel   uint32
	Logger     *logrus.Logger
	Config     *server.BackupRestoreComponentConfig
}

// newServerOptions returns a new Options object.
func newServerOptions() *serverOptions {
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)

	return &serverOptions{
		LogLevel: 4,
		Version:  false,
		Config:   server.NewBackupRestoreComponentConfig(),
		Logger:   logger,
	}
}

func (o *serverOptions) validate() error {
	return o.Config.Validate()
}

func (o *serverOptions) addFlags(fs *flag.FlagSet) {
	fs.StringVar(&o.ConfigFile, "config-file", o.ConfigFile, "path to the configuration file")
	fs.Uint32Var(&o.LogLevel, "log-level", o.LogLevel, "verbosity level of logs")
	o.Config.AddFlags(fs)
}

func (o *serverOptions) complete() {
	o.Config.Complete()
	o.Logger.SetLevel(logrus.Level(o.LogLevel))
}

func (o *serverOptions) loadConfigFromFile() error {
	if len(o.ConfigFile) != 0 {
		data, err := ioutil.ReadFile(o.ConfigFile)
		if err != nil {
			return err
		}
		config := server.NewBackupRestoreComponentConfig()
		if err := yaml.Unmarshal(data, config); err != nil {
			return err
		}
		o.Config = config
	}
	// TODO: Overwrite config with flags
	return nil
}

func (o *serverOptions) run(ctx context.Context) error {
	brServer, err := server.NewBackupRestoreServer(o.Logger, o.Config)
	if err != nil {
		return err
	}
	return brServer.Run(ctx)
}

type initializerOptions struct {
	validatorOptions *validatorOptions
	restorerOptions  *restorerOptions
}

// newInitializerOptions returns the validation config.
func newInitializerOptions() *initializerOptions {
	return &initializerOptions{
		validatorOptions: newValidatorOptions(),
		restorerOptions:  newRestorerOptions(),
	}
}

// AddFlags adds the flags to flagset.
func (c *initializerOptions) addFlags(fs *flag.FlagSet) {
	c.validatorOptions.addFlags(fs)
	c.restorerOptions.addFlags(fs)
}

// Validate validates the config.
func (c *initializerOptions) validate() error {
	if err := c.validatorOptions.validate(); err != nil {
		return err
	}

	return c.restorerOptions.validate()
}

// Complete completes the config.
func (c *initializerOptions) complete() {
	c.restorerOptions.complete()
}

type restoreOpts interface {
	getRestorationConfig() *brtypes.RestorationConfig
	getSnapstoreConfig() *brtypes.SnapstoreConfig
	validate() error
	complete()
}

type compactOptions struct {
	restorationConfig   *brtypes.RestorationConfig
	snapstoreConfig     *brtypes.SnapstoreConfig
	needDefragmentation bool
}

// newCompactOptions returns the validation config.
func newCompactOptions() *compactOptions {
	return &compactOptions{
		restorationConfig:   brtypes.NewRestorationConfig(),
		snapstoreConfig:     snapstore.NewSnapstoreConfig(),
		needDefragmentation: true,
	}
}

func (c *compactOptions) getRestorationConfig() *brtypes.RestorationConfig {
	return c.restorationConfig
}

func (c *compactOptions) getSnapstoreConfig() *brtypes.SnapstoreConfig {
	return c.snapstoreConfig
}

// AddFlags adds the flags to flagset.
func (c *compactOptions) addFlags(fs *flag.FlagSet) {
	c.restorationConfig.AddFlags(fs)
	c.snapstoreConfig.AddFlags(fs)
	fs.BoolVar(&c.needDefragmentation, "defragment", c.needDefragmentation, "defragment after compaction")
}

// Validate validates the config.
func (c *compactOptions) validate() error {
	if err := c.snapstoreConfig.Validate(); err != nil {
		return err
	}

	if err := c.snapstoreConfig.Validate(); err != nil {
		return err
	}

	return c.restorationConfig.Validate()
}

// complete completes the config.
func (c *compactOptions) complete() {
	c.snapstoreConfig.Complete()
}

type restorerOptions struct {
	restorationConfig *brtypes.RestorationConfig
	snapstoreConfig   *brtypes.SnapstoreConfig
}

// newRestorerOptions returns the validation config.
func newRestorerOptions() *restorerOptions {
	return &restorerOptions{
		restorationConfig: brtypes.NewRestorationConfig(),
		snapstoreConfig:   snapstore.NewSnapstoreConfig(),
	}
}

func (c *restorerOptions) getRestorationConfig() *brtypes.RestorationConfig {
	return c.restorationConfig
}

func (c *restorerOptions) getSnapstoreConfig() *brtypes.SnapstoreConfig {
	return c.snapstoreConfig
}

// AddFlags adds the flags to flagset.
func (c *restorerOptions) addFlags(fs *flag.FlagSet) {
	c.restorationConfig.AddFlags(fs)
	c.snapstoreConfig.AddFlags(fs)
}

// Validate validates the config.
func (c *restorerOptions) validate() error {
	if err := c.snapstoreConfig.Validate(); err != nil {
		return err
	}

	return c.restorationConfig.Validate()
}

// complete completes the config.
func (c *restorerOptions) complete() {
	//c.snapstoreConfig.Complete()
}

type validatorOptions struct {
	ValidationMode    string `json:"validationMode,omitempty"`
	FailBelowRevision int64  `json:"experimentalFailBelowRevision,omitempty"`
}

// newValidatorOptions returns the validation config.
func newValidatorOptions() *validatorOptions {
	return &validatorOptions{
		ValidationMode: string(validator.Full),
	}
}

// AddFlags adds the flags to flagset.
func (c *validatorOptions) addFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.ValidationMode, "validation-mode", string(c.ValidationMode), "mode to do data initialization[full/sanity]")
	fs.Int64Var(&c.FailBelowRevision, "experimental-fail-below-revision", c.FailBelowRevision, "minimum required etcd revision, below which validation fails")
}

// Validate validates the config.
func (c *validatorOptions) validate() error {
	return nil
}

type snapshotterOptions struct {
	etcdConnectionConfig    *etcdutil.EtcdConnectionConfig
	compressionConfig       *compressor.CompressionConfig
	snapstoreConfig         *brtypes.SnapstoreConfig
	snapshotterConfig       *brtypes.SnapshotterConfig
	defragmentationSchedule string
}

// newSnapshotterOptions returns the snapshotter options.
func newSnapshotterOptions() *snapshotterOptions {
	return &snapshotterOptions{
		etcdConnectionConfig:    etcdutil.NewEtcdConnectionConfig(),
		snapstoreConfig:         snapstore.NewSnapstoreConfig(),
		snapshotterConfig:       snapshotter.NewSnapshotterConfig(),
		compressionConfig:       compressor.NewCompressorConfig(),
		defragmentationSchedule: "0 0 */3 * *",
	}
}

// AddFlags adds the flags to flagset.
func (c *snapshotterOptions) addFlags(fs *flag.FlagSet) {
	c.etcdConnectionConfig.AddFlags(fs)
	c.snapstoreConfig.AddFlags(fs)
	c.snapshotterConfig.AddFlags(fs)
	c.compressionConfig.AddFlags(fs)

	// Miscellaneous
	fs.StringVar(&c.defragmentationSchedule, "defragmentation-schedule", c.defragmentationSchedule, "schedule to defragment etcd data directory")
}

// Validate validates the config.
func (c *snapshotterOptions) validate() error {
	if err := c.snapstoreConfig.Validate(); err != nil {
		return err
	}

	if err := c.snapshotterConfig.Validate(); err != nil {
		return err
	}

	if err := c.compressionConfig.Validate(); err != nil {
		return err
	}

	return c.etcdConnectionConfig.Validate()
}

// complete completes the config.
func (c *snapshotterOptions) complete() {
	c.snapstoreConfig.Complete()
}
