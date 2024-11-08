// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"errors"
	"os"

	"github.com/gardener/etcd-backup-restore/pkg/compressor"

	"github.com/gardener/etcd-backup-restore/pkg/initializer/validator"
	"github.com/gardener/etcd-backup-restore/pkg/server"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/snapshotter"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/gardener/etcd-backup-restore/pkg/wrappers"

	"github.com/sirupsen/logrus"
	flag "github.com/spf13/pflag"
	"sigs.k8s.io/yaml"
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
		data, err := os.ReadFile(o.ConfigFile)
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
	validatorOptions     *validatorOptions
	restorerOptions      *restorerOptions
	etcdConnectionConfig *brtypes.EtcdConnectionConfig
}

// newInitializerOptions returns the validation config.
func newInitializerOptions() *initializerOptions {
	return &initializerOptions{
		validatorOptions:     newValidatorOptions(),
		restorerOptions:      newRestorerOptions(),
		etcdConnectionConfig: brtypes.NewEtcdConnectionConfig(),
	}
}

// AddFlags adds the flags to flagset.
func (c *initializerOptions) addFlags(fs *flag.FlagSet) {
	c.validatorOptions.addFlags(fs)
	c.restorerOptions.addFlags(fs)
	c.etcdConnectionConfig.AddFlags(fs)
}

// Validate validates the config.
func (c *initializerOptions) validate() error {
	if err := c.validatorOptions.validate(); err != nil {
		return err
	}

	if err := c.etcdConnectionConfig.Validate(); err != nil {
		return err
	}

	return c.restorerOptions.validate()
}

// Complete completes the config.
func (c *initializerOptions) complete() {
	c.restorerOptions.complete()
}

type compactOptions struct {
	*restorerOptions
	compactorConfig *brtypes.CompactorConfig
}

// newCompactOptions returns the validation config.
func newCompactOptions() *compactOptions {
	return &compactOptions{
		restorerOptions: &restorerOptions{
			restorationConfig: brtypes.NewRestorationConfig(),
			snapstoreConfig:   snapstore.NewSnapstoreConfig(),
		},
		compactorConfig: brtypes.NewCompactorConfig(),
	}
}

// AddFlags adds the flags to flagset.
func (c *compactOptions) addFlags(fs *flag.FlagSet) {
	c.restorationConfig.AddFlags(fs)
	c.snapstoreConfig.AddFlags(fs)
	c.compactorConfig.AddFlags(fs)
}

// Validate validates the config.
func (c *compactOptions) validate() error {
	return c.compactorConfig.Validate()
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
	c.snapstoreConfig.Complete()
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
	etcdConnectionConfig     *brtypes.EtcdConnectionConfig
	compressionConfig        *compressor.CompressionConfig
	snapstoreConfig          *brtypes.SnapstoreConfig
	snapshotterConfig        *brtypes.SnapshotterConfig
	defragmentationSchedule  string
	exponentialBackoffConfig *brtypes.ExponentialBackoffConfig
}

// newSnapshotterOptions returns the snapshotter options.
func newSnapshotterOptions() *snapshotterOptions {
	return &snapshotterOptions{
		etcdConnectionConfig:     brtypes.NewEtcdConnectionConfig(),
		snapstoreConfig:          snapstore.NewSnapstoreConfig(),
		snapshotterConfig:        snapshotter.NewSnapshotterConfig(),
		compressionConfig:        compressor.NewCompressorConfig(),
		exponentialBackoffConfig: brtypes.NewExponentialBackOffConfig(),
		defragmentationSchedule:  "0 0 */3 * *",
	}
}

// AddFlags adds the flags to flagset.
func (c *snapshotterOptions) addFlags(fs *flag.FlagSet) {
	c.etcdConnectionConfig.AddFlags(fs)
	c.snapstoreConfig.AddFlags(fs)
	c.snapshotterConfig.AddFlags(fs)
	c.compressionConfig.AddFlags(fs)
	c.exponentialBackoffConfig.AddFlags(fs)

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

	if err := c.exponentialBackoffConfig.Validate(); err != nil {
		return err
	}
	return c.etcdConnectionConfig.Validate()
}

// complete completes the config.
func (c *snapshotterOptions) complete() {
	c.snapstoreConfig.Complete()
}

type copierOptions struct {
	sourceSnapStoreConfig       *brtypes.SnapstoreConfig
	snapstoreConfig             *brtypes.SnapstoreConfig
	maxBackups                  int
	maxBackupAge                int
	maxParallelCopyOperations   int
	waitForFinalSnapshot        bool
	waitForFinalSnapshotTimeout wrappers.Duration
}

func newCopierOptions() *copierOptions {
	sourceSnapStoreConfig := snapstore.NewSnapstoreConfig()
	sourceSnapStoreConfig.IsSource = true
	return &copierOptions{
		sourceSnapStoreConfig: sourceSnapStoreConfig,
		snapstoreConfig:       snapstore.NewSnapstoreConfig(),
	}
}

func (c *copierOptions) addFlags(fs *flag.FlagSet) {
	fs.IntVar(&c.maxBackups, "max-backups-to-copy", -1, "copy the specified number of backups sorted by date from newest to oldest")
	fs.IntVar(&c.maxBackupAge, "max-backup-age", -1, "copy only the backups not older than the specified number of days")
	fs.IntVar(&c.maxParallelCopyOperations, "max-parallel-copy-operations", 10, "maximum number of parallel copy operations")
	fs.BoolVar(&c.waitForFinalSnapshot, "wait-for-final-snapshot", false, "wait for a final full snapshot before copying backups")
	fs.DurationVar(&c.waitForFinalSnapshotTimeout.Duration, "wait-for-final-snapshot-timeout", 0, "timeout for waiting for a final full snapshot")
	c.sourceSnapStoreConfig.AddSourceFlags(fs)
	c.snapstoreConfig.AddFlags(fs)
}

func (c *copierOptions) validate() error {
	if c.maxBackups < -1 {
		return errors.New("parameter max-backups-to-copy must not be less than -1")
	}
	if c.maxBackupAge < -1 {
		return errors.New("parameter max-backup-age must not be less than -1")
	}
	if c.maxParallelCopyOperations <= 0 {
		return errors.New("parameter max-parallel-copy-operations must be greater than 0")
	}
	if c.waitForFinalSnapshotTimeout.Duration < 0 {
		return errors.New("parameter wait-for-final-snapshot-timeout must not be less than 0")
	}
	if err := c.snapstoreConfig.Validate(); err != nil {
		return err
	}
	return c.sourceSnapStoreConfig.Validate()
}

func (c *copierOptions) complete() {
	c.snapstoreConfig.Complete()
	c.sourceSnapStoreConfig.MergeWith(c.snapstoreConfig)
}
