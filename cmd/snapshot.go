// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/defragmentor"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/snapshotter"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

	"github.com/go-logr/logr"
	cron "github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	runtimelog "sigs.k8s.io/controller-runtime/pkg/log"
)

// NewSnapshotCommand create cobra command for snapshot
func NewSnapshotCommand(ctx context.Context) *cobra.Command {
	opts := newSnapshotterOptions()
	var command = &cobra.Command{
		Use:   "snapshot",
		Short: "takes the snapshot of etcd periodically.",
		Long: `Snapshot utility will backup the etcd at regular interval. It supports
storing snapshots on various cloud storage providers as well as local disk location.`,
		Run: func(_ *cobra.Command, _ []string) {
			printVersionInfo()
			logger := logrus.NewEntry(logrus.New())
			runtimelog.SetLogger(logr.New(runtimelog.NullLogSink{}))
			if err := opts.validate(); err != nil {
				logger.Fatalf("failed to validate the options: %v", err)
				return
			}

			opts.complete()

			ss, err := snapstore.GetSnapstoreWithCopier(opts.snapstoreConfig)
			if err != nil {
				logger.Fatalf("Failed to create snapstore from configured storage provider: %v", err)
			}

			// Start backup copier if available and sync is enabled
			if ss.Copier != nil && opts.snapstoreConfig.BackupSyncEnabled {
				logger.Info("Starting backup copier for dual endpoint...")
				ss.Copier.Start(ctx)
			}

			ssr, err := snapshotter.NewSnapshotter(logger, opts.snapshotterConfig, ss.Store, opts.etcdConnectionConfig, opts.compressionConfig, brtypes.NewHealthConfig(), opts.snapstoreConfig)
			if err != nil {
				logger.Fatalf("Failed to create snapshotter: %v", err)
			}

			defragSchedule, err := cron.ParseStandard(opts.defragmentationSchedule)
			if err != nil {
				logger.Fatalf("failed to parse defragmentation schedule: %v", err)
				return
			}

			go defragmentor.DefragDataPeriodically(ctx, opts.etcdConnectionConfig, defragSchedule, ssr.TriggerFullSnapshot, logger)

			// Start periodic sync timestamp monitoring if backup copier is available
			if ss.Copier != nil {
				go func() {
					ticker := time.NewTicker(4 * time.Minute)
					defer ticker.Stop()

					for {
						select {
						case <-ctx.Done():
							logger.Info("Context cancelled, stopping sync timestamp monitoring")
							return
						case <-ticker.C:
							lastSyncTime := ss.Copier.GetLastSyncTimestamp()
							if !lastSyncTime.IsZero() {
								logger.Infof("Last backup sync timestamp: %v", lastSyncTime.Format(time.RFC3339))
							} else {
								logger.Info("No backup sync has occurred yet")
							}
						}
					}
				}()
			}

			go ssr.RunGarbageCollector(ctx.Done())
			if err := ssr.Run(ctx.Done(), true); err != nil {
				logger.Fatalf("Snapshotter failed with error: %v", err)
			}

			if ss.Copier != nil && ss.Copier.IsRunning() {
				logger.Info("Stopping backup copier for dual endpoint...")
				ss.Copier.Stop()
			}
			logger.Info("Shutting down...")
		},
	}
	opts.addFlags(command.Flags())
	return command
}
