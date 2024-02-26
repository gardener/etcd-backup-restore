// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"

	"github.com/gardener/etcd-backup-restore/pkg/defragmentor"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/snapshotter"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"

	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	cron "github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// NewSnapshotCommand create cobra command for snapshot
func NewSnapshotCommand(ctx context.Context) *cobra.Command {
	opts := newSnapshotterOptions()
	var command = &cobra.Command{
		Use:   "snapshot",
		Short: "takes the snapshot of etcd periodically.",
		Long: `Snapshot utility will backup the etcd at regular interval. It supports
storing snapshots on various cloud storage providers as well as local disk location.`,
		Run: func(cmd *cobra.Command, args []string) {
			printVersionInfo()
			logger := logrus.NewEntry(logrus.New())
			if err := opts.validate(); err != nil {
				logger.Fatalf("failed to validate the options: %v", err)
				return
			}

			opts.complete()

			ss, err := snapstore.GetSnapstore(opts.snapstoreConfig)
			if err != nil {
				logger.Fatalf("Failed to create snapstore from configured storage provider: %v", err)
			}

			ssr, err := snapshotter.NewSnapshotter(logger, opts.snapshotterConfig, ss, opts.etcdConnectionConfig, opts.compressionConfig, brtypes.NewHealthConfig(), opts.snapstoreConfig)
			if err != nil {
				logger.Fatalf("Failed to create snapshotter: %v", err)
			}

			defragSchedule, err := cron.ParseStandard(opts.defragmentationSchedule)
			if err != nil {
				logger.Fatalf("failed to parse defragmentation schedule: %v", err)
				return
			}

			go defragmentor.DefragDataPeriodically(ctx, opts.etcdConnectionConfig, defragSchedule, ssr.TriggerFullSnapshot, logger)

			go ssr.RunGarbageCollector(ctx.Done())
			if err := ssr.Run(ctx.Done(), true); err != nil {
				logger.Fatalf("Snapshotter failed with error: %v", err)
			}
			logger.Info("Shutting down...")
		},
	}
	opts.addFlags(command.Flags())
	return command
}
