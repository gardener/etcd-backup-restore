// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"

	"github.com/gardener/etcd-backup-restore/pkg/snapshot/copier"

	"github.com/go-logr/logr"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	runtimelog "sigs.k8s.io/controller-runtime/pkg/log"
)

// NewCopyCommand creates a cobra command for copy.
func NewCopyCommand(ctx context.Context) *cobra.Command {
	opts := newCopierOptions()
	var command = &cobra.Command{
		Use:   "copy",
		Short: "copy data between buckets",
		Long:  `Copy data between buckets`,
		Run: func(cmd *cobra.Command, args []string) {
			printVersionInfo()
			logger := logrus.NewEntry(logrus.New())
			runtimelog.SetLogger(logr.New(runtimelog.NullLogSink{}))
			if err := opts.validate(); err != nil {
				logger.Fatalf("failed to validate the options: %v", err)
			}
			opts.complete()

			sourceStorage, destStorage, err := copier.GetSourceAndDestinationStores(opts.sourceSnapStoreConfig, opts.snapstoreConfig)
			if err != nil {
				logger.Fatalf("Could not get source and destination snapstores: %v", err)
			}

			copier := copier.NewCopier(
				logger,
				sourceStorage,
				destStorage,
				opts.maxBackups,
				opts.maxBackupAge,
				opts.maxParallelCopyOperations,
				opts.waitForFinalSnapshot,
				opts.waitForFinalSnapshotTimeout.Duration,
			)
			if err := copier.Run(ctx); err != nil {
				logger.Fatalf("Copy operation failed: %v", err)
			}

			logger.Info("Shutting down...")
		},
	}
	opts.addFlags(command.Flags())
	return command
}
