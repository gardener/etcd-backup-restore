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
	"context"

	"github.com/gardener/etcd-backup-restore/pkg/defragmentor"

	"github.com/gardener/etcd-backup-restore/pkg/snapshot/snapshotter"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
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

			ssr, err := snapshotter.NewSnapshotter(ctx, logger, opts.snapshotterConfig, ss, opts.etcdConnectionConfig)
			if err != nil {
				logger.Fatalf("Failed to create snapshotter: %v", err)
			}

			defragSchedule, err := cron.ParseStandard(opts.defragmentationSchedule)
			if err != nil {
				logger.Fatalf("failed to parse defragmentation schedule: %v", err)
				return
			}

			go defragmentor.DefragDataPeriodically(ctx, opts.etcdConnectionConfig, defragSchedule, ssr.TriggerFullSnapshot, logger)

			go ssr.RunGarbageCollector(ctx)
			if err := ssr.Run(ctx, true); err != nil {
				logger.Fatalf("Snapshotter failed with error: %v", err)
			}
			logger.Info("Shutting down...")
			return
		},
	}
	opts.addFlags(command.Flags())
	return command
}
