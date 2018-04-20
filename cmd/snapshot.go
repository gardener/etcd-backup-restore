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
	"path"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/snapshot/snapshotter"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	"github.com/spf13/cobra"
)

// NewSnapshotCommand create cobra command for snapshot
func NewSnapshotCommand(stopCh <-chan struct{}) *cobra.Command {
	var command = &cobra.Command{
		Use:   "snapshot",
		Short: "takes the snapshot of etcd periodically.",
		Long: `Snapshot utility will backup the etcd at regular interval. It supports
storing snapshots on various cloud storage providers as well as local disk location.`,
		Run: func(cmd *cobra.Command, args []string) {
			snapstoreConfig := &snapstore.Config{
				Provider:  storageProvider,
				Container: storageContainer,
				Prefix:    path.Join(storagePrefix, backupFormatVersion),
			}
			ss, err := snapstore.GetSnapstore(snapstoreConfig)
			if err != nil {
				logger.Fatalf("Failed to create snapstore from configured storage provider: %v", err)
			}
			ssr, err := snapshotter.NewSnapshotter(
				etcdEndpoints,
				schedule,
				ss,
				logger,
				maxBackups,
				time.Duration(etcdConnectionTimeout))
			if err != nil {
				logger.Fatalf("Failed to create snapshotter: %v", err)
			}
			err = ssr.Run(stopCh)
			if err != nil {
				logger.Fatalf("Snapshotter failed with error: %v", err)
			}
			logger.Info("Shutting down...")
			//TODO: do cleanup work here.
			return
		},
	}
	initializeSnapstoreFlags(command)
	initializeSnapshotterFlags(command)
	return command
}

// initializeSnapshotterFlags adds snapshotter related flags to <cmd>
func initializeSnapshotterFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&etcdEndpoints, "etcd-endpoints", "e", "http://localhost:2379", "comma separated list of etcd endpoints")
	cmd.Flags().StringVarP(&schedule, "schedule", "s", "* */1 * * *", "schedule for snapshots")
	cmd.Flags().IntVarP(&maxBackups, "max-backups", "m", 7, "maximum number of previous backups to keep")
	cmd.Flags().IntVar(&etcdConnectionTimeout, "etcd-connection-timeout", 30, "etcd client connection timeout")
}
