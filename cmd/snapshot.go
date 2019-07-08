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

	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
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
			printVersionInfo()
			snapstoreConfig := &snapstore.Config{
				Provider:                storageProvider,
				Container:               storageContainer,
				Prefix:                  path.Join(storagePrefix, backupFormatVersion),
				MaxParallelChunkUploads: maxParallelChunkUploads,
				TempDir:                 snapstoreTempDir,
			}
			ss, err := snapstore.GetSnapstore(snapstoreConfig)
			if err != nil {
				logger.Fatalf("Failed to create snapstore from configured storage provider: %v", err)
			}

			tlsConfig := etcdutil.NewTLSConfig(
				certFile,
				keyFile,
				caFile,
				insecureTransport,
				insecureSkipVerify,
				etcdEndpoints,
				etcdUsername,
				etcdPassword)
			snapshotterConfig, err := snapshotter.NewSnapshotterConfig(
				schedule,
				ss,
				maxBackups,
				deltaSnapshotIntervalSeconds,
				deltaSnapshotMemoryLimit,
				time.Duration(etcdConnectionTimeout),
				time.Duration(garbageCollectionPeriodSeconds),
				garbageCollectionPolicy,
				tlsConfig)
			if err != nil {
				logger.Fatalf("failed to create snapstore config: %v", err)
			}
			ssr := snapshotter.NewSnapshotter(
				logger,
				snapshotterConfig)

			if defragmentationPeriodHours < 1 {
				logger.Infof("Disabling defragmentation since defragmentation period [%d] is less than 1", defragmentationPeriodHours)
			} else {
				go etcdutil.DefragDataPeriodically(stopCh, tlsConfig, time.Duration(defragmentationPeriodHours)*time.Hour, time.Duration(etcdConnectionTimeout)*time.Second, ssr.TriggerFullSnapshot)
			}

			gcStopCh := make(chan struct{})
			go ssr.RunGarbageCollector(gcStopCh)
			if err := ssr.Run(stopCh, true); err != nil {
				logger.Fatalf("Snapshotter failed with error: %v", err)
			}
			close(gcStopCh)
			logger.Info("Shutting down...")
			return
		},
	}
	initializeSnapstoreFlags(command)
	initializeSnapshotterFlags(command)
	return command
}

// initializeSnapshotterFlags adds snapshotter related flags to <cmd>
func initializeSnapshotterFlags(cmd *cobra.Command) {
	cmd.Flags().StringSliceVarP(&etcdEndpoints, "endpoints", "e", []string{"127.0.0.1:2379"}, "comma separated list of etcd endpoints")
	cmd.Flags().StringVarP(&schedule, "schedule", "s", "* */1 * * *", "schedule for snapshots")
	cmd.Flags().IntVarP(&deltaSnapshotIntervalSeconds, "delta-snapshot-period-seconds", "i", snapshotter.DefaultDeltaSnapshotIntervalSeconds, "Period in seconds after which delta snapshot will be persisted. If this value is set to be lesser than 1, delta snapshotting will be disabled.")
	cmd.Flags().IntVar(&deltaSnapshotMemoryLimit, "delta-snapshot-memory-limit", snapshotter.DefaultDeltaSnapMemoryLimit, "memory limit after which delta snapshots will be taken")
	cmd.Flags().IntVarP(&maxBackups, "max-backups", "m", snapshotter.DefaultMaxBackups, "maximum number of previous backups to keep")
	cmd.Flags().IntVar(&etcdConnectionTimeout, "etcd-connection-timeout", 30, "etcd client connection timeout")
	cmd.Flags().IntVar(&garbageCollectionPeriodSeconds, "garbage-collection-period-seconds", 60, "Period in seconds for garbage collecting old backups")
	cmd.Flags().StringVar(&garbageCollectionPolicy, "garbage-collection-policy", snapshotter.GarbageCollectionPolicyExponential, "Policy for garbage collecting old backups")
	cmd.Flags().BoolVar(&insecureTransport, "insecure-transport", true, "disable transport security for client connections")
	cmd.Flags().BoolVar(&insecureSkipVerify, "insecure-skip-tls-verify", false, "skip server certificate verification")
	cmd.Flags().StringVar(&certFile, "cert", "", "identify secure client using this TLS certificate file")
	cmd.Flags().StringVar(&keyFile, "key", "", "identify secure client using this TLS key file")
	cmd.Flags().StringVar(&caFile, "cacert", "", "verify certificates of TLS-enabled secure servers using this CA bundle")
	cmd.Flags().StringVar(&etcdUsername, "etcd-username", "", "etcd server username, if one is required")
	cmd.Flags().StringVar(&etcdPassword, "etcd-password", "", "etcd server password, if one is required")
	cmd.Flags().IntVar(&defragmentationPeriodHours, "defragmentation-period-hours", 72, "period after which we should defragment etcd data directory")
}
