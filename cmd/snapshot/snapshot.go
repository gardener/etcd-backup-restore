// Copyright © 2018 The Gardener Authors.
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

package snapshot

import (
	"fmt"
	"os"
	"path"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/snapshot/snapshotter"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

const (
	envStorageContainer = "STORAGE_CONTAINER"
	defaultLocalStore   = "default.etcd.bkp"
	backupFormatVersion = "v1"
)

var logger = logrus.New()

// configuration is struct to hold configuration for utility
type configuration struct {
	schedule              string
	etcdEndpoints         string
	storageProvider       string
	maxBackups            int
	etcdConnectionTimeout int
}

// NewSnapshotCommand create cobra command for snapshot
func NewSnapshotCommand(stopCh <-chan struct{}) *cobra.Command {
	config := &configuration{}
	var command = &cobra.Command{
		Use:   "snapshot",
		Short: "takes the snapshot of etcd periodically.",
		Long: `Snapshot utility will backup the etcd at regular interval. It supports
storing snapshots on various cloud storage providers as well as local disk location.`,
		Run: func(cmd *cobra.Command, args []string) {
			ss, err := getSnapstore(config.storageProvider)
			if err != nil {
				logger.Fatalf("Failed to create snapstore from configured storage provider: %v", err)
			}
			ssr, err := snapshotter.NewSnapshotter(
				config.etcdEndpoints,
				config.schedule,
				ss,
				logger,
				config.maxBackups,
				time.Duration(config.etcdConnectionTimeout))
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
	initializeFlags(config, command)
	return command
}

// initializeFlags adds the flags to <cmd>
func initializeFlags(config *configuration, cmd *cobra.Command) {
	cmd.Flags().StringVarP(&config.etcdEndpoints, "etcd-endpoints", "e", "http://localhost:2379", "comma separated list of etcd endpoints")
	cmd.Flags().StringVar(&config.storageProvider, "storage-provider", snapstore.SnapstoreProviderLocal, "snapshot storage provider")
	cmd.Flags().StringVarP(&config.schedule, "schedule", "s", "* */1 * * *", "schedule for snapshots")
	cmd.Flags().IntVarP(&config.maxBackups, "max-backups", "m", 7, "maximum number of previous backups to keep")
	cmd.Flags().IntVar(&config.etcdConnectionTimeout, "etcd-connection-timeout", 30, "etcd client connection timeout")
}

// getSnapstore returns the snapstore object for give storageProvider with specified container
func getSnapstore(storageProvider string) (snapstore.SnapStore, error) {
	switch storageProvider {
	case snapstore.SnapstoreProviderLocal, "":
		container := os.Getenv(envStorageContainer)
		if container == "" {
			container = defaultLocalStore
		}
		return snapstore.NewLocalSnapStore(path.Join(container, backupFormatVersion))
	case snapstore.SnapstoreProviderS3:
		container := os.Getenv(envStorageContainer)
		if container == "" {
			return nil, fmt.Errorf("storage container name not specified")
		}
		return snapstore.NewS3SnapStore(container, backupFormatVersion)
	default:
		return nil, fmt.Errorf("unsupported storage provider : %s", storageProvider)

	}
}
