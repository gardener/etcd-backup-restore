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

package initializer

import (
	"fmt"
	"os"

	"github.com/coreos/etcd/pkg/types"

	"path"

	"github.com/gardener/etcd-backup-restore/pkg/snapshot/restorer"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

const (
	envStorageContainer             = "STORAGE_CONTAINER"
	defaultLocalStore               = "default.etcd.bkp"
	defaultName                     = "default"
	backupFormatVersion             = "v1"
	defaultInitialAdvertisePeerURLs = "http://localhost:2380"
)

var (
	//restore flags
	restoreCluster      string
	restoreClusterToken string
	restoreDataDir      string
	restorePeerURLs     []string
	restoreName         string
	skipHashCheck       bool
	storageProvider     string
)

// NewRestoreCommand returns the command to restore
func NewRestoreCommand(stopCh <-chan struct{}) *cobra.Command {

	// restoreCmd represents the restore command
	restoreCmd := &cobra.Command{
		Use:   "restore",
		Short: "restores an etcd member data directory from snapshots",
		Long:  fmt.Sprintf(`Restores an etcd member data directory from existing backup stored in snapshot store.`),
		Run: func(cmd *cobra.Command, args []string) {
			/* Restore operation
			- Find the latest snapshot.
			- Restore etcd data diretory from full snapshot.
			*/
			logger := logrus.New()
			if restoreName != defaultName && restoreCluster == initialClusterFromName("<name>") {
				restoreCluster = initialClusterFromName(restoreName)
			}
			clusterUrlsMap, err := types.NewURLsMap(restoreCluster)
			if err != nil {
				logger.Errorf("failed creating url map for restore cluster: %v", err)
				return
			}

			peerUrls, err := types.NewURLs(restorePeerURLs)
			if err != nil {
				logger.Errorf("failed parsing peers urls for restore cluster: %v", err)
				return
			}
			store, err := getSnapstore(storageProvider)
			if err != nil {
				logger.Errorf("failed to create snapstore from configured storage provider: %v", err)
				return
			}
			logger.Infoln("Finding latest snapshot...")
			snap, err := getLatestSnapshot(store)
			if err != nil {
				logger.Errorf("failed to get latest snapshot: %v", err)
				return
			}
			if snap == nil {
				logger.Infof("No snapshot found. Will do nothing.")
				return
			}

			rs := restorer.NewRestorer(store, logger)

			options := &restorer.RestoreOptions{
				RestoreDataDir: restoreDataDir,
				Name:           restoreName,
				Snapshot:       *snap,
				ClusterURLs:    clusterUrlsMap,
				PeerURLs:       peerUrls,
				ClusterToken:   restoreClusterToken,
				SkipHashCheck:  skipHashCheck,
			}

			logger.Infof("Restoring from latest snapshot: %s...", snap.SnapPath)
			err = rs.Restore(*options)
			if err != nil {
				logger.Errorf("Failed to restore snapshot: %v", err)
				return
			}
			logger.Infoln("Successfully restored the etcd data directory.")
		},
	}
	initializeFlags(restoreCmd)
	return restoreCmd
}

func initializeFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&storageProvider, "storage-provider", snapstore.SnapstoreProviderLocal, "snapshot storage provider")
	cmd.Flags().StringVarP(&restoreDataDir, "data-dir", "d", fmt.Sprintf("%s.etcd", defaultName), "path to the data directory")
	cmd.Flags().StringVar(&restoreCluster, "initial-cluster", initialClusterFromName("<name>"), "initial cluster configuration for restore bootstrap")
	cmd.Flags().StringVar(&restoreClusterToken, "initial-cluster-token", "etcd-cluster", "initial cluster token for the etcd cluster during restore bootstrap")
	cmd.Flags().StringArrayVar(&restorePeerURLs, "initial-advertise-peer-urls", []string{defaultInitialAdvertisePeerURLs}, "list of this member's peer URLs to advertise to the rest of the cluster")
	cmd.Flags().StringVar(&restoreName, "name", defaultName, "human-readable name for this member")
	cmd.Flags().BoolVar(&skipHashCheck, "skip-hash-check", false, "ignore snapshot integrity hash value (required if copied from data directory)")
	//cmd.Flags().BoolVar(&fromLatest, "from-latest", false, "Restore from latest snapshot in snapstore")
}

func initialClusterFromName(name string) string {
	n := name
	if name == "" {
		n = defaultName
	}
	return fmt.Sprintf("%s=http://localhost:2380", n)
}

// getLatestSnapshot returns the latest snapshot in snapstore
func getLatestSnapshot(store snapstore.SnapStore) (*snapstore.Snapshot, error) {
	snapList, err := store.List()
	if err != nil {
		return nil, err
	}
	if snapList.Len() == 0 {
		return nil, nil
	}
	return snapList[snapList.Len()-1], nil
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
