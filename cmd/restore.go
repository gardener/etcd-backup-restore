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
	"fmt"
	"path"

	"github.com/coreos/etcd/pkg/types"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/restorer"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
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
			clusterUrlsMap, err := types.NewURLsMap(restoreCluster)
			if err != nil {
				logger.Fatalf("failed creating url map for restore cluster: %v", err)
			}

			peerUrls, err := types.NewURLs(restorePeerURLs)
			if err != nil {
				logger.Fatalf("failed parsing peers urls for restore cluster: %v", err)
			}
			snapstoreConfig := &snapstore.Config{
				Provider:  storageProvider,
				Container: storageContainer,
				Prefix:    path.Join(storagePrefix, backupFormatVersion),
			}
			store, err := snapstore.GetSnapstore(snapstoreConfig)
			if err != nil {
				logger.Fatalf("failed to create snapstore from configured storage provider: %v", err)
			}
			logger.Info("Finding latest set of snapshot to recover from...")
			baseSnap, deltaSnapList, err := miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
			if err != nil {
				logger.Fatalf("failed to get latest snapshot: %v", err)
			}
			if baseSnap == nil {
				logger.Infof("No snapshot found. Will do nothing.")
				return
			}

			rs := restorer.NewRestorer(store, logger)

			options := &restorer.RestoreOptions{
				RestoreDataDir: restoreDataDir,
				Name:           restoreName,
				BaseSnapshot:   *baseSnap,
				DeltaSnapList:  deltaSnapList,
				ClusterURLs:    clusterUrlsMap,
				PeerURLs:       peerUrls,
				ClusterToken:   restoreClusterToken,
				SkipHashCheck:  skipHashCheck,
			}

			err = rs.Restore(*options)
			if err != nil {
				logger.Fatalf("Failed to restore snapshot: %v", err)
				return
			}
			logger.Info("Successfully restored the etcd data directory.")
		},
	}

	initializeSnapstoreFlags(restoreCmd)
	initializeEtcdFlags(restoreCmd)
	return restoreCmd
}

// initializeEtcdFlags adds the etcd related flags to <cmd>
func initializeEtcdFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&restoreDataDir, "data-dir", "d", fmt.Sprintf("%s.etcd", defaultName), "path to the data directory")
	cmd.Flags().StringVar(&restoreCluster, "initial-cluster", initialClusterFromName(defaultName), "initial cluster configuration for restore bootstrap")
	cmd.Flags().StringVar(&restoreClusterToken, "initial-cluster-token", "etcd-cluster", "initial cluster token for the etcd cluster during restore bootstrap")
	cmd.Flags().StringArrayVar(&restorePeerURLs, "initial-advertise-peer-urls", []string{defaultInitialAdvertisePeerURLs}, "list of this member's peer URLs to advertise to the rest of the cluster")
	cmd.Flags().StringVar(&restoreName, "name", defaultName, "human-readable name for this member")
	cmd.Flags().BoolVar(&skipHashCheck, "skip-hash-check", false, "ignore snapshot integrity hash value (required if copied from data directory)")
}

func initialClusterFromName(name string) string {
	n := name
	if name == "" {
		n = defaultName
	}
	return fmt.Sprintf("%s=http://localhost:2380", n)
}
