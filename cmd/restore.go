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
	"fmt"

	"github.com/coreos/etcd/pkg/types"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/restorer"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// NewRestoreCommand returns the command to restore
func NewRestoreCommand(ctx context.Context) *cobra.Command {
	opts := newRestorerOptions()
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
			if err := opts.validate(); err != nil {
				logger.Fatalf("failed to validate the options: %v", err)
				return
			}

			opts.complete()

			clusterUrlsMap, err := types.NewURLsMap(opts.restorationConfig.InitialCluster)
			if err != nil {
				logger.Fatalf("failed creating url map for restore cluster: %v", err)
			}

			peerUrls, err := types.NewURLs(opts.restorationConfig.InitialAdvertisePeerURLs)
			if err != nil {
				logger.Fatalf("failed parsing peers urls for restore cluster: %v", err)
			}

			store, err := snapstore.GetSnapstore(opts.snapstoreConfig)
			if err != nil {
				logger.Fatalf("failed to create snapstore from configured storage provider: %v", err)
			}

			logger.Info("Finding latest set of snapshot to recover from...")
			baseSnap, deltaSnapList, err := miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(ctx, store)
			if err != nil {
				logger.Fatalf("failed to get latest snapshot: %v", err)
			}
			if baseSnap == nil {
				logger.Infof("No snapshot found. Will do nothing.")
				return
			}

			rs := restorer.NewRestorer(store, logrus.NewEntry(logger))

			options := &restorer.RestoreOptions{
				Config:        opts.restorationConfig,
				BaseSnapshot:  *baseSnap,
				DeltaSnapList: deltaSnapList,
				ClusterURLs:   clusterUrlsMap,
				PeerURLs:      peerUrls,
			}

			if err := rs.Restore(ctx, *options); err != nil {
				logger.Fatalf("Failed to restore snapshot: %v", err)
				return
			}
			logger.Info("Successfully restored the etcd data directory.")
		},
	}

	opts.addFlags(restoreCmd.Flags())
	return restoreCmd
}
