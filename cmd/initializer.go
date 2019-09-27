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
	"path"

	"github.com/coreos/etcd/pkg/types"
	"github.com/gardener/etcd-backup-restore/pkg/initializer"
	"github.com/gardener/etcd-backup-restore/pkg/initializer/validator"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/restorer"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// NewInitializeCommand returns the command to initialize etcd by validating the data
// directory and restoring from cloud store if needed.
func NewInitializeCommand(ctx context.Context) *cobra.Command {

	// restoreCmd represents the restore command
	initializeCmd := &cobra.Command{
		Use:   "initialize",
		Short: "initialize an etcd instance.",
		Long:  fmt.Sprintf(`Initializes an etcd instance. Data directory is checked for corruption and restored in case of corruption.`),
		Run: func(cmd *cobra.Command, args []string) {
			var mode validator.Mode
			logger := logrus.New()

			clusterUrlsMap, err := types.NewURLsMap(restoreCluster)
			if err != nil {
				logger.Fatalf("failed creating url map for restore cluster: %v", err)
			}

			peerUrls, err := types.NewURLs(restorePeerURLs)
			if err != nil {
				logger.Fatalf("failed parsing peers urls for restore cluster: %v", err)
			}

			switch validator.Mode(validationMode) {
			case validator.Full:
				mode = validator.Full
			case validator.Sanity:
				mode = validator.Sanity
			default:
				logger.Fatal("validation-mode can only be one of these values [full/sanity]")
			}

			options := &restorer.RestoreOptions{
				RestoreDataDir:         path.Clean(restoreDataDir),
				Name:                   restoreName,
				ClusterURLs:            clusterUrlsMap,
				PeerURLs:               peerUrls,
				ClusterToken:           restoreClusterToken,
				SkipHashCheck:          skipHashCheck,
				MaxFetchers:            restoreMaxFetchers,
				EmbeddedEtcdQuotaBytes: embeddedEtcdQuotaBytes,
			}

			var snapstoreConfig *snapstore.Config
			if storageProvider != "" {
				snapstoreConfig = &snapstore.Config{
					Provider:                storageProvider,
					Container:               storageContainer,
					Prefix:                  path.Join(storagePrefix, backupFormatVersion),
					MaxParallelChunkUploads: maxParallelChunkUploads,
					TempDir:                 snapstoreTempDir,
				}
			}
			etcdInitializer := initializer.NewInitializer(options, snapstoreConfig, logger)
			err = etcdInitializer.Initialize(mode, failBelowRevision)
			if err != nil {
				logger.Fatalf("initializer failed. %v", err)
			}
		},
	}
	initializeEtcdFlags(initializeCmd)
	initializeSnapstoreFlags(initializeCmd)
	initializeValidatorFlags(initializeCmd)
	initializeCmd.Flags().Int64Var(&failBelowRevision, "experimental-fail-below-revision", 0, "minimum required etcd revision, below which validation fails")
	return initializeCmd
}

func initializeValidatorFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&validationMode, "validation-mode", string(validator.Full), "mode to do data initialization[full/sanity]")
}
