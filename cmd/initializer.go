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
	"github.com/gardener/etcd-backup-restore/pkg/initializer"
	"github.com/gardener/etcd-backup-restore/pkg/initializer/validator"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/restorer"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// NewInitializeCommand returns the command to initialize etcd by validating the data
// directory and restoring from cloud store if needed.
func NewInitializeCommand(ctx context.Context) *cobra.Command {
	opts := newInitializerOptions()
	// restoreCmd represents the restore command
	initializeCmd := &cobra.Command{
		Use:   "initialize",
		Short: "initialize an etcd instance.",
		Long:  fmt.Sprintf(`Initializes an etcd instance. Data directory is checked for corruption and restored in case of corruption.`),
		Run: func(cmd *cobra.Command, args []string) {
			logger := logrus.New()
			if err := opts.validate(); err != nil {
				logger.Fatalf("failed to validate the options: %v", err)
				return
			}

			opts.complete()

			clusterUrlsMap, err := types.NewURLsMap(opts.restorerOptions.restorationConfig.InitialCluster)
			if err != nil {
				logger.Fatalf("failed creating url map for restore cluster: %v", err)
			}

			peerUrls, err := types.NewURLs(opts.restorerOptions.restorationConfig.InitialAdvertisePeerURLs)
			if err != nil {
				logger.Fatalf("failed parsing peers urls for restore cluster: %v", err)
			}

			var mode validator.Mode
			switch validator.Mode(opts.validatorOptions.ValidationMode) {
			case validator.Full:
				mode = validator.Full
			case validator.Sanity:
				mode = validator.Sanity
			default:
				logger.Fatal("validation-mode can only be one of these values [full/sanity]")
			}

			restoreOptions := &restorer.RestoreOptions{
				Config:      opts.restorerOptions.restorationConfig,
				ClusterURLs: clusterUrlsMap,
				PeerURLs:    peerUrls,
			}

			etcdInitializer := initializer.NewInitializer(restoreOptions, opts.restorerOptions.snapstoreConfig, logger)
			if err := etcdInitializer.Initialize(ctx, mode, opts.validatorOptions.FailBelowRevision); err != nil {
				logger.Fatalf("initializer failed. %v", err)
			}
		},
	}

	opts.addFlags(initializeCmd.Flags())
	return initializeCmd
}
