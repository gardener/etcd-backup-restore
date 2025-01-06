// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"

	"github.com/gardener/etcd-backup-restore/pkg/initializer"
	"github.com/gardener/etcd-backup-restore/pkg/initializer/validator"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/go-logr/logr"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"go.etcd.io/etcd/pkg/types"
	runtimelog "sigs.k8s.io/controller-runtime/pkg/log"
)

// NewInitializeCommand returns the command to initialize etcd by validating the data
// directory and restoring from cloud store if needed.
func NewInitializeCommand(ctx context.Context) *cobra.Command {
	opts := newInitializerOptions()
	// restoreCmd represents the restore command
	initializeCmd := &cobra.Command{
		Use:   "initialize",
		Short: "initialize an etcd instance.",
		Long:  `Initializes an etcd instance. Data directory is checked for corruption and restored in case of corruption.`,
		Run: func(cmd *cobra.Command, args []string) {
			logger := logrus.New()
			runtimelog.SetLogger(logr.New(runtimelog.NullLogSink{}))
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

			restoreOptions := &brtypes.RestoreOptions{
				Config:      opts.restorerOptions.restorationConfig,
				ClusterURLs: clusterUrlsMap,
				PeerURLs:    peerUrls,
			}

			etcdInitializer, err := initializer.NewInitializer(restoreOptions, opts.restorerOptions.snapstoreConfig, opts.etcdConnectionConfig, logger)
			if err != nil {
				logger.Fatalf("failed to create initializer object: %v", err)
			}
			if err := etcdInitializer.Initialize(mode, opts.validatorOptions.FailBelowRevision); err != nil {
				logger.Fatalf("initializer failed. %v", err)
			}
		},
	}

	opts.addFlags(initializeCmd.Flags())
	return initializeCmd
}
