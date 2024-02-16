// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"

	"github.com/gardener/etcd-backup-restore/pkg/snapshot/restorer"
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
		Long:  "Restores an etcd member data directory from existing backup stored in snapshot store.",
		Run: func(cmd *cobra.Command, args []string) {
			/* Restore operation
			- Find the latest snapshot.
			- Restore etcd data diretory from full snapshot.
			*/
			logger := logrus.New()

			options, store, err := BuildRestoreOptionsAndStore(opts)
			if err != nil {
				return
			}

			rs, err := restorer.NewRestorer(store, logrus.NewEntry(logger))
			if err != nil {
				logger.Fatalf("failed to create restorer object: %v", err)
			}
			if err := rs.RestoreAndStopEtcd(*options, nil); err != nil {
				logger.Fatalf("Failed to restore snapshot: %v", err)
				return
			}
			logger.Info("Successfully restored the etcd data directory.")
		},
	}

	opts.addFlags(restoreCmd.Flags())
	return restoreCmd
}
