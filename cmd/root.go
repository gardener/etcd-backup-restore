// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"

	"github.com/spf13/cobra"
)

// NewBackupRestoreCommand represents the base command when called without any subcommands
func NewBackupRestoreCommand(ctx context.Context) *cobra.Command {
	var RootCmd = &cobra.Command{
		Use:   "etcdbrctl",
		Short: "command line utility for etcd backup restore",
		Long: `The etcdbrctl, command line utility, is built to support etcd's backup and restore
related functionality. Sub-command for this root command will support features
like scheduled snapshot of etcd, etcd data directory validation and restore etcd
from previously taken snapshot.`,
		Run: func(cmd *cobra.Command, args []string) {
			if version {
				printVersionInfo()
			}
		},
	}
	RootCmd.Flags().BoolVarP(&version, "version", "v", false, "print version info")
	RootCmd.AddCommand(NewSnapshotCommand(ctx),
		NewRestoreCommand(ctx),
		NewCompactCommand(ctx),
		NewInitializeCommand(ctx),
		NewServerCommand(ctx),
		NewCopyCommand(ctx))
	return RootCmd
}
