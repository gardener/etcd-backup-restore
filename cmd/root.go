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
