// Copyright (c) 2021 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
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

	"github.com/gardener/etcd-backup-restore/pkg/snapshot/copier"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// NewCopyCommand creates a cobra command for copy.
func NewCopyCommand(ctx context.Context) *cobra.Command {
	opts := newCopierOptions()
	var command = &cobra.Command{
		Use:   "copy",
		Short: "copy data between buckets",
		Long:  `Copy data between buckets`,
		Run: func(cmd *cobra.Command, args []string) {
			printVersionInfo()
			logger := logrus.NewEntry(logrus.New())
			if err := opts.validate(); err != nil {
				logger.Fatalf("failed to validate the options: %v", err)
			}
			opts.complete()

			sourceStorage, destStorage, err := copier.GetSourceAndDestinationStores(opts.sourceSnapStoreConfig, opts.snapstoreConfig)
			if err != nil {
				logger.Fatalf("Could not get source and destination snapstores: %v", err)
			}

			copier := copier.NewCopier(sourceStorage, destStorage, logger, opts.maxBackups, opts.maxBackupAge)
			if err := copier.Run(); err != nil {
				logger.Fatalf("Copy operation failed: %v", err)
			}

			logger.Info("Shutting down...")
		},
	}
	opts.addFlags(command.Flags())
	return command
}
