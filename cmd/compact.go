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
	"strings"

	"github.com/gardener/etcd-backup-restore/pkg/compactor"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"go.etcd.io/etcd/mvcc"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// NewCompactCommand compacts the ETCD instance
func NewCompactCommand(ctx context.Context) *cobra.Command {
	opts := newCompactOptions()
	// compactCmd represents the restore command
	compactCmd := &cobra.Command{
		Use:   "compact",
		Short: "compacts multiple incremental snapshots in etcd backup into a single full snapshot",
		Long:  "Compacts an existing backup stored in snapshot store.",
		Run: func(cmd *cobra.Command, args []string) {
			/* Compact operation
			- Restore from all the latest snapshots (Base + Delta).
			- Compact the newly created embedded ETCD instance.
			- Defragment
			- Save the snapshot
			*/
			logger := logrus.New()
			if err := opts.validate(); err != nil {
				logger.Fatalf("failed to validate the options: %v", err)
				return
			}

			options, store, err := BuildRestoreOptionsAndStore(opts.restorerOptions)
			if err != nil {
				return
			}

			var clientSet client.Client
			if opts.compactorConfig.EnabledLeaseRenewal {
				clientSet, err = miscellaneous.GetKubernetesClientSetOrError()
				if err != nil {
					logger.Fatalf("failed to create clientset, %v", err)
				}
			}

			cp := compactor.NewCompactor(store, logrus.NewEntry(logger), clientSet)
			compactOptions := &brtypes.CompactOptions{
				RestoreOptions:  options,
				CompactorConfig: opts.compactorConfig,
			}

			snapshot, err := cp.Compact(ctx, compactOptions)
			if err != nil {
				if strings.Contains(err.Error(), mvcc.ErrCompacted.Error()) {
					logger.Warnf("Stopping backup compaction: %v", err)
				} else {
					logger.Fatalf("Failed to compact snapshot: %v", err)
				}
				return
			}
			logger.Infof("Compacted snapshot name : %v", snapshot.SnapName)

		},
	}

	opts.addFlags(compactCmd.Flags())
	return compactCmd
}
