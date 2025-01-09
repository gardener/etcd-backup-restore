// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"strings"

	"github.com/gardener/etcd-backup-restore/pkg/compactor"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

	"github.com/go-logr/logr"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"go.etcd.io/etcd/mvcc"
	"sigs.k8s.io/controller-runtime/pkg/client"
	runtimelog "sigs.k8s.io/controller-runtime/pkg/log"
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
			runtimelog.SetLogger(logr.New(runtimelog.NullLogSink{}))
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
