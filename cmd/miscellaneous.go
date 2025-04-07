// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"fmt"
	"runtime"

	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	ver "github.com/gardener/etcd-backup-restore/pkg/version"

	"go.etcd.io/etcd/pkg/types"
)

func printVersionInfo() {
	logger.Infof("etcd-backup-restore Version: %s", ver.Version)
	logger.Infof("Git SHA: %s", ver.GitSHA)
	logger.Infof("Go Version: %s", runtime.Version())
	logger.Infof("Go OS/Arch: %s/%s", runtime.GOOS, runtime.GOARCH)
}

// BuildRestoreOptionsAndStore forms the RestoreOptions and Store object
func BuildRestoreOptionsAndStore(opts *restorerOptions) (*brtypes.RestoreOptions, brtypes.SnapStore, error) {
	if err := opts.validate(); err != nil {
		logger.Fatalf("failed to validate the options: %v", err)
		return nil, nil, err
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
		logger.Fatalf("failed to create restore snapstore from configured storage provider: %v", err)
	}

	logger.Info("Finding latest set of snapshot to recover from...")
	baseSnap, deltaSnapList, err := miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
	if err != nil {
		logger.Fatalf("failed to get latest snapshot: %v", err)
	}

	if baseSnap == nil {
		logger.Infof("No base snapshot found. Will do nothing.")
		return nil, nil, fmt.Errorf("no base snapshot found")
	}

	return &brtypes.RestoreOptions{
		Config:        opts.restorationConfig,
		BaseSnapshot:  baseSnap,
		DeltaSnapList: deltaSnapList,
		ClusterURLs:   clusterUrlsMap,
		PeerURLs:      peerUrls,
	}, store, nil
}
