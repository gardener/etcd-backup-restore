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
func BuildRestoreOptionsAndStore(opts restoreOpts) (*brtypes.RestoreOptions, snapstore.SnapStore, error) {
	if err := opts.validate(); err != nil {
		logger.Fatalf("failed to validate the options: %v", err)
		return nil, nil, err
	}

	opts.complete()

	clusterUrlsMap, err := types.NewURLsMap(opts.getRestorationConfig().InitialCluster)
	if err != nil {
		logger.Fatalf("failed creating url map for restore cluster: %v", err)
	}

	peerUrls, err := types.NewURLs(opts.getRestorationConfig().InitialAdvertisePeerURLs)
	if err != nil {
		logger.Fatalf("failed parsing peers urls for restore cluster: %v", err)
	}

	store, err := snapstore.GetSnapstore(opts.getSnapstoreConfig())
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
		return nil, nil, fmt.Errorf("No base snapshot found")
	}

	return &brtypes.RestoreOptions{
		Config:        opts.getRestorationConfig(),
		BaseSnapshot:  baseSnap,
		DeltaSnapList: deltaSnapList,
		ClusterURLs:   clusterUrlsMap,
		PeerURLs:      peerUrls,
	}, store, nil
}
