// Copyright © 2018 The Gardener Authors.
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
	"path"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/pkg/types"
	"github.com/gardener/etcd-backup-restore/pkg/errors"
	"github.com/gardener/etcd-backup-restore/pkg/initializer"
	"github.com/gardener/etcd-backup-restore/pkg/server"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/restorer"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/snapshotter"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	"github.com/spf13/cobra"
)

// NewServerCommand create cobra command for snapshot
func NewServerCommand(stopCh <-chan struct{}) *cobra.Command {
	var serverCmd = &cobra.Command{
		Use:   "server",
		Short: "start the http server with backup scheduler.",
		Long:  `Server will keep listening for http request to deliver its functionality through http endpoins.`,
		Run: func(cmd *cobra.Command, args []string) {

			clusterUrlsMap, err := types.NewURLsMap(restoreCluster)
			if err != nil {
				logger.Fatalf("failed creating url map for restore cluster: %v", err)
			}
			peerUrls, err := types.NewURLs(restorePeerURLs)
			if err != nil {
				logger.Fatalf("failed parsing peers urls for restore cluster: %v", err)
			}

			options := &restorer.RestoreOptions{
				RestoreDataDir: restoreDataDir,
				Name:           restoreName,
				ClusterURLs:    clusterUrlsMap,
				PeerURLs:       peerUrls,
				ClusterToken:   restoreClusterToken,
				SkipHashCheck:  skipHashCheck,
			}

			var snapstoreConfig *snapstore.Config
			if storageProvider != "" {
				snapstoreConfig = &snapstore.Config{
					Provider:  storageProvider,
					Container: storageContainer,
					Prefix:    path.Join(storagePrefix, backupFormatVersion),
				}
			}

			etcdInitializer := initializer.NewInitializer(options, snapstoreConfig, logger)
			handler := &server.HTTPHandler{
				Port:            port,
				EtcdInitializer: *etcdInitializer,
				Logger:          logger,
			}
			logger.Info("Regsitering the http request handlers...")
			handler.RegisterHandler()
			logger.Info("Starting the http server...")
			go handler.Start()
			defer handler.Stop()
			if snapstoreConfig == nil {
				logger.Warnf("No snapstore storage provider configured. Will not start backup schedule.")
				<-stopCh
				return
			}
			ss, err := snapstore.GetSnapstore(snapstoreConfig)
			if err != nil {
				logger.Fatalf("Failed to create snapstore from configured storage provider: %v", err)
			}
			logger.Info("Created snapstore from provider: %s", storageProvider)
			ssr, err := snapshotter.NewSnapshotter(
				etcdEndpoints,
				schedule,
				ss,
				logger,
				maxBackups,
				time.Duration(etcdConnectionTimeout))
			if err != nil {
				logger.Fatalf("Failed to create snapshotter: %v", err)
			}
			for {
				for {
					logger.Infof("Probing etcd...")
					select {
					case <-stopCh:
						logger.Info("Shutting down...")
						return
					default:
						err = probeEtcd(etcdEndpoints, time.Duration(etcdConnectionTimeout))
					}
					if err == nil {
						break
					}
				}
				err = ssr.Run(stopCh)
				if err != nil {
					if etcdErr, ok := err.(*errors.EtcdError); ok == true {
						logger.Errorf("Snapshotter failed with error: %v", etcdErr)
					} else {
						logger.Fatalf("Snapshotter failed with error: %v", err)
					}
				}
			}
		},
	}
	initializeServerFlags(serverCmd)
	initializeSnapshotterFlags(serverCmd)
	initializeSnapstoreFlags(serverCmd)
	initializeEtcdFlags(serverCmd)
	return serverCmd
}

func probeEtcd(endPoints string, etcdConnectionTimeout time.Duration) error {
	client, err := clientv3.NewFromURL(endPoints)
	if err != nil {
		logger.Errorf("Failed to create etcd client: %v", err)
		return err
	}

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*etcdConnectionTimeout)
	defer cancel()
	_, err = client.Get(ctx, "foo")
	if err != nil {
		logger.Errorf("Failed to connect to client: %v", err)
		return err
	}
	return nil
}

// initializeServerFlags adds the flags to <cmd>
func initializeServerFlags(serverCmd *cobra.Command) {
	serverCmd.Flags().IntVarP(&port, "server-port", "p", defaultServerPort, "port on which server should listen")
}
