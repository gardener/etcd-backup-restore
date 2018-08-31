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
	"fmt"
	"net/http"
	"path"
	"time"

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
			// Start http handler with Error state and wait till snapshotter is up
			// and running before setting the state to OK.

			handler := &server.HTTPHandler{
				Port:            port,
				EtcdInitializer: *etcdInitializer,
				Logger:          logger,
				Status:          http.StatusServiceUnavailable,
				StopCh:          make(chan struct{}),
			}
			logger.Info("Regsitering the http request handlers...")
			handler.RegisterHandler()
			logger.Info("Starting the http server...")
			go handler.Start()
			defer handler.Stop()

			ssrStopCh := make(chan struct{})
			go func() {
				for {
					var s struct{}
					select {
					case <-handler.StopCh:
					case <-stopCh:
					}
					ssrStopCh <- s
				}
			}()

			if snapstoreConfig == nil {
				logger.Warnf("No snapstore storage provider configured. Will not start backup schedule.")
				handler.Status = http.StatusOK
				<-stopCh
				return
			}

			for {
				ss, err := snapstore.GetSnapstore(snapstoreConfig)
				if err != nil {
					logger.Fatalf("Failed to create snapstore from configured storage provider: %v", err)
				}
				logger.Infof("Created snapstore from provider: %s", storageProvider)

				tlsConfig := snapshotter.NewTLSConfig(
					certFile,
					keyFile,
					caFile,
					insecureTransport,
					insecureSkipVerify,
					etcdEndpoints)
				ssr, err := snapshotter.NewSnapshotter(
					schedule,
					ss,
					logger,
					maxBackups,
					deltaSnapshotIntervalSeconds,
					time.Duration(etcdConnectionTimeout),
					time.Duration(garbageCollectionPeriodSeconds),
					garbageCollectionPolicy,
					tlsConfig)
				if err != nil {
					logger.Fatalf("Failed to create snapshotter from configured storage provider: %v", err)
				}

				logger.Infof("Probing etcd...")
				select {
				case <-stopCh:
					logger.Info("Shutting down...")
					return
				default:
					err = ProbeEtcd(tlsConfig)
				}
				if err != nil {
					logger.Errorf("Failed to probe etcd: %v", err)
					handler.Status = http.StatusServiceUnavailable
					continue
				}

				// Try to take snapshot before setting
				logger.Infof("Taking initial snapshot at time: %s", time.Now().Local())
				if err := ssr.TakeFullSnapshot(); err != nil {
					if etcdErr, ok := err.(*errors.EtcdError); ok == true {
						logger.Errorf("Snapshotter failed with etcd error: %v", etcdErr)
					} else {
						logger.Fatalf("Snapshotter failed with error: %v", err)
					}
					handler.Status = http.StatusServiceUnavailable
					continue
				} else {
					handler.Status = http.StatusOK
				}

				gcStopCh := make(chan bool)

				go ssr.GarbageCollector(gcStopCh)

				if err := ssr.Run(true, ssrStopCh); err != nil {
					handler.Status = http.StatusServiceUnavailable
					if etcdErr, ok := err.(*errors.EtcdError); ok == true {
						logger.Errorf("Snapshotter failed with etcd error: %v", etcdErr)
					} else {
						logger.Fatalf("Snapshotter failed with error: %v", err)
					}
				} else {
					handler.Status = http.StatusOK
				}
				gcStopCh <- true
			}
		},
	}

	initializeServerFlags(serverCmd)
	initializeSnapshotterFlags(serverCmd)
	initializeSnapstoreFlags(serverCmd)
	initializeEtcdFlags(serverCmd)
	return serverCmd
}

// initializeServerFlags adds the flags to <cmd>
func initializeServerFlags(serverCmd *cobra.Command) {
	serverCmd.Flags().IntVarP(&port, "server-port", "p", defaultServerPort, "port on which server should listen")
}

// ProbeEtcd will make the snapshotter probe for etcd endpoint to be available
// before it starts taking regular snapshots.
func ProbeEtcd(tlsConfig *snapshotter.TLSConfig) error {
	client, err := snapshotter.GetTLSClientForEtcd(tlsConfig)
	if err != nil {
		return &errors.EtcdError{
			Message: fmt.Sprintf("failed to create etcd client: %v", err),
		}
	}

	ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(etcdConnectionTimeout)*time.Second)
	defer cancel()
	if _, err := client.Get(ctx, "foo"); err != nil {
		logger.Errorf("Failed to connect to client: %v", err)
		return err
	}
	return nil
}
