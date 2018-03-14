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

package server

import (
	"context"
	"fmt"
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
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

const (
	backupFormatVersion             = "v1"
	defaultServerPort               = 8080
	defaultName                     = "default"
	defaultInitialAdvertisePeerURLs = "http://localhost:2380"
)

var logger = logrus.New()

// configuration is struct to hold configuration for utility
type configuration struct {
	schedule              string
	etcdEndpoints         string
	storageProvider       string
	maxBackups            int
	etcdConnectionTimeout int
}

var (
	port int

	//restore flags
	restoreCluster      string
	restoreClusterToken string
	restoreDataDir      string
	restorePeerURLs     []string
	restoreName         string
	skipHashCheck       bool
	storageProvider     string
	storePrefix         string
)

// NewServerCommand create cobra command for snapshot
func NewServerCommand(stopCh <-chan struct{}) *cobra.Command {
	config := &configuration{}
	var command = &cobra.Command{
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

			etcdInitializer := initializer.NewInitializer(options, storageProvider, storePrefix, logger)
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

			ss, err := snapstore.GetSnapstore(storageProvider, path.Join(storePrefix, backupFormatVersion))
			if err != nil {
				logger.Fatalf("Failed to create snapstore from configured storage provider: %v", err)
			}
			logger.Info("Created snapstore from provider: %s", storageProvider)
			ssr, err := snapshotter.NewSnapshotter(
				config.etcdEndpoints,
				config.schedule,
				ss,
				logger,
				config.maxBackups,
				time.Duration(config.etcdConnectionTimeout))
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
						err = probeEtcd(config.etcdEndpoints, time.Duration(config.etcdConnectionTimeout))
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
	initializeFlags(config, command)
	return command
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

// initializeFlags adds the flags to <cmd>
func initializeFlags(config *configuration, cmd *cobra.Command) {
	cmd.Flags().IntVarP(&port, "server-port", "p", defaultServerPort, "port on which server should listen")
	cmd.Flags().StringVarP(&config.etcdEndpoints, "etcd-endpoints", "e", "http://localhost:2379", "comma separated list of etcd endpoints")
	cmd.Flags().StringVarP(&config.schedule, "schedule", "s", "* */1 * * *", "schedule for snapshots")
	cmd.Flags().IntVarP(&config.maxBackups, "max-backups", "m", 7, "maximum number of previous backups to keep")
	cmd.Flags().IntVar(&config.etcdConnectionTimeout, "etcd-connection-timeout", 30, "etcd client connection timeout")
	cmd.Flags().StringVar(&storageProvider, "storage-provider", snapstore.SnapstoreProviderLocal, "snapshot storage provider")
	cmd.Flags().StringVar(&storePrefix, "store-prefix", "", "prefix or directory under which snapstore is created")
	cmd.Flags().StringVarP(&restoreDataDir, "data-dir", "d", fmt.Sprintf("%s.etcd", defaultName), "path to the data directory")
	cmd.Flags().StringVar(&restoreCluster, "initial-cluster", initialClusterFromName(defaultName), "initial cluster configuration for restore bootstrap")
	cmd.Flags().StringVar(&restoreClusterToken, "initial-cluster-token", "etcd-cluster", "initial cluster token for the etcd cluster during restore bootstrap")
	cmd.Flags().StringArrayVar(&restorePeerURLs, "initial-advertise-peer-urls", []string{defaultInitialAdvertisePeerURLs}, "list of this member's peer URLs to advertise to the rest of the cluster")
	cmd.Flags().StringVar(&restoreName, "name", defaultName, "human-readable name for this member")
	cmd.Flags().BoolVar(&skipHashCheck, "skip-hash-check", false, "ignore snapshot integrity hash value (required if copied from data directory)")
}

func initialClusterFromName(name string) string {
	n := name
	if name == "" {
		n = defaultName
	}
	return fmt.Sprintf("%s=http://localhost:2380", n)
}
