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
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/pkg/types"
	"github.com/gardener/etcd-backup-restore/pkg/errors"
	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
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
		Long:  `Server will keep listening for http request to deliver its functionality through http endpoints.`,
		Run: func(cmd *cobra.Command, args []string) {
			printVersionInfo()
			var (
				snapstoreConfig *snapstore.Config
				ssrStopCh       chan struct{}
				ackCh           chan struct{}
			)
			ackCh = make(chan struct{})
			clusterUrlsMap, err := types.NewURLsMap(restoreCluster)
			if err != nil {
				logger.Fatalf("failed creating url map for restore cluster: %v", err)
			}
			peerUrls, err := types.NewURLs(restorePeerURLs)
			if err != nil {
				logger.Fatalf("failed parsing peers urls for restore cluster: %v", err)
			}

			options := &restorer.RestoreOptions{
				RestoreDataDir:         path.Clean(restoreDataDir),
				Name:                   restoreName,
				ClusterURLs:            clusterUrlsMap,
				PeerURLs:               peerUrls,
				ClusterToken:           restoreClusterToken,
				SkipHashCheck:          skipHashCheck,
				MaxFetchers:            restoreMaxFetchers,
				EmbeddedEtcdQuotaBytes: embeddedEtcdQuotaBytes,
			}

			if storageProvider != "" {
				snapstoreConfig = &snapstore.Config{
					Provider:                storageProvider,
					Container:               storageContainer,
					Prefix:                  path.Join(storagePrefix, backupFormatVersion),
					MaxParallelChunkUploads: maxParallelChunkUploads,
					TempDir:                 snapstoreTempDir,
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
				EnableProfiling: enableProfiling,
				ReqCh:           make(chan struct{}),
				AckCh:           make(chan struct{}),
			}
			logger.Info("Registering the http request handlers...")
			handler.RegisterHandler()
			logger.Info("Starting the http server...")
			go handler.Start()
			defer handler.Stop()

			if snapstoreConfig == nil {
				logger.Warnf("No snapstore storage provider configured. Will not start backup schedule.")
				go handleNoSsrRequest(handler)
				handler.Status = http.StatusOK
				<-stopCh
				logger.Infof("Received stop signal. Terminating !!")
				return
			}

			tlsConfig := etcdutil.NewTLSConfig(
				certFile,
				keyFile,
				caFile,
				insecureTransport,
				insecureSkipVerify,
				etcdEndpoints)

			ss, err := snapstore.GetSnapstore(snapstoreConfig)
			if err != nil {
				logger.Fatalf("Failed to create snapstore from configured storage provider: %v", err)
			}
			logger.Infof("Created snapstore from provider: %s", storageProvider)

			snapshotterConfig, err := snapshotter.NewSnapshotterConfig(
				schedule,
				ss,
				maxBackups,
				deltaSnapshotIntervalSeconds,
				deltaSnapshotMemoryLimit,
				time.Duration(etcdConnectionTimeout),
				time.Duration(garbageCollectionPeriodSeconds),
				garbageCollectionPolicy,
				tlsConfig)
			if err != nil {
				logger.Fatalf("failed to create snapstore config: %v", err)
			}
			logger.Infof("Creating snapshotter...")
			ssr := snapshotter.NewSnapshotter(
				logger,
				snapshotterConfig)
			ssrStopCh = make(chan struct{})
			go handleSsrRequest(handler, ssr, ackCh, ssrStopCh, stopCh)
			go handleAckState(handler, ackCh)
			if defragmentationPeriodHours < 1 {
				logger.Infof("Disabling defragmentation since defragmentation period [%d] is less than 1", defragmentationPeriodHours)
			} else {
				go etcdutil.DefragDataPeriodically(stopCh, tlsConfig, time.Duration(defragmentationPeriodHours)*time.Hour, time.Duration(etcdConnectionTimeout)*time.Second, ssr.TriggerFullSnapshot)
			}
			for {
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

				// The decision to either take an initial delta snapshot or
				// or a full snapshot directly is based on whether there has
				// been a previous full snapshot (if not, we assume the etcd
				// to be a fresh etcd) or it has been more than 24 hours since
				// the last full snapshot was taken.
				// If this is not the case, we take a delta snapshot by first
				// collecting all the delta events since the previous snapshot
				// and take a delta snapshot of these (there may be multiple
				// delta snapshots based on the amount of events collected and
				// the delta snapshot memory limit), after which a full snapshot
				// is taken and the regular snapshot schedule comes into effect.

				var startWithFullSnapshot bool

				// TODO: write code to find out if prev full snapshot is older than it is
				// supposed to be, according to the given cron schedule, instead of the
				// hard-coded "24 hours" full snapshot interval
				if ssr.PrevFullSnapshot == nil || time.Now().Sub(ssr.PrevFullSnapshot.CreatedOn).Hours() > 24 {
					startWithFullSnapshot = false
					if err = ssr.TakeFullSnapshotAndResetTimer(); err != nil {
						logger.Errorf("Failed to take first full snapshot: %v", err)
						continue
					}
				} else {
					startWithFullSnapshot = true
					ssrStopped, skipInitialDeltaSnapshot, err := ssr.CollectEventsSincePrevSnapshot(ssrStopCh)
					if err != nil {
						if ssrStopped {
							logger.Infof("Snapshotter stopped.")
							ackCh <- emptyStruct
							handler.Status = http.StatusServiceUnavailable
							return
						}
						if etcdErr, ok := err.(*errors.EtcdError); ok == true {
							logger.Errorf("Failed to take first delta snapshot: snapshotter failed with etcd error: %v", etcdErr)
						} else {
							logger.Errorf("Failed to take first delta snapshot: snapshotter failed with error: %v", err)
						}
						continue
					}
					if !skipInitialDeltaSnapshot {
						if err = ssr.TakeDeltaSnapshot(); err != nil {
							logger.Errorf("Failed to take first delta snapshot: snapshotter failed with error: %v", err)
							continue
						}
					}
				}

				// set server's healthz endpoint status to OK so that
				// etcd is marked as ready to serve traffic
				handler.Status = http.StatusOK

				ssr.SsrStateMutex.Lock()
				ssr.SsrState = snapshotter.SnapshotterActive
				ssr.SsrStateMutex.Unlock()
				gcStopCh := make(chan struct{})
				go ssr.RunGarbageCollector(gcStopCh)
				logger.Infof("Starting snapshotter...")
				if err := ssr.Run(ssrStopCh, startWithFullSnapshot); err != nil {
					if etcdErr, ok := err.(*errors.EtcdError); ok == true {
						logger.Errorf("Snapshotter failed with etcd error: %v", etcdErr)
					} else {
						logger.Errorf("Snapshotter failed with error: %v", err)
					}
				}
				logger.Infof("Snapshotter stopped.")
				ackCh <- emptyStruct
				handler.Status = http.StatusServiceUnavailable
				close(gcStopCh)
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
	serverCmd.Flags().BoolVar(&enableProfiling, "enable-profiling", false, "enable profiling")
}

// ProbeEtcd will make the snapshotter probe for etcd endpoint to be available
// before it starts taking regular snapshots.
func ProbeEtcd(tlsConfig *etcdutil.TLSConfig) error {
	client, err := etcdutil.GetTLSClientForEtcd(tlsConfig)
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

func handleAckState(handler *server.HTTPHandler, ackCh chan struct{}) {
	for {
		<-ackCh
		if atomic.CompareAndSwapUint32(&handler.AckState, server.HandlerAckWaiting, server.HandlerAckDone) {
			handler.AckCh <- emptyStruct
		}
	}
}

// handleNoSsrRequest responds to handlers request with acknowledgment when snapshotter is not running.
func handleNoSsrRequest(handler *server.HTTPHandler) {
	for {
		_, ok := <-handler.ReqCh
		if !ok {
			return
		}
		if atomic.CompareAndSwapUint32(&handler.AckState, server.HandlerAckWaiting, server.HandlerAckDone) {
			handler.AckCh <- emptyStruct
		}
	}
}

// handleSsrRequest responds to handlers request and stop interrupt.
func handleSsrRequest(handler *server.HTTPHandler, ssr *snapshotter.Snapshotter, ackCh, ssrStopCh chan struct{}, stopCh <-chan struct{}) {
	for {
		var ok bool
		select {
		case _, ok = <-handler.ReqCh:
		case _, ok = <-stopCh:
		}

		ssr.SsrStateMutex.Lock()
		if ssr.SsrState == snapshotter.SnapshotterActive {
			ssr.SsrStateMutex.Unlock()
			ssrStopCh <- emptyStruct
		} else {
			ssr.SsrState = snapshotter.SnapshotterInactive
			ssr.SsrStateMutex.Unlock()
			ackCh <- emptyStruct
		}
		if !ok {
			return
		}
	}
}
