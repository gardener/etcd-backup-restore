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
				snapstoreConfig    *snapstore.Config
				ssrStopCh          chan struct{}
				ackCh              chan struct{}
				ssr                *snapshotter.Snapshotter
				handler            *server.HTTPHandler
				snapshotterEnabled bool
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

			if storageProvider == "" {
				snapshotterEnabled = false
				logger.Warnf("No snapstore storage provider configured. Will not start backup schedule.")
			} else {
				snapshotterEnabled = true
				snapstoreConfig = &snapstore.Config{
					Provider:                storageProvider,
					Container:               storageContainer,
					Prefix:                  path.Join(storagePrefix, backupFormatVersion),
					MaxParallelChunkUploads: maxParallelChunkUploads,
					TempDir:                 snapstoreTempDir,
				}
			}

			etcdInitializer := initializer.NewInitializer(options, snapstoreConfig, logger)

			tlsConfig := etcdutil.NewTLSConfig(
				certFile,
				keyFile,
				caFile,
				insecureTransport,
				insecureSkipVerify,
				etcdEndpoints)

			if snapshotterEnabled {
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
					logger.Fatalf("failed to create snapshotter config: %v", err)
				}

				logger.Infof("Creating snapshotter...")
				ssr = snapshotter.NewSnapshotter(
					logger,
					snapshotterConfig,
				)

				handler = startHTTPServer(etcdInitializer, ssr)
				defer handler.Stop()

				ssrStopCh = make(chan struct{})
				go handleSsrStopRequest(handler, ssr, ackCh, ssrStopCh, stopCh)
				go handleAckState(handler, ackCh)
				startDefragmentationThread(defragmentationPeriodHours, stopCh, tlsConfig, ssr.TriggerFullSnapshot)
				runEtcdProbeLoopWithSnapshotter(tlsConfig, handler, ssr, ssrStopCh, stopCh, ackCh)
			} else {
				// If no storage provider is given, snapshotter will be nil, in which
				// case the status is set to OK as soon as etcd probe is successful
				handler = startHTTPServer(etcdInitializer, nil)
				defer handler.Stop()

				// start defragmentation without trigerring full snapshot
				// after each successful data defragmentation
				startDefragmentationThread(defragmentationPeriodHours, stopCh, tlsConfig, func() error {
					return nil
				})
				runEtcdProbeLoopWithoutSnapshotter(tlsConfig, handler, stopCh, ackCh)
			}
		},
	}

	initializeServerFlags(serverCmd)
	initializeSnapshotterFlags(serverCmd)
	initializeSnapstoreFlags(serverCmd)
	initializeEtcdFlags(serverCmd)
	return serverCmd
}

// startHTTPServer creates and starts the HTTP handler
// with status 503 (Service Unavailable)
func startHTTPServer(etcdInitializer *initializer.EtcdInitializer, ssr *snapshotter.Snapshotter) *server.HTTPHandler {
	// Start http handler with Error state and wait till snapshotter is up
	// and running before setting the status to OK.
	handler := &server.HTTPHandler{
		Port:            port,
		EtcdInitializer: *etcdInitializer,
		Snapshotter:     ssr,
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

	return handler
}

// startDefragmentationThread starts the etcd data defragmentation thread
func startDefragmentationThread(defragPeriod int, stopCh <-chan struct{}, tlsConfig *etcdutil.TLSConfig, triggerFullSnapshotCallback func() error) {
	if defragPeriod < 1 {
		logger.Infof("Disabling defragmentation since defragmentation period [%d] is less than 1", defragPeriod)
		return
	}
	go etcdutil.DefragDataPeriodically(stopCh, tlsConfig, time.Duration(defragPeriod)*time.Hour, time.Duration(etcdConnectionTimeout)*time.Second, triggerFullSnapshotCallback)
}

// runEtcdProbeLoopWithoutSnapshotter runs the etcd probe loop
// for the case where snapshotter is configured correctly
func runEtcdProbeLoopWithSnapshotter(tlsConfig *etcdutil.TLSConfig, handler *server.HTTPHandler, ssr *snapshotter.Snapshotter, ssrStopCh chan struct{}, stopCh <-chan struct{}, ackCh chan struct{}) {
	var (
		err                       error
		initialDeltaSnapshotTaken bool
	)

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

		// TODO: write code to find out if prev full snapshot is older than it is
		// supposed to be, according to the given cron schedule, instead of the
		// hard-coded "24 hours" full snapshot interval
		if ssr.PrevFullSnapshot != nil && time.Since(ssr.PrevFullSnapshot.CreatedOn).Hours() <= 24 {
			ssrStopped, err := ssr.CollectEventsSincePrevSnapshot(ssrStopCh)
			if ssrStopped {
				logger.Info("Snapshotter stopped.")
				ackCh <- emptyStruct
				handler.Status = http.StatusServiceUnavailable
				logger.Info("Shutting down...")
				return
			}
			if err == nil {
				if err := ssr.TakeDeltaSnapshot(); err != nil {
					logger.Warnf("Failed to take first delta snapshot: snapshotter failed with error: %v", err)
					continue
				}
				initialDeltaSnapshotTaken = true
			} else {
				logger.Warnf("Failed to collect events for first delta snapshot: %v", err)
			}
		}
		if !initialDeltaSnapshotTaken {
			if err := ssr.TakeFullSnapshotAndResetTimer(); err != nil {
				logger.Errorf("Failed to take substitute first full snapshot: %v", err)
				continue
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
		if err := ssr.Run(ssrStopCh, initialDeltaSnapshotTaken); err != nil {
			if etcdErr, ok := err.(*errors.EtcdError); ok == true {
				logger.Errorf("Snapshotter failed with etcd error: %v", etcdErr)
			} else {
				logger.Fatalf("Snapshotter failed with error: %v", err)
			}
		}
		logger.Infof("Snapshotter stopped.")
		ackCh <- emptyStruct
		handler.Status = http.StatusServiceUnavailable
		close(gcStopCh)
	}
}

// runEtcdProbeLoopWithoutSnapshotter runs the etcd probe loop
// for the case where snapshotter is not configured
func runEtcdProbeLoopWithoutSnapshotter(tlsConfig *etcdutil.TLSConfig, handler *server.HTTPHandler, stopCh <-chan struct{}, ackCh chan struct{}) {
	var err error
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

		handler.Status = http.StatusOK
		<-stopCh
		handler.Status = http.StatusServiceUnavailable
		logger.Infof("Received stop signal. Terminating !!")
		return
	}
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

// handleSsrStopRequest responds to handlers request and stop interrupt.
func handleSsrStopRequest(handler *server.HTTPHandler, ssr *snapshotter.Snapshotter, ackCh, ssrStopCh chan struct{}, stopCh <-chan struct{}) {
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
