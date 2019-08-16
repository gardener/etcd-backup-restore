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
	"os"
	"path"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/pkg/types"
	"github.com/gardener/etcd-backup-restore/pkg/errors"
	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	"github.com/gardener/etcd-backup-restore/pkg/initializer"
	"github.com/gardener/etcd-backup-restore/pkg/metrics"
	"github.com/gardener/etcd-backup-restore/pkg/server"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/restorer"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/snapshotter"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	"github.com/prometheus/client_golang/prometheus"
	cron "github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// NewServerCommand create cobra command for snapshot
func NewServerCommand(ctx context.Context) *cobra.Command {
	var serverCmd = &cobra.Command{
		Use:   "server",
		Short: "start the http server with backup scheduler.",
		Long:  `Server will keep listening for http request to deliver its functionality through http endpoints.`,
		Run: func(cmd *cobra.Command, args []string) {
			// TODO: Refactor the code to move it to coordination logic to server package.
			printVersionInfo()

			// TODO: move this to validate config
			clusterUrlsMap, err := types.NewURLsMap(restoreCluster)
			if err != nil {
				logger.Fatalf("failed creating url map for restore cluster: %v", err)
			}
			peerUrls, err := types.NewURLs(restorePeerURLs)
			if err != nil {
				logger.Fatalf("failed parsing peers urls for restore cluster: %v", err)
			}

			defragSchedule, err := cron.ParseStandard(defragmentationSchedule)
			if err != nil {
				logger.Fatalf("failed to parse defragmentation schedule: %v", err)
			}

			// TODO: move this to build/complete config
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

			tlsConfig := etcdutil.NewTLSConfig(
				certFile,
				keyFile,
				caFile,
				insecureTransport,
				insecureSkipVerify,
				etcdEndpoints,
				etcdUsername,
				etcdPassword)

			if storageProvider == "" {
				logger.Warnf("No snapstore storage provider configured. Will not start backup schedule.")
				runServerWithoutSnapshotter(ctx, tlsConfig, options, defragSchedule)
				return
			}
			runServerWithSnapshotter(ctx, tlsConfig, options, defragSchedule)
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
func startHTTPServer(initializer initializer.Initializer, ssr *snapshotter.Snapshotter) (*server.HTTPHandler, error) {
	enableTLS := serverTLSCertFile != "" && serverTLSKeyFile != ""
	if enableTLS {
		// Check for existence of server cert and key files before proceeding
		if _, err := os.Stat(serverTLSCertFile); err != nil {
			logger.Errorf("TLS enabled but server TLS cert file is invalid. Will not start HTTPS server: %v", err)
			return nil, err
		}
		if _, err := os.Stat(serverTLSKeyFile); err != nil {
			logger.Errorf("TLS enabled but server TLS key file is invalid. Will not start HTTPS server: %v", err)
			return nil, err
		}
	}

	// Start http handler with Error state and wait till snapshotter is up
	// and running before setting the status to OK.
	handler := &server.HTTPHandler{
		Port:              port,
		Initializer:       initializer,
		Snapshotter:       ssr,
		Logger:            logger,
		StopCh:            make(chan struct{}),
		EnableProfiling:   enableProfiling,
		ReqCh:             make(chan struct{}),
		AckCh:             make(chan struct{}),
		EnableTLS:         enableTLS,
		ServerTLSCertFile: serverTLSCertFile,
		ServerTLSKeyFile:  serverTLSKeyFile,
	}
	handler.SetStatus(http.StatusServiceUnavailable)
	logger.Info("Registering the http request handlers...")
	handler.RegisterHandler()
	logger.Info("Starting the http server...")
	go handler.Start()

	return handler, nil
}

// runServerWithoutSnapshotter runs the etcd-backup-restore
// for the case where snapshotter is configured correctly
func runServerWithSnapshotter(ctx context.Context, tlsConfig *etcdutil.TLSConfig, restoreOpts *restorer.RestoreOptions, defragSchedule cron.Schedule) {
	ackCh := make(chan struct{})
	snapstoreConfig := &snapstore.Config{
		Provider:                storageProvider,
		Container:               storageContainer,
		Prefix:                  path.Join(storagePrefix, backupFormatVersion),
		MaxParallelChunkUploads: maxParallelChunkUploads,
		TempDir:                 snapstoreTempDir,
	}

	etcdInitializer := initializer.NewInitializer(restoreOpts, snapstoreConfig, logger)

	ss, err := snapstore.GetSnapstore(snapstoreConfig)
	if err != nil {
		logger.Fatalf("Failed to create snapstore from configured storage provider: %v", err)
	}
	logger.Infof("Created snapstore from provider: %s", storageProvider)

	snapshotterConfig, err := snapshotter.NewSnapshotterConfig(
		fullSnapshotSchedule,
		ss,
		maxBackups,
		deltaSnapshotMemoryLimit,
		deltaSnapshotInterval,
		etcdConnectionTimeout,
		garbageCollectionPeriod,
		garbageCollectionPolicy,
		tlsConfig)
	if err != nil {
		logger.Fatalf("failed to create snapshotter config: %v", err)
	}

	logger.Infof("Creating snapshotter...")
	ssr := snapshotter.NewSnapshotter(
		logrus.NewEntry(logger),
		snapshotterConfig,
	)

	handler, err := startHTTPServer(etcdInitializer, ssr)
	if err != nil {
		logger.Fatalf("Failed to start HTTP server: %v", err)
	}
	defer handler.Stop()

	ssrStopCh := make(chan struct{})
	go handleSsrStopRequest(ctx, handler, ssr, ackCh, ssrStopCh)
	go handleAckState(handler, ackCh)

	go etcdutil.DefragDataPeriodically(ctx, tlsConfig, defragSchedule, etcdConnectionTimeout, ssr.TriggerFullSnapshot, logrus.NewEntry(logger))

	runEtcdProbeLoopWithSnapshotter(ctx, tlsConfig, handler, ssr, ssrStopCh, ackCh)
}

// runEtcdProbeLoopWithoutSnapshotter runs the etcd probe loop
// for the case where snapshotter is configured correctly
func runEtcdProbeLoopWithSnapshotter(ctx context.Context, tlsConfig *etcdutil.TLSConfig, handler *server.HTTPHandler, ssr *snapshotter.Snapshotter, ssrStopCh chan struct{}, ackCh chan struct{}) {
	var (
		err                       error
		initialDeltaSnapshotTaken bool
	)

	for {
		logger.Infof("Probing etcd...")
		select {
		case <-ctx.Done():
			logger.Info("Shutting down...")
			return
		default:
			err = ProbeEtcd(tlsConfig)
		}
		if err != nil {
			logger.Errorf("Failed to probe etcd: %v", err)
			handler.SetStatus(http.StatusServiceUnavailable)
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
				handler.SetStatus(http.StatusServiceUnavailable)
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
				logger.Warnf("Failed to collect events for first delta snapshot(s): %v", err)
			}
		}
		if !initialDeltaSnapshotTaken {
			// need to take a full snapshot here
			metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: snapstore.SnapshotKindDelta}).Set(0)
			metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: snapstore.SnapshotKindFull}).Set(1)
			if err := ssr.TakeFullSnapshotAndResetTimer(); err != nil {
				logger.Errorf("Failed to take substitute first full snapshot: %v", err)
				continue
			}
		}

		// set server's healthz endpoint status to OK so that
		// etcd is marked as ready to serve traffic
		handler.SetStatus(http.StatusOK)

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
		handler.SetStatus(http.StatusServiceUnavailable)
		close(gcStopCh)
	}
}

// runServerWithoutSnapshotter runs the etcd-backup-restore
// for the case where snapshotter is not configured
func runServerWithoutSnapshotter(ctx context.Context, tlsConfig *etcdutil.TLSConfig, restoreOpts *restorer.RestoreOptions, defragSchedule cron.Schedule) {
	etcdInitializer := initializer.NewInitializer(restoreOpts, nil, logger)

	// If no storage provider is given, snapshotter will be nil, in which
	// case the status is set to OK as soon as etcd probe is successful
	handler, err := startHTTPServer(etcdInitializer, nil)
	if err != nil {
		logger.Fatalf("Failed to start HTTP server: %v", err)
	}
	defer handler.Stop()

	// start defragmentation without trigerring full snapshot
	// after each successful data defragmentation
	go etcdutil.DefragDataPeriodically(ctx, tlsConfig, defragSchedule, etcdConnectionTimeout, nil, logrus.NewEntry(logger))

	runEtcdProbeLoopWithoutSnapshotter(ctx, tlsConfig, handler)
}

// runEtcdProbeLoopWithoutSnapshotter runs the etcd probe loop
// for the case where snapshotter is not configured
func runEtcdProbeLoopWithoutSnapshotter(ctx context.Context, tlsConfig *etcdutil.TLSConfig, handler *server.HTTPHandler) {
	var err error
	for {
		logger.Infof("Probing etcd...")
		select {
		case <-ctx.Done():
			logger.Info("Shutting down...")
			return
		default:
			err = ProbeEtcd(tlsConfig)
		}
		if err != nil {
			logger.Errorf("Failed to probe etcd: %v", err)
			handler.SetStatus(http.StatusServiceUnavailable)
			continue
		}

		handler.SetStatus(http.StatusOK)
		<-ctx.Done()
		handler.SetStatus(http.StatusServiceUnavailable)
		logger.Infof("Received stop signal. Terminating !!")
		return
	}
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

	ctx, cancel := context.WithTimeout(context.TODO(), etcdConnectionTimeout)
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
func handleSsrStopRequest(ctx context.Context, handler *server.HTTPHandler, ssr *snapshotter.Snapshotter, ackCh, ssrStopCh chan struct{}) {
	for {
		var ok bool
		select {
		case _, ok = <-handler.ReqCh:
		case _, ok = <-ctx.Done():
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

// initializeServerFlags adds the flags to <cmd>
func initializeServerFlags(serverCmd *cobra.Command) {
	serverCmd.Flags().IntVarP(&port, "server-port", "p", defaultServerPort, "port on which server should listen")
	serverCmd.Flags().BoolVar(&enableProfiling, "enable-profiling", false, "enable profiling")
	serverCmd.Flags().StringVar(&serverTLSCertFile, "server-cert", "", "TLS certificate file for backup-restore server")
	serverCmd.Flags().StringVar(&serverTLSKeyFile, "server-key", "", "TLS key file for backup-restore server")
}
