// Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
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
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/errors"
	"github.com/gardener/etcd-backup-restore/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/coreos/etcd/pkg/types"
	"github.com/gardener/etcd-backup-restore/pkg/defragmentor"
	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	"github.com/gardener/etcd-backup-restore/pkg/initializer"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/restorer"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/snapshotter"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	cron "github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
)

// BackupRestoreServer holds the details for backup-restore server.
type BackupRestoreServer struct {
	config                  *BackupRestoreComponentConfig
	logger                  *logrus.Entry
	defragmentationSchedule cron.Schedule
}

// NewBackupRestoreServer return new backup restore server.
func NewBackupRestoreServer(logger *logrus.Logger, config *BackupRestoreComponentConfig) (*BackupRestoreServer, error) {
	parsedDefragSchedule, err := cron.ParseStandard(config.DefragmentationSchedule)
	if err != nil {
		// Ideally this case should not occur, since this check is done at the config validaitions.
		return nil, err
	}
	return &BackupRestoreServer{
		logger:                  logger.WithField("actor", "backup-restore-server"),
		config:                  config,
		defragmentationSchedule: parsedDefragSchedule,
	}, nil
}

// Run starts the backup restore server.
func (b *BackupRestoreServer) Run(ctx context.Context) error {
	clusterURLsMap, err := types.NewURLsMap(b.config.RestorationConfig.InitialCluster)
	if err != nil {
		// Ideally this case should not occur, since this check is done at the config validaitions.
		b.logger.Fatalf("failed creating url map for restore cluster: %v", err)
	}

	peerURLs, err := types.NewURLs(b.config.RestorationConfig.InitialAdvertisePeerURLs)
	if err != nil {
		// Ideally this case should not occur, since this check is done at the config validaitions.
		b.logger.Fatalf("failed creating url map for restore cluster: %v", err)
	}

	options := &restorer.RestoreOptions{
		Config:      b.config.RestorationConfig,
		ClusterURLs: clusterURLsMap,
		PeerURLs:    peerURLs,
	}

	if b.config.SnapstoreConfig == nil || len(b.config.SnapstoreConfig.Provider) == 0 {
		b.logger.Warnf("No snapstore storage provider configured. Will not start backup schedule.")
		b.runServerWithoutSnapshotter(ctx, options)
		return nil
	}
	return b.runServerWithSnapshotter(ctx, options)
}

// startHTTPServer creates and starts the HTTP handler
// with status 503 (Service Unavailable)
func (b *BackupRestoreServer) startHTTPServer(initializer initializer.Initializer, ssr *snapshotter.Snapshotter) *HTTPHandler {
	// Start http handler with Error state and wait till snapshotter is up
	// and running before setting the status to OK.
	handler := &HTTPHandler{
		Port:              b.config.ServerConfig.Port,
		Initializer:       initializer,
		Snapshotter:       ssr,
		Logger:            b.logger,
		StopCh:            make(chan struct{}),
		EnableProfiling:   b.config.ServerConfig.EnableProfiling,
		ReqCh:             make(chan struct{}),
		AckCh:             make(chan struct{}),
		EnableTLS:         (b.config.ServerConfig.TLSCertFile != "" && b.config.ServerConfig.TLSKeyFile != ""),
		ServerTLSCertFile: b.config.ServerConfig.TLSCertFile,
		ServerTLSKeyFile:  b.config.ServerConfig.TLSKeyFile,
	}
	handler.SetStatus(http.StatusServiceUnavailable)
	b.logger.Info("Registering the http request handlers...")
	handler.RegisterHandler()
	b.logger.Info("Starting the http server...")
	go handler.Start()

	return handler
}

// runServerWithoutSnapshotter runs the etcd-backup-restore
// for the case where snapshotter is not configured
func (b *BackupRestoreServer) runServerWithoutSnapshotter(ctx context.Context, restoreOpts *restorer.RestoreOptions) {
	etcdInitializer := initializer.NewInitializer(restoreOpts, nil, b.logger.Logger)

	// If no storage provider is given, snapshotter will be nil, in which
	// case the status is set to OK as soon as etcd probe is successful
	handler := b.startHTTPServer(etcdInitializer, nil)
	defer handler.Stop()

	// start defragmentation without trigerring full snapshot
	// after each successful data defragmentation
	go defragmentor.DefragDataPeriodically(ctx, b.config.EtcdConnectionConfig, b.defragmentationSchedule, nil, b.logger)

	b.runEtcdProbeLoopWithoutSnapshotter(ctx, handler)
}

// runServerWithoutSnapshotter runs the etcd-backup-restore
// for the case where snapshotter is configured correctly
func (b *BackupRestoreServer) runServerWithSnapshotter(ctx context.Context, restoreOpts *restorer.RestoreOptions) error {
	ackCh := make(chan struct{})

	etcdInitializer := initializer.NewInitializer(restoreOpts, b.config.SnapstoreConfig, b.logger.Logger)

	b.logger.Infof("Creating snapstore from provider: %s", b.config.SnapstoreConfig.Provider)
	ss, err := snapstore.GetSnapstore(b.config.SnapstoreConfig)
	if err != nil {
		return fmt.Errorf("failed to create snapstore from configured storage provider: %v", err)
	}

	b.logger.Infof("Creating snapshotter...")
	ssr, err := snapshotter.NewSnapshotter(b.logger, b.config.SnapshotterConfig, ss, b.config.EtcdConnectionConfig)
	if err != nil {
		return err
	}

	handler := b.startHTTPServer(etcdInitializer, ssr)
	defer handler.Stop()

	ssrStopCh := make(chan struct{})
	go handleSsrStopRequest(ctx, handler, ssr, ackCh, ssrStopCh)
	go handleAckState(handler, ackCh)

	go defragmentor.DefragDataPeriodically(ctx, b.config.EtcdConnectionConfig, b.defragmentationSchedule, ssr.TriggerFullSnapshot, b.logger)

	b.runEtcdProbeLoopWithSnapshotter(ctx, handler, ssr, ssrStopCh, ackCh)
	return nil
}

// runEtcdProbeLoopWithoutSnapshotter runs the etcd probe loop
// for the case where snapshotter is configured correctly
func (b *BackupRestoreServer) runEtcdProbeLoopWithSnapshotter(ctx context.Context, handler *HTTPHandler, ssr *snapshotter.Snapshotter, ssrStopCh chan struct{}, ackCh chan struct{}) {
	var (
		err                       error
		initialDeltaSnapshotTaken bool
	)

	for {
		b.logger.Infof("Probing etcd...")
		select {
		case <-ctx.Done():
			b.logger.Info("Shutting down...")
			return
		default:
			err = b.probeEtcd(ctx)
		}
		if err != nil {
			b.logger.Errorf("Failed to probe etcd: %v", err)
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
		const recentFullSnapshotPeriodInHours = 24
		initialDeltaSnapshotTaken = false
		if ssr.PrevFullSnapshot != nil && time.Since(ssr.PrevFullSnapshot.CreatedOn).Hours() <= recentFullSnapshotPeriodInHours {
			ssrStopped, err := ssr.CollectEventsSincePrevSnapshot(ssrStopCh)
			if ssrStopped {
				b.logger.Info("Snapshotter stopped.")
				ackCh <- emptyStruct
				handler.SetStatus(http.StatusServiceUnavailable)
				b.logger.Info("Shutting down...")
				return
			}
			if err == nil {
				if _, err := ssr.TakeDeltaSnapshot(); err != nil {
					b.logger.Warnf("Failed to take first delta snapshot: snapshotter failed with error: %v", err)
					continue
				}
				initialDeltaSnapshotTaken = true
			} else {
				b.logger.Warnf("Failed to collect events for first delta snapshot(s): %v", err)
			}
		}
		if !initialDeltaSnapshotTaken {
			// need to take a full snapshot here
			metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: snapstore.SnapshotKindDelta}).Set(0)
			metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: snapstore.SnapshotKindFull}).Set(1)
			if _, err := ssr.TakeFullSnapshotAndResetTimer(); err != nil {
				b.logger.Errorf("Failed to take substitute first full snapshot: %v", err)
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
		b.logger.Infof("Starting snapshotter...")
		startWithFullSnapshot := !(initialDeltaSnapshotTaken && time.Since(ssr.PrevFullSnapshot.CreatedOn).Hours() <= recentFullSnapshotPeriodInHours)
		if err := ssr.Run(ssrStopCh, startWithFullSnapshot); err != nil {
			if etcdErr, ok := err.(*errors.EtcdError); ok == true {
				b.logger.Errorf("Snapshotter failed with etcd error: %v", etcdErr)
			} else {
				b.logger.Fatalf("Snapshotter failed with error: %v", err)
			}
		}
		b.logger.Infof("Snapshotter stopped.")
		ackCh <- emptyStruct
		handler.SetStatus(http.StatusServiceUnavailable)
		close(gcStopCh)
	}
}

// runEtcdProbeLoopWithoutSnapshotter runs the etcd probe loop
// for the case where snapshotter is not configured
func (b *BackupRestoreServer) runEtcdProbeLoopWithoutSnapshotter(ctx context.Context, handler *HTTPHandler) {
	var err error
	for {
		b.logger.Infof("Probing etcd...")
		select {
		case <-ctx.Done():
			b.logger.Info("Shutting down...")
			return
		default:
			err = b.probeEtcd(ctx)
		}
		if err != nil {
			b.logger.Errorf("Failed to probe etcd: %v", err)
			handler.SetStatus(http.StatusServiceUnavailable)
			continue
		}

		handler.SetStatus(http.StatusOK)
		<-ctx.Done()
		handler.SetStatus(http.StatusServiceUnavailable)
		b.logger.Infof("Received stop signal. Terminating !!")
		return
	}
}

// probeEtcd will make the snapshotter probe for etcd endpoint to be available
// before it starts taking regular snapshots.
func (b *BackupRestoreServer) probeEtcd(ctx context.Context) error {
	client, err := etcdutil.GetTLSClientForEtcd(b.config.EtcdConnectionConfig)
	if err != nil {
		return &errors.EtcdError{
			Message: fmt.Sprintf("failed to create etcd client: %v", err),
		}
	}

	ctx, cancel := context.WithTimeout(ctx, b.config.EtcdConnectionConfig.ConnectionTimeout.Duration)
	defer cancel()
	if _, err := client.Get(ctx, "foo"); err != nil {
		b.logger.Errorf("Failed to connect to client: %v", err)
		return err
	}
	return nil
}

func handleAckState(handler *HTTPHandler, ackCh chan struct{}) {
	for {
		<-ackCh
		if atomic.CompareAndSwapUint32(&handler.AckState, HandlerAckWaiting, HandlerAckDone) {
			handler.AckCh <- emptyStruct
		}
	}
}

// handleSsrStopRequest responds to handlers request and stop interrupt.
func handleSsrStopRequest(ctx context.Context, handler *HTTPHandler, ssr *snapshotter.Snapshotter, ackCh, ssrStopCh chan struct{}) {
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
