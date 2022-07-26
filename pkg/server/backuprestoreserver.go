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
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/backoff"
	"github.com/gardener/etcd-backup-restore/pkg/common"
	"github.com/gardener/etcd-backup-restore/pkg/leaderelection"
	"github.com/gardener/etcd-backup-restore/pkg/metrics"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/ghodss/yaml"

	"github.com/gardener/etcd-backup-restore/pkg/defragmentor"
	"github.com/gardener/etcd-backup-restore/pkg/errors"
	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	"github.com/gardener/etcd-backup-restore/pkg/health/heartbeat"
	"github.com/gardener/etcd-backup-restore/pkg/health/membergarbagecollector"
	"github.com/gardener/etcd-backup-restore/pkg/initializer"
	"github.com/gardener/etcd-backup-restore/pkg/member"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/snapshotter"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"

	"github.com/prometheus/client_golang/prometheus"
	cron "github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/pkg/types"
	"k8s.io/apimachinery/pkg/util/clock"
	"k8s.io/client-go/util/retry"
)

// BackupRestoreServer holds the details for backup-restore server.
type BackupRestoreServer struct {
	logger                  *logrus.Entry
	config                  *BackupRestoreComponentConfig
	ownerChecker            common.Checker
	etcdProcessKiller       common.ProcessKiller
	defragmentationSchedule cron.Schedule
	backoffConfig           *backoff.ExponentialBackoff
}

var (
	// runServerWithSnapshotter indicates whether to start server with or without snapshotter.
	runServerWithSnapshotter bool = true
	retryTimeout                  = 5 * time.Second
)

// NewBackupRestoreServer return new backup restore server.
func NewBackupRestoreServer(logger *logrus.Logger, config *BackupRestoreComponentConfig) (*BackupRestoreServer, error) {
	serverLogger := logger.WithField("actor", "backup-restore-server")
	occ := config.OwnerCheckConfig
	var ownerChecker common.Checker
	if occ.OwnerName != "" && occ.OwnerID != "" {
		resolver := common.NewCachingResolver(net.DefaultResolver, clock.RealClock{}, occ.OwnerCheckDNSCacheTTL.Duration)
		ownerChecker = common.NewOwnerChecker(occ.OwnerName, occ.OwnerID, occ.OwnerCheckTimeout.Duration, resolver, serverLogger)
	}
	etcdProcessKiller := common.NewNamedProcessKiller(config.EtcdProcessName, common.NewGopsutilProcessLister(), serverLogger)
	defragmentationSchedule, err := cron.ParseStandard(config.DefragmentationSchedule)
	if err != nil {
		// Ideally this case should not occur, since this check is done at the config validaitions.
		return nil, err
	}
	exponentialBackoffConfig := backoff.NewExponentialBackOffConfig(config.ExponentialBackoffConfig.AttemptLimit, config.ExponentialBackoffConfig.Multiplier, config.ExponentialBackoffConfig.ThresholdTime.Duration)

	return &BackupRestoreServer{
		logger:                  serverLogger,
		config:                  config,
		ownerChecker:            ownerChecker,
		etcdProcessKiller:       etcdProcessKiller,
		defragmentationSchedule: defragmentationSchedule,
		backoffConfig:           exponentialBackoffConfig,
	}, nil
}

// Run starts the backup restore server.
func (b *BackupRestoreServer) Run(ctx context.Context) error {
	var inputFileName string
	var err error

	inputFileName = miscellaneous.GetConfigFilePath()

	configYML, err := os.ReadFile(inputFileName)
	if err != nil {
		b.logger.Fatalf("Unable to read etcd config file: %v", err)
		return err
	}

	config := map[string]interface{}{}
	if err := yaml.Unmarshal([]byte(configYML), &config); err != nil {
		b.logger.Fatalf("Unable to unmarshal etcd config yaml file: %v", err)
		return err
	}

	initialClusterSize, err := miscellaneous.GetClusterSize(fmt.Sprint(config["initial-cluster"]))
	if err != nil {
		b.logger.Fatal("Please provide initial cluster value for embedded ETCD")
	}

	clusterURLsMap, err := types.NewURLsMap(b.config.RestorationConfig.InitialCluster)
	if err != nil {
		// Ideally this case should not occur, since this check is done at the config validations.
		b.logger.Fatalf("failed creating url map for restore cluster: %v", err)
	}

	peerURLs, err := types.NewURLs(b.config.RestorationConfig.InitialAdvertisePeerURLs)
	if err != nil {
		// Ideally this case should not occur, since this check is done at the config validations.
		b.logger.Fatalf("failed creating url map for restore cluster: %v", err)
	}

	options := &brtypes.RestoreOptions{
		Config:              b.config.RestorationConfig,
		ClusterURLs:         clusterURLsMap,
		OriginalClusterSize: initialClusterSize,
		PeerURLs:            peerURLs,
	}

	if b.config.SnapstoreConfig == nil || len(b.config.SnapstoreConfig.Provider) == 0 {
		b.logger.Warnf("No snapstore storage provider configured. Will not start backup schedule.")
		runServerWithSnapshotter = false
	}
	return b.runServer(ctx, options)
}

// startHTTPServer creates and starts the HTTP handler
// with status 503 (Service Unavailable)
func (b *BackupRestoreServer) startHTTPServer(initializer initializer.Initializer, storageProvider string, etcdConfig *brtypes.EtcdConnectionConfig, snapstoreConfig *brtypes.SnapstoreConfig, ssr *snapshotter.Snapshotter) *HTTPHandler {
	// Start http handler with Error state and wait till snapshotter is up
	// and running before setting the status to OK.
	handler := &HTTPHandler{
		Port:                 b.config.ServerConfig.Port,
		Initializer:          initializer,
		Snapshotter:          ssr,
		Logger:               b.logger,
		StopCh:               make(chan struct{}),
		EnableProfiling:      b.config.ServerConfig.EnableProfiling,
		ReqCh:                make(chan struct{}),
		AckCh:                make(chan struct{}),
		EnableTLS:            (b.config.ServerConfig.TLSCertFile != "" && b.config.ServerConfig.TLSKeyFile != ""),
		ServerTLSCertFile:    b.config.ServerConfig.TLSCertFile,
		ServerTLSKeyFile:     b.config.ServerConfig.TLSKeyFile,
		HTTPHandlerMutex:     &sync.Mutex{},
		EtcdConnectionConfig: etcdConfig,
		StorageProvider:      storageProvider,
		SnapstoreConfig:      snapstoreConfig,
	}
	handler.SetStatus(http.StatusServiceUnavailable)
	b.logger.Info("Registering the http request handlers...")
	handler.RegisterHandler()
	b.logger.Info("Starting the http server...")
	go handler.Start()

	return handler
}

// runServer runs the etcd-backup-restore server according to snapstore provider configuration.
func (b *BackupRestoreServer) runServer(ctx context.Context, restoreOpts *brtypes.RestoreOptions) error {
	var (
		snapstoreConfig *brtypes.SnapstoreConfig
		ssr             *snapshotter.Snapshotter
		ss              brtypes.SnapStore
	)
	ackCh := make(chan struct{})
	ssrStopCh := make(chan struct{})
	mmStopCh := make(chan struct{})

	if runServerWithSnapshotter {
		snapstoreConfig = b.config.SnapstoreConfig
	}
	etcdInitializer := initializer.NewInitializer(restoreOpts, snapstoreConfig, b.config.EtcdConnectionConfig, b.logger.Logger)

	handler := b.startHTTPServer(etcdInitializer, b.config.SnapstoreConfig.Provider, b.config.EtcdConnectionConfig, b.config.SnapstoreConfig, nil)
	defer handler.Stop()

	// Promotes member if it is a learner
	if restoreOpts.OriginalClusterSize > 1 {
		for {
			select {
			case <-ctx.Done():
				b.logger.Info("Context cancelled. Stopping retry promoting member")
				return ctx.Err()
			default:
			}
			m := member.NewMemberControl(b.config.EtcdConnectionConfig)
			err := m.PromoteMember(ctx)
			if err == nil {
				break
			}
			miscellaneous.SleepWithContext(ctx, retryTimeout)
		}
	} else {
		// when OriginalClusterSize = 1
		m := member.NewMemberControl(b.config.EtcdConnectionConfig)
		err := retry.OnError(retry.DefaultBackoff, func(err error) bool {
			return err != nil
		}, func() error {
			cli, err := etcdutil.NewFactory(*b.config.EtcdConnectionConfig).NewCluster()
			if err != nil {
				return err
			}
			return m.UpdateMember(ctx, cli)
		})
		if err != nil {
			b.logger.Error("unable to update the member")
		}
	}

	leaderCallbacks := &brtypes.LeaderCallbacks{
		OnStartedLeading: func(leCtx context.Context) {
			ssrStopCh = make(chan struct{})
			var err error
			var defragCallBack defragmentor.CallbackFunc
			if runServerWithSnapshotter {
				b.logger.Infof("Creating snapstore from provider: %s", b.config.SnapstoreConfig.Provider)
				ss, err = snapstore.GetSnapstore(b.config.SnapstoreConfig)
				if err != nil {
					b.logger.Fatalf("failed to create snapstore from configured storage provider: %v", err)
				}

				// Get the new snapshotter object
				b.logger.Infof("Creating snapshotter...")
				ssr, err = snapshotter.NewSnapshotter(b.logger, b.config.SnapshotterConfig, ss, b.config.EtcdConnectionConfig, b.config.CompressionConfig, b.config.HealthConfig, b.config.SnapstoreConfig)
				if err != nil {
					b.logger.Fatalf("failed to create new Snapshotter object: %v", err)
				}

				// set "http handler" with the latest snapshotter object
				handler.SetSnapshotter(ssr)
				defragCallBack = ssr.TriggerFullSnapshot
				go handleSsrStopRequest(leCtx, handler, ssr, ackCh, ssrStopCh)
			}
			go b.runEtcdProbeLoopWithSnapshotter(leCtx, handler, ssr, ss, ssrStopCh, ackCh)
			go defragmentor.DefragDataPeriodically(leCtx, b.config.EtcdConnectionConfig, b.defragmentationSchedule, defragCallBack, b.logger)
			//start etcd member garbage collector
			if b.config.HealthConfig.EtcdMemberGCEnabled {
				go membergarbagecollector.RunMemberGarbageCollectorPeriodically(leCtx, b.config.HealthConfig, b.logger, b.config.EtcdConnectionConfig)
			}
		},
		OnStoppedLeading: func() {
			// stops the running snapshotter
			if runServerWithSnapshotter {
				ssr.SsrStateMutex.Lock()
				defer ssr.SsrStateMutex.Unlock()
				if ssr.SsrState == brtypes.SnapshotterActive {
					ssrStopCh <- emptyStruct
					b.logger.Info("backup-restore stops leading...")
				}
				handler.SetSnapshotterToNil()

				// TODO @ishan16696: For Multi-node etcd HTTP status need to be set to `StatusServiceUnavailable` only when backup-restore is in "StateUnknown".
				handler.SetStatus(http.StatusServiceUnavailable)
			}
		},
	}

	memberLeaseCallbacks := &brtypes.MemberLeaseCallbacks{
		StartLeaseRenewal: func() {
			mmStopCh = make(chan struct{})
			if b.config.HealthConfig.MemberLeaseRenewalEnabled {
				go func() {
					if err := heartbeat.RenewMemberLeasePeriodically(ctx, mmStopCh, b.config.HealthConfig, b.logger, b.config.EtcdConnectionConfig); err != nil {
						b.logger.Fatalf("failed RenewMemberLeases: %v", err)
					}
				}()
			}
		},
		StopLeaseRenewal: func() {
			if b.config.HealthConfig.MemberLeaseRenewalEnabled {
				mmStopCh <- emptyStruct
			}
		},
	}

	promoteCallback := &brtypes.PromoteLearnerCallback{
		Promote: func(ctx context.Context, logger *logrus.Entry) {
			if restoreOpts.OriginalClusterSize > 1 {
				m := member.NewMemberControl(b.config.EtcdConnectionConfig)
				if err := m.PromoteMember(ctx); err == nil {
					logger.Info("Successfully promoted the learner to a voting member...")
				} else if err != nil {
					logger.Errorf("unable to promote the learner to a voting member: %v", err)
				}
			}
		},
	}

	checkLeadershipFunc := leaderelection.IsLeader

	b.logger.Infof("Creating leaderElector...")
	le, err := leaderelection.NewLeaderElector(b.logger, b.config.EtcdConnectionConfig, b.config.LeaderElectionConfig, leaderCallbacks, memberLeaseCallbacks, checkLeadershipFunc, promoteCallback)
	if err != nil {
		return err
	}

	if runServerWithSnapshotter {
		go handleAckState(handler, ackCh)
	}

	if b.config.HealthConfig.MemberLeaseRenewalEnabled {
		go func() {
			if err := heartbeat.RenewMemberLeasePeriodically(ctx, mmStopCh, b.config.HealthConfig, b.logger, b.config.EtcdConnectionConfig); err != nil {
				b.logger.Fatalf("failed RenewMemberLeases: %v", err)
			}
		}()
	}

	return le.Run(ctx)
}

// runEtcdProbeLoopWithSnapshotter runs the etcd probe loop
// for the case when backup-restore becomes leading sidecar.
func (b *BackupRestoreServer) runEtcdProbeLoopWithSnapshotter(ctx context.Context, handler *HTTPHandler, ssr *snapshotter.Snapshotter, ss brtypes.SnapStore, ssrStopCh chan struct{}, ackCh chan struct{}) {
	var (
		err                               error
		initialDeltaSnapshotTaken         bool
		killEtcdBeforeTakingFinalSnapshot bool
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

		if b.backoffConfig.Start {
			backoffTime := b.backoffConfig.GetNextBackoffTime()
			b.logger.Info("Backoff time: ", backoffTime)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoffTime):
			}
		}

		if runServerWithSnapshotter {
			if b.ownerChecker != nil {
				// Check if the actual owner ID matches the expected one
				// If the check returns false, take a final full snapshot if needed.
				// If the check returns an error continue with normal operation
				b.logger.Debugf("Checking owner before starting snapshotter...")
				result, err := b.ownerChecker.Check(ctx)
				if err != nil {
					b.logger.Errorf("ownerChecker check fails: %v", err)
				} else if !result {
					handler.SetStatus(http.StatusServiceUnavailable)
					// If owner check was previously successful, but is now failed, kill the etcd process before attempting
					// to take a final snapshot to ensure that any open connections from kube-apiserver are terminated
					if killEtcdBeforeTakingFinalSnapshot {
						if _, err := b.etcdProcessKiller.Kill(ctx); err != nil {
							b.logger.Errorf("Could not kill etcd process: %v", err)
							// only attempt to take final snapshot if process was successfully killed.
							continue
						}
						killEtcdBeforeTakingFinalSnapshot = false
					}
					// If the previous full snapshot doesn't exist or is not marked as final, take a final full snapshot
					if ssr.PrevFullSnapshot == nil || !ssr.PrevFullSnapshot.IsFinal {
						b.logger.Infof("Taking final full snapshot...")
						var snapshot *brtypes.Snapshot
						if snapshot, err = ssr.TakeFullSnapshotAndResetTimer(true); err != nil {
							b.logger.Errorf("Could not take final full snapshot: %v", err)
							continue
						}
						if b.config.HealthConfig.SnapshotLeaseRenewalEnabled {
							leaseUpdatectx, cancel := context.WithTimeout(ctx, brtypes.LeaseUpdateTimeoutDuration)
							defer cancel()
							if err = heartbeat.FullSnapshotCaseLeaseUpdate(leaseUpdatectx, b.logger, snapshot, ssr.K8sClientset, b.config.HealthConfig.FullSnapshotLeaseName, b.config.HealthConfig.DeltaSnapshotLeaseName); err != nil {
								b.logger.Warnf("Snapshot lease update failed : %v", err)
							}
						}
					}

					// Wait for the configured interval before making another attempt
					b.logger.Infof("Waiting for %s...", b.config.OwnerCheckConfig.OwnerCheckInterval.Duration)
					select {
					case <-ctx.Done():
						b.logger.Info("Shutting down...")
						return
					case <-time.After(b.config.OwnerCheckConfig.OwnerCheckInterval.Duration):
					}

					continue
				}

				// At this point owner check was successful, so set killEtcdBeforeTakingFinalSnapshot to true.
				// The etcd process will be killed before taking a final snapshot if the owner check fails.
				killEtcdBeforeTakingFinalSnapshot = true
			}

			// set server's healthz endpoint status to OK so that
			// etcd is marked as ready to serve traffic
			handler.SetStatus(http.StatusOK)

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

			// Temporary fix for missing alternate full snapshots for Gardener shoots
			// with hibernation schedule set: change value from 24 ot 23.5 to
			// accommodate for slight pod spin-up delays on shoot wake-up
			const recentFullSnapshotPeriodInHours = 23.5
			initialDeltaSnapshotTaken = false
			if ssr.PrevFullSnapshot != nil && !ssr.PrevFullSnapshot.IsFinal && time.Since(ssr.PrevFullSnapshot.CreatedOn).Hours() <= recentFullSnapshotPeriodInHours {
				ssrStopped, err := ssr.CollectEventsSincePrevSnapshot(ssrStopCh)
				if ssrStopped {
					b.logger.Info("Snapshotter stopped.")
					ackCh <- emptyStruct
					b.logger.Info("Shutting down...")
					return
				}
				if err == nil {
					if _, err := ssr.TakeDeltaSnapshot(); err != nil {
						b.logger.Warnf("Failed to take first delta snapshot: snapshotter failed with error: %v", err)
						continue
					}
					initialDeltaSnapshotTaken = true
					if b.config.HealthConfig.SnapshotLeaseRenewalEnabled {
						leaseUpdatectx, cancel := context.WithTimeout(ctx, brtypes.LeaseUpdateTimeoutDuration)
						defer cancel()
						if err = heartbeat.DeltaSnapshotCaseLeaseUpdate(leaseUpdatectx, b.logger, ssr.K8sClientset, b.config.HealthConfig.DeltaSnapshotLeaseName, ss); err != nil {
							b.logger.Warnf("Snapshot lease update failed : %v", err)
						}
					}
					if b.backoffConfig.Start {
						b.backoffConfig.ResetExponentialBackoff()
					}
				} else {
					b.logger.Warnf("Failed to collect events for first delta snapshot(s): %v", err)
				}
			}

			if !initialDeltaSnapshotTaken {
				// need to take a full snapshot here
				var snapshot *brtypes.Snapshot
				metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindDelta}).Set(0)
				metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindFull}).Set(1)
				if snapshot, err = ssr.TakeFullSnapshotAndResetTimer(false); err != nil {
					metrics.SnapshotterOperationFailure.With(prometheus.Labels{metrics.LabelError: err.Error()}).Inc()
					b.logger.Errorf("Failed to take substitute first full snapshot: %v", err)
					continue
				}
				if b.config.HealthConfig.SnapshotLeaseRenewalEnabled {
					leaseUpdatectx, cancel := context.WithTimeout(ctx, brtypes.LeaseUpdateTimeoutDuration)
					defer cancel()
					if err = heartbeat.FullSnapshotCaseLeaseUpdate(leaseUpdatectx, b.logger, snapshot, ssr.K8sClientset, b.config.HealthConfig.FullSnapshotLeaseName, b.config.HealthConfig.DeltaSnapshotLeaseName); err != nil {
						b.logger.Warnf("Snapshot lease update failed : %v", err)
					}
				}
				if b.backoffConfig.Start {
					b.backoffConfig.ResetExponentialBackoff()
				}
			}

			// Set snapshotter state to Active
			ssr.SsrStateMutex.Lock()
			ssr.SsrState = brtypes.SnapshotterActive
			ssr.SsrStateMutex.Unlock()

			// Start owner check watchdog
			var ownerCheckWatchdog common.Watchdog
			if b.ownerChecker != nil {
				ownerCheckWatchdog = common.NewCheckerActionWatchdog(b.ownerChecker, common.ActionFunc(func(ctx context.Context) {
					b.stopSnapshotter(handler)
				}), b.config.OwnerCheckConfig.OwnerCheckInterval.Duration, clock.RealClock{}, b.logger)
				ownerCheckWatchdog.Start(ctx)
			}

			// Start garbage collector
			gcStopCh := make(chan struct{})
			b.logger.Info("Starting the garbage collector...")
			go ssr.RunGarbageCollector(gcStopCh)

			// Start snapshotter
			b.logger.Infof("Starting snapshotter...")
			startWithFullSnapshot := ssr.PrevFullSnapshot == nil || ssr.PrevFullSnapshot.IsFinal || !(time.Since(ssr.PrevFullSnapshot.CreatedOn).Hours() <= recentFullSnapshotPeriodInHours)
			if err := ssr.Run(ssrStopCh, startWithFullSnapshot); err != nil {
				if etcdErr, ok := err.(*errors.EtcdError); ok {
					metrics.SnapshotterOperationFailure.With(prometheus.Labels{metrics.LabelError: etcdErr.Error()}).Inc()
					b.logger.Errorf("Snapshotter failed with etcd error: %v", etcdErr)
				} else {
					metrics.SnapshotterOperationFailure.With(prometheus.Labels{metrics.LabelError: err.Error()}).Inc()
					b.logger.Errorf("Snapshotter failed with error: %v", err)
				}
				// snapshotter failed
				// start the backoff exponential mechanism.
				b.backoffConfig.Start = true
			}
			b.logger.Infof("Snapshotter stopped.")
			ackCh <- emptyStruct

			// Stop garbage collector
			close(gcStopCh)

			if b.ownerChecker != nil {
				// Stop owner check watchdog
				ownerCheckWatchdog.Stop()
			}

		} else {
			// for the case when snapshotter is not configured

			// set server's healthz endpoint status to OK so that
			// etcd is marked as ready to serve traffic
			handler.SetStatus(http.StatusOK)
			<-ctx.Done()
			handler.SetStatus(http.StatusServiceUnavailable)
			b.logger.Infof("Received stop signal. Terminating !!")
		}
	}
}

// probeEtcd will make the snapshotter probe for etcd endpoint to be available
// before it starts taking regular snapshots.
func (b *BackupRestoreServer) probeEtcd(ctx context.Context) error {
	clientFactory := etcdutil.NewFactory(*b.config.EtcdConnectionConfig)
	clientKV, err := clientFactory.NewKV()
	if err != nil {
		return &errors.EtcdError{
			Message: fmt.Sprintf("Failed to create etcd KV client: %v", err),
		}
	}
	defer clientKV.Close()

	ctx, cancel := context.WithTimeout(ctx, b.config.EtcdConnectionConfig.ConnectionTimeout.Duration)
	defer cancel()
	if _, err := clientKV.Get(ctx, "foo"); err != nil {
		b.logger.Errorf("Failed to connect to etcd KV client: %v", err)
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
		if ssr.SsrState == brtypes.SnapshotterActive {
			ssr.SsrStateMutex.Unlock()
			ssrStopCh <- emptyStruct
		} else {
			ssr.SsrState = brtypes.SnapshotterInactive
			ssr.SsrStateMutex.Unlock()
			ackCh <- emptyStruct
		}
		if !ok {
			return
		}
	}
}

func (b *BackupRestoreServer) stopSnapshotter(handler *HTTPHandler) {
	b.logger.Infof("Stopping snapshotter...")
	atomic.StoreUint32(&handler.AckState, HandlerAckWaiting)
	handler.Logger.Info("Changing handler state...")
	handler.ReqCh <- emptyStruct
	handler.Logger.Info("Waiting for acknowledgment...")
	<-handler.AckCh
}
