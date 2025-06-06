// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/backoff"
	"github.com/gardener/etcd-backup-restore/pkg/defragmentor"
	"github.com/gardener/etcd-backup-restore/pkg/errors"
	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	"github.com/gardener/etcd-backup-restore/pkg/etcdutil/client"
	"github.com/gardener/etcd-backup-restore/pkg/health/heartbeat"
	"github.com/gardener/etcd-backup-restore/pkg/health/membergarbagecollector"
	"github.com/gardener/etcd-backup-restore/pkg/initializer"
	"github.com/gardener/etcd-backup-restore/pkg/leaderelection"
	"github.com/gardener/etcd-backup-restore/pkg/member"
	"github.com/gardener/etcd-backup-restore/pkg/metrics"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/snapshotter"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/util/retry"
)

// BackupRestoreServer holds the details for backup-restore server.
type BackupRestoreServer struct {
	logger                  *logrus.Entry
	config                  *BackupRestoreComponentConfig
	defragmentationSchedule cron.Schedule
	backoffConfig           *backoff.ExponentialBackoff
}

var (
	// runServerWithSnapshotter indicates whether to start server with or without snapshotter.
	runServerWithSnapshotter = true
)

// NewBackupRestoreServer return new backup restore server.
func NewBackupRestoreServer(logger *logrus.Logger, config *BackupRestoreComponentConfig) (*BackupRestoreServer, error) {
	serverLogger := logger.WithField("actor", "backup-restore-server")
	defragmentationSchedule, err := cron.ParseStandard(config.DefragmentationSchedule)
	if err != nil {
		// Ideally this case should not occur, since this check is done at the config validaitions.
		return nil, err
	}
	exponentialBackoffConfig := backoff.NewExponentialBackOffConfig(config.ExponentialBackoffConfig.AttemptLimit, config.ExponentialBackoffConfig.Multiplier, config.ExponentialBackoffConfig.ThresholdTime.Duration)

	return &BackupRestoreServer{
		logger:                  serverLogger,
		config:                  config,
		defragmentationSchedule: defragmentationSchedule,
		backoffConfig:           exponentialBackoffConfig,
	}, nil
}

// Run starts the backup restore server.
func (b *BackupRestoreServer) Run(ctx context.Context) error {
	var etcdConfigPath string
	var err error

	etcdConfigPath = miscellaneous.GetConfigFilePath()

	config, err := miscellaneous.ReadConfigFileAsMap(etcdConfigPath)
	if err != nil {
		b.logger.WithFields(logrus.Fields{
			"configFile": etcdConfigPath,
		}).Fatalf("failed to read etcd config file: %v", err)
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
		EnableTLS:            b.config.ServerConfig.TLSCertFile != "" && b.config.ServerConfig.TLSKeyFile != "",
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

func waitUntilEtcdRunning(ctx context.Context, etcdConnectionConfig *brtypes.EtcdConnectionConfig, logger *logrus.Logger) error {
	ticker := time.NewTicker(4 * time.Second)
	defer ticker.Stop()
	logger.Info("Checking if etcd is running")
	for !isEtcdRunning(ctx, 2*time.Second, etcdConnectionConfig, logger) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
	logger.Info("Etcd is now running. Continuing backup-restore startup.")
	return nil
}

func isEtcdRunning(ctx context.Context, timeout time.Duration, etcdConnectionConfig *brtypes.EtcdConnectionConfig, logger *logrus.Logger) bool {
	factory := etcdutil.NewFactory(*etcdConnectionConfig)
	client, err := factory.NewMaintenance()
	if err != nil {
		logger.Errorf("failed to create etcd maintenance client: %v", err)
		return false
	}
	defer client.Close()

	if len(etcdConnectionConfig.Endpoints) == 0 {
		logger.Errorf("etcd endpoints are not passed correctly")
		return false
	}

	endpoint := etcdConnectionConfig.Endpoints[0]

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	_, err = client.Status(ctx, endpoint)
	if err != nil {
		logger.Errorf("failed to get status of etcd endPoint: %v with error: %v", endpoint, err)
		return false
	}
	return true
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
	etcdInitializer, err := initializer.NewInitializer(restoreOpts, snapstoreConfig, b.config.EtcdConnectionConfig, b.logger.Logger)
	if err != nil {
		return err
	}

	handler := b.startHTTPServer(etcdInitializer, b.config.SnapstoreConfig.Provider, b.config.EtcdConnectionConfig, b.config.SnapstoreConfig, nil)
	defer func() {
		if err := handler.Stop(); err != nil {
			b.logger.Errorf("unable to stop HTTP server: %s", err)
		}
	}()

	metrics.CurrentClusterSize.With(prometheus.Labels{}).Set(float64(restoreOpts.OriginalClusterSize))

	if err := waitUntilEtcdRunning(ctx, b.config.EtcdConnectionConfig, b.logger.Logger); err != nil {
		return err
	}

	if err := b.updatePeerURLIfChanged(ctx, handler.EnableTLS, b.logger.Logger); err != nil {
		b.logger.Errorf("failed to update member peer url: %v", err)
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
				go handleSsrStopRequest(leCtx, handler, ssr, ackCh, ssrStopCh, b.logger)
			}
			go b.runEtcdProbeLoopWithSnapshotter(leCtx, handler, ssr, ss, ssrStopCh, ackCh)
			go defragmentor.DefragDataPeriodically(leCtx, b.config.EtcdConnectionConfig, b.defragmentationSchedule, defragCallBack, b.logger)
			//start etcd member garbage collector
			if b.config.HealthConfig.EtcdMemberGCEnabled {
				go membergarbagecollector.RunMemberGarbageCollectorPeriodically(leCtx, b.config.HealthConfig, b.logger, b.config.EtcdConnectionConfig)
			}
		},
		OnStoppedLeading: func() {
			if runServerWithSnapshotter {
				b.logger.Info("backup-restore stops leading...")
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
				} else {
					logger.Errorf("unable to promote the learner to a voting member: %v", err)
				}
			}
		},
	}

	checkLeadershipFunc := leaderelection.EtcdMemberStatus

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

func (b *BackupRestoreServer) updatePeerURLIfChanged(ctx context.Context, tlsEnabled bool, logger *logrus.Logger) error {
	logger.Info("Checking if peerURL has changed or not.")

	m := member.NewMemberControl(b.config.EtcdConnectionConfig)

	cli, err := etcdutil.NewFactory(*b.config.EtcdConnectionConfig).NewCluster()
	if err != nil {
		return err
	}
	defer func() {
		if err := cli.Close(); err != nil {
			b.logger.Errorf("failed to close etcd client: %v", err)
		}
	}()

	changed, err := hasPeerURLChanged(ctx, m, cli)
	if err != nil {
		return err
	}
	if changed {
		b.logger.Info("Etcd member peerURLs found to be changed.")
		if err = retry.OnError(retry.DefaultBackoff, errors.IsErrNotNil, func() error {
			if err = m.UpdateMemberPeerURL(ctx, cli); err != nil {
				return err
			}
			b.logger.Info("Successfully updated the peerURLs for etcd member.")
			return nil
		}); err != nil {
			return err
		}
		if b.config.UseEtcdWrapper {
			if err := miscellaneous.RestartEtcdWrapper(ctx, tlsEnabled, b.config.EtcdConnectionConfig); err != nil {
				b.logger.Fatalf("failed to restart the etcd-wrapper: %v", err)
			}
		} else {
			b.logger.Info("Usage of etcd-wrapper found to be disabled")
			b.logger.Warnf("To correcly reflect peerURLs in etcd cluster. Please restart the etcd member. More info: https://etcd.io/docs/v3.5/op-guide/runtime-configuration/#update-advertise-peer-urls")
		}
	} else {
		b.logger.Info("No change in peerURLs found. Skipping update of member peer URLs.")
	}
	return nil
}

// runEtcdProbeLoopWithSnapshotter runs the etcd probe loop
// for the case when backup-restore becomes leading sidecar.
func (b *BackupRestoreServer) runEtcdProbeLoopWithSnapshotter(ctx context.Context, handler *HTTPHandler, ssr *snapshotter.Snapshotter, ss brtypes.SnapStore, ssrStopCh <-chan struct{}, ackCh chan<- struct{}) {
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
			b.logger.Errorf("failed to probe etcd: %v", err)
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

			fullSnapshotMaxTimeWindowInHours := ssr.GetFullSnapshotMaxTimeWindow(b.config.SnapshotterConfig.FullSnapshotSchedule)
			initialDeltaSnapshotTaken = false
			if !ssr.IsFullSnapshotRequiredAtStartup(fullSnapshotMaxTimeWindowInHours) {
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
				// if initial deltaSnapshot is not taken
				// or previous full snapshot wasn't successful
				var snapshot *brtypes.Snapshot
				metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindDelta}).Set(0)
				metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindFull}).Set(1)
				if snapshot, err = ssr.TakeFullSnapshotAndResetTimer(false); err != nil {
					metrics.SnapshotterOperationFailure.With(prometheus.Labels{metrics.LabelError: err.Error()}).Inc()
					ssr.PrevFullSnapshotSucceeded = false
					b.logger.Errorf("Failed to take substitute full snapshot: %v", err)
					continue
				}
				ssr.PrevFullSnapshotSucceeded = true
				if b.config.HealthConfig.SnapshotLeaseRenewalEnabled {
					leaseUpdatectx, cancel := context.WithTimeout(ctx, brtypes.LeaseUpdateTimeoutDuration)
					defer cancel()
					if err = heartbeat.FullSnapshotCaseLeaseUpdate(leaseUpdatectx, b.logger, snapshot, ssr.K8sClientset, b.config.HealthConfig.FullSnapshotLeaseName, snapshot.CreatedOn); err != nil {
						b.logger.Warnf("Snapshot lease update failed : %v", err)
					}
				}
				if b.backoffConfig.Start {
					b.backoffConfig.ResetExponentialBackoff()
				}
			}

			// Set snapshotter state to Active
			ssr.SetSnapshotterActive()

			// Start garbage collector
			gcStopCh := make(chan struct{})
			b.logger.Info("Starting the garbage collector...")
			go ssr.RunGarbageCollector(gcStopCh)

			// Start snapshotter
			b.logger.Infof("Starting snapshotter...")
			startWithFullSnapshot := ssr.IsFullSnapshotRequiredAtStartup(fullSnapshotMaxTimeWindowInHours)
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
	b.logger.Info("Probing Etcd by checking etcd status ...")
	var endPoint string
	client, err := etcdutil.NewFactory(*b.config.EtcdConnectionConfig).NewMaintenance()
	if err != nil {
		return &errors.EtcdError{
			Message: fmt.Sprintf("failed to create etcd maintenance client: %v", err),
		}
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, brtypes.DefaultEtcdStatusConnecTimeout)
	defer cancel()

	if len(b.config.EtcdConnectionConfig.Endpoints) == 0 {
		return fmt.Errorf("etcd endpoints are not passed correctly")
	}
	endPoint = b.config.EtcdConnectionConfig.Endpoints[0]

	if _, err := client.Status(ctx, endPoint); err != nil {
		b.logger.Errorf("failed to get status of etcd endPoint: %v with error: %v", endPoint, err)
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
func handleSsrStopRequest(ctx context.Context, handler *HTTPHandler, _ *snapshotter.Snapshotter, ackCh chan<- struct{}, ssrStopCh chan<- struct{}, logger *logrus.Entry) {
	logger.Info("Starting the handleSsrStopRequest handler...")
	for {
		var ok bool
		select {
		case _, ok = <-handler.ReqCh:
		case _, ok = <-ctx.Done():
			logger.Infof("handleSsrStopRequest: %v", ctx.Err())
		}

		ackCh <- emptyStruct
		close(ssrStopCh)

		if !ok {
			logger.Info("Stopping handleSsrStopRequest handler...")
			return
		}
	}
}

func hasPeerURLChanged(ctx context.Context, m member.Control, cli client.ClusterCloser) (bool, error) {
	peerURLsFromEtcdConfig, err := miscellaneous.GetMemberPeerURLs(miscellaneous.GetConfigFilePath())
	if err != nil {
		return false, fmt.Errorf("failed to get initial advertise peer URLs: %w", err)
	}
	existingPeerURLs, err := m.GetPeerURLs(ctx, cli)
	if err != nil {
		return false, err
	}
	return sets.New[string](peerURLsFromEtcdConfig...).Difference(sets.New[string](existingPeerURLs...)).Len() > 0, nil
}
