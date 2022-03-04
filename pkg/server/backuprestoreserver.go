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
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/backoff"
	"github.com/gardener/etcd-backup-restore/pkg/common"
	"github.com/gardener/etcd-backup-restore/pkg/leaderelection"
	"github.com/gardener/etcd-backup-restore/pkg/metrics"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"gopkg.in/yaml.v2"

	"github.com/gardener/etcd-backup-restore/pkg/defragmentor"
	"github.com/gardener/etcd-backup-restore/pkg/errors"
	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	"github.com/gardener/etcd-backup-restore/pkg/health/heartbeat"
	"github.com/gardener/etcd-backup-restore/pkg/initializer"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/snapshotter"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"

	"github.com/prometheus/client_golang/prometheus"
	cron "github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/pkg/transport"
	"go.etcd.io/etcd/pkg/types"
	v1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/clock"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/pointer"
	k8sClient "sigs.k8s.io/controller-runtime/pkg/client"
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
	runServerWithSnapshotter bool   = true
	etcdConfig               string = "/var/etcd/config/etcd.conf.yaml"
	defaultRestoreProtocol   string = "http"
	defaultRestoreClientPort string = "2479"
	defaultRestorePeerPort   string = "2380"
	defaultRestoreLCurl      string = "http://0.0.0.0:2479"
	defaultRestoreLPurl      string = "http://0.0.0.0:2380"
)

const (
	podNamespace = "POD_NAMESPACE"
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

	// (For testing purpose) If no ETCD_CONF variable set as environment variable, then consider backup-restore server is not used for tests.
	// For tests or to run backup-restore server as standalone, user needs to set ETCD_CONF variable with proper location of ETCD config yaml
	etcdConfigForTest := os.Getenv("ETCD_CONF")
	if etcdConfigForTest != "" {
		inputFileName = etcdConfigForTest
	} else {
		inputFileName = etcdConfig
	}

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

	// fetch pod name from env
	podName := os.Getenv("POD_NAME")
	config["name"] = podName

	protocol := "http"

	initAdPeerURL := config["initial-advertise-peer-urls"]
	protocol, svcName, namespace, peerPort, err := parsePeerURL(fmt.Sprint(initAdPeerURL))
	if err != nil {
		b.logger.Warnf("Unable to determine protocol, service name, namespace, port from advertise peer urls : %v", err)
		b.logger.Warn("If this is not testing environment, you may face serious problem running ETCD cluster as service name and namespace could not be determined")
	}

	if peerPort == "" {
		peerPort = defaultRestorePeerPort
	}

	var domainName string = ""
	if svcName != "" {
		domainName = fmt.Sprintf("%s.%s.%s", svcName, namespace, "svc")
	}

	if domainName != "" {
		config["initial-advertise-peer-urls"] = fmt.Sprintf("%s://%s.%s:%s", protocol, podName, domainName, peerPort)
	}

	pURLS := strings.Split(fmt.Sprint(config["initial-advertise-peer-urls"]), ",")
	peerURLs, err := types.NewURLs(pURLS)
	if err != nil {
		// Ideally this case should not occur, since this check is done at the config validations.
		b.logger.Fatalf("failed creating peer URLs restore cluster: %v", err)
		return err
	}

	var peerTLSInfo *transport.TLSInfo
	var peerAutoTLS bool
	if protocol == "https" {
		// marshal peer-transport-security into yaml
		out, err := yaml.Marshal(config["peer-transport-security"])
		if err != nil {
			b.logger.Fatalf("Unable to marshal peer-transport-security to yaml : %v", err)
			return err
		}

		tls := map[string]interface{}{}
		if err := yaml.Unmarshal(out, &tls); err != nil {
			b.logger.Fatalf("Unable to unmarshal peer-transport-security: %v", err)
			return err
		}

		peerTLSInfo = &transport.TLSInfo{
			TrustedCAFile: fmt.Sprint(tls["trusted-ca-file"]),
			CertFile:      fmt.Sprint(tls["cert-file"]),
			KeyFile:       fmt.Sprint(tls["key-file"]),
		}

		auth, _ := strconv.ParseBool(fmt.Sprint(tls["client-cert-auth"]))
		peerTLSInfo.ClientCertAuth = auth

		peerAutoTLS, _ = strconv.ParseBool(fmt.Sprint((tls["auto-tls"])))
	}

	if protocol == "" {
		protocol = defaultRestoreProtocol
	}

	lPurl := fmt.Sprint(config["listen-peer-urls"])
	if lPurl == "" {
		lPurl = defaultRestoreLPurl
	}

	lPUrlParsed, err := url.Parse(lPurl)
	if err != nil {
		b.logger.Fatalf("Could not parse listen-peer-urls : %s", fmt.Sprint(config["listen-peer-urls"]))
	}

	lPurl = fmt.Sprintf("%s://%s:%s", protocol, lPUrlParsed.Hostname(), peerPort)

	advClientURL := config["advertise-client-urls"]
	protocol, svcName, namespace, clientPort, err := parseAdvClientURL(fmt.Sprint(advClientURL))
	if err != nil {
		b.logger.Warnf("Unable to determine protocol, service name, namespace, port from advertise client url : %v", err)
		b.logger.Warn("If this is not testing environment, you may face serious problem running ETCD cluster as service name and namespace could not be determined")
	}

	if clientPort == defaultRestoreClientPort {
		b.logger.Fatalf("Client Port %s for main ETCD must not be same as client port %s for embedded ETCD", clientPort, defaultRestoreClientPort)
	}
	domainName = ""
	if svcName != "" {
		domainName = fmt.Sprintf("%s.%s.%s", svcName, namespace, "svc")
	}

	if domainName != "" {
		config["advertise-client-urls"] = fmt.Sprintf("%s://%s.%s:%s", protocol, podName, domainName, defaultRestoreClientPort)
	}

	cURLs := strings.Split(fmt.Sprint(config["advertise-client-urls"]), ",")

	var clientTLSInfo *transport.TLSInfo
	var clientAutoTLS bool
	if protocol == "https" {
		// marshal client-transport-security into yaml
		out, err := yaml.Marshal(config["client-transport-security"])
		if err != nil {
			b.logger.Fatalf("Unable to marshal client-transport-security to yaml : %v", err)
			return err
		}

		tls := map[string]interface{}{}
		if err := yaml.Unmarshal(out, &tls); err != nil {
			b.logger.Fatalf("Unable to unmarshal client-transport-security: %v", err)
			return err
		}

		clientTLSInfo = &transport.TLSInfo{
			TrustedCAFile: fmt.Sprint(tls["trusted-ca-file"]),
			CertFile:      fmt.Sprint(tls["cert-file"]),
			KeyFile:       fmt.Sprint(tls["key-file"]),
		}

		auth, _ := strconv.ParseBool(fmt.Sprint(tls["client-cert-auth"]))
		clientTLSInfo.ClientCertAuth = auth

		clientAutoTLS, _ = strconv.ParseBool(fmt.Sprint(tls["auto-tls"]))
	}

	if protocol == "" {
		protocol = defaultRestoreProtocol
	}

	lCurl := fmt.Sprint(config["listen-client-urls"])
	if lCurl == "" {
		lCurl = defaultRestoreLCurl
	}

	lCUrlParsed, err := url.Parse(lCurl)
	if err != nil {
		b.logger.Fatalf("Could not parse listen-client-urls : %s", fmt.Sprint(config["listen-client-urls"]))
	}

	lCurl = fmt.Sprintf("%s://%s:%s", protocol, lCUrlParsed.Hostname(), defaultRestoreClientPort)

	embeddedEtcdQuotaBytes, err := strconv.ParseInt(fmt.Sprint(config["quota-backend-bytes"]), 10, 64)
	if err != nil {
		b.logger.Fatalf("failed to fetch ETCD Quota Bytes for restore cluster: %v", err)
		return err
	}

	restoreConfig := &brtypes.RestorationConfig{
		InitialClusterToken:      fmt.Sprint(config["initial-cluster-token"]),
		RestoreDataDir:           fmt.Sprint(config["data-dir"]),
		InitialAdvertisePeerURLs: pURLS,
		AdvertiseClientURLs:      cURLs,
		Name:                     fmt.Sprint((config["name"])),
		SkipHashCheck:            false,
		MaxFetchers:              b.config.RestorationConfig.MaxFetchers,
		MaxCallSendMsgSize:       b.config.RestorationConfig.MaxCallSendMsgSize,
		MaxRequestBytes:          b.config.RestorationConfig.MaxRequestBytes,
		MaxTxnOps:                b.config.RestorationConfig.MaxTxnOps,
		EmbeddedEtcdQuotaBytes:   embeddedEtcdQuotaBytes,
		AutoCompactionMode:       fmt.Sprint(config["auto-compaction-mode"]),
		AutoCompactionRetention:  fmt.Sprint(config["auto-compaction-retention"]),
		ListenClientUrls:         pointer.StringPtr(lCurl),
		ListenPeerUrls:           pointer.StringPtr(lPurl),
	}

	initialClusterMap, err := types.NewURLsMap(fmt.Sprint(config["initial-cluster"]))
	if err != nil {
		b.logger.Fatal("Please provide initial cluster value for embedded ETCD")
	}

	if protocol == "" {
		protocol = defaultRestoreProtocol
	}

	var initialCluster string = ""
	for name, url := range initialClusterMap {
		initialCluster = initialCluster + fmt.Sprintf("%s=%s://%s:%s,", name, protocol, url[0].Hostname(), peerPort)
	}
	initialCluster = strings.Trim(initialCluster, ",")
	b.logger.Infof("Initial cluster for restore cluster is: %s", initialCluster)

	restoreConfig.InitialCluster = initialCluster
	clusterURLsMap, err := types.NewURLsMap(initialCluster)
	if err != nil {
		// Ideally this case should not occur, since this check is done at the config validations.
		b.logger.Fatalf("failed creating url map for restore cluster: %v", err)
		return err
	}

	if clientTLSInfo != nil {
		restoreConfig.ClientTLSInfo = clientTLSInfo
		restoreConfig.ClientAutoTLS = clientAutoTLS
	}

	if peerTLSInfo != nil {
		restoreConfig.PeerTLSInfo = peerTLSInfo
		restoreConfig.PeerAutoTLS = peerAutoTLS
	}

	options := &brtypes.RestoreOptions{
		Config:                  restoreConfig,
		ClusterURLs:             clusterURLsMap,
		PeerURLs:                peerURLs,
		ClusterRestoreLeaseName: brtypes.DefaultRestorationIndicatorLease,
	}

	if b.config.SnapstoreConfig == nil || len(b.config.SnapstoreConfig.Provider) == 0 {
		b.logger.Warnf("No snapstore storage provider configured. Will not start backup schedule.")
		runServerWithSnapshotter = false
	}
	return b.runServer(ctx, options)
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
	etcdInitializer := initializer.NewInitializer(restoreOpts, snapstoreConfig, b.logger.Logger)

	handler := b.startHTTPServer(etcdInitializer, b.config.SnapstoreConfig.Provider, b.config.EtcdConnectionConfig, nil)
	defer handler.Stop()

	leaderCallbacks := &brtypes.LeaderCallbacks{
		OnStartedLeading: func(leCtx context.Context) {
			ssrStopCh = make(chan struct{})
			var err error
			var defragCallBack defragmentor.CallbackFunc
			if runServerWithSnapshotter {
				b.logger.Infof("Creating snapstore from provider: %s", b.config.SnapstoreConfig.Provider)
				ss, err = snapstore.GetSnapstore(b.config.SnapstoreConfig)
				if err != nil {
					b.logger.Errorf("failed to create snapstore from configured storage provider: %v", err)
					return
				}

				// Get the new snapshotter object
				b.logger.Infof("Creating snapshotter...")
				ssr, err = snapshotter.NewSnapshotter(b.logger, b.config.SnapshotterConfig, ss, b.config.EtcdConnectionConfig, b.config.CompressionConfig, b.config.HealthConfig, b.config.SnapstoreConfig)
				if err != nil {
					b.logger.Errorf("failed to create new Snapshotter object: %v", err)
					return
				}

				// set "http handler" with the latest snapshotter object
				handler.SetSnapshotter(ssr)
				defragCallBack = ssr.TriggerFullSnapshot
				go handleSsrStopRequest(leCtx, handler, ssr, ackCh, ssrStopCh)
			}
			go b.resetRestorationIndicatorLease(leCtx, b.config.EtcdConnectionConfig, restoreOpts)
			go b.runEtcdProbeLoopWithSnapshotter(leCtx, handler, ssr, ss, ssrStopCh, ackCh)
			go defragmentor.DefragDataPeriodically(leCtx, b.config.EtcdConnectionConfig, b.defragmentationSchedule, defragCallBack, b.logger)
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
				go heartbeat.RenewMemberLeasePeriodically(ctx, mmStopCh, b.config.HealthConfig, b.logger, b.config.EtcdConnectionConfig)
			}
		},
		StopLeaseRenewal: func() {
			if b.config.HealthConfig.MemberLeaseRenewalEnabled {
				mmStopCh <- emptyStruct
			}
		},
	}

	checkLeadershipFunc := leaderelection.IsLeader

	b.logger.Infof("Creating leaderElector...")
	le, err := leaderelection.NewLeaderElector(b.logger, b.config.EtcdConnectionConfig, b.config.LeaderElectionConfig, leaderCallbacks, memberLeaseCallbacks, checkLeadershipFunc)
	if err != nil {
		return err
	}

	if runServerWithSnapshotter {
		go handleAckState(handler, ackCh)
	}

	//TODO @aaronfern: Add functionality for member garbage collection
	if b.config.HealthConfig.MemberLeaseRenewalEnabled {
		go heartbeat.RenewMemberLeasePeriodically(ctx, mmStopCh, b.config.HealthConfig, b.logger, b.config.EtcdConnectionConfig)
	}

	return le.Run(ctx)
}

// runEtcdProbeLoopWithSnapshotter runs the etcd probe loop
// for the case when backup-restore becomes leading sidecar.
func (b *BackupRestoreServer) runEtcdProbeLoopWithSnapshotter(ctx context.Context, handler *HTTPHandler, ssr *snapshotter.Snapshotter, ss brtypes.SnapStore, ssrStopCh chan struct{}, ackCh chan struct{}) {
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

				// If the owner check fails or returns false, kill the etcd process
				// to ensure that any open connections from kube-apiserver are terminated
				result, err := b.ownerChecker.Check(ctx)
				if err != nil {
					b.logger.Errorf("ownerChecker check fails: %v", err)
				} else if !result {
					if _, err := b.etcdProcessKiller.Kill(ctx); err != nil {
						b.logger.Errorf("Could not kill etcd process: %v", err)
					}
				}
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
	/*ep, err := url.Parse(b.config.EtcdConnectionConfig.Endpoints[0])
	if err != nil {
		b.logger.Errorf("ETCD connection endpints could not be parsed : %v", err)
		return err
	}

	conn, err := net.DialTimeout("tcp", ep.Host, b.config.EtcdConnectionConfig.ConnectionTimeout.Duration)
	if err != nil {
		b.logger.Infof("Main ETCD endpoint %s is unreachable, error: %v", b.config.EtcdConnectionConfig.Endpoints[0], err)
	}
	defer conn.Close()
	b.logger.Info("Can't run leader elector because all ETCD instances have not restored yet. Wait more..")*/

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

// resetRestorationIndicatorLease resets the restoration indicator lease to the code UNRESTORED
func (b *BackupRestoreServer) resetRestorationIndicatorLease(ctx context.Context, etcdConnectionConfig *brtypes.EtcdConnectionConfig, options *brtypes.RestoreOptions) error {
	namespace, err := snapstore.GetEnvVarOrError(podNamespace)
	if err != nil {
		// the instance is run in a local test environment
		b.logger.Info("POD_NAMESPACE could not be obtained. Are you running this instance inside a test environment? Otherwise, restoration can be in inconsistent state")
		return nil
	}

	clientSet, err := miscellaneous.GetKubernetesClientSetOrError()
	if err != nil {
		return fmt.Errorf("failed to create clientset while resetting restoration indicator lease: %v", err)
	}

	if err := retry.OnError(retry.DefaultBackoff, func(err error) bool { return true }, func() error {
		b.logger.Info("Resetting restoration indicator")
		//Create etcd client maintenance to get etcd ID
		clientFactory := etcdutil.NewFactory(*b.config.EtcdConnectionConfig)
		etcdClient, err := clientFactory.NewCluster()
		if err != nil {
			return &errors.EtcdError{
				Message: fmt.Sprintf("Failed to create etcd cluster client while resetting restoration indicator lease: %v", err),
			}
		}
		defer etcdClient.Close()

		response, err := etcdClient.MemberList(ctx)
		if err != nil {
			return &errors.EtcdError{
				Message: fmt.Sprintf("Failed to get list of members in cluster while resetting restoration indicator lease: %v", err),
			}
		}

		members := response.Members

		// for each member of the cluster, check whether the client port is still as same as restoration port.
		// if client port of any member is still same as restoration port, retry resetting indicator lease.
		for _, member := range members {
			clientURL := member.GetClientURLs()[0]
			b.logger.Infof("The clientURl of member %v is : %v", member.GetName(), clientURL)

			// parse clientURl to obtain the port number
			parsed, err := url.Parse(clientURL)
			if err != nil {
				return &errors.EtcdError{
					Message: fmt.Sprintf("clientURL could not be parsed while resetting restoration indicator lease: %v", err),
				}
			}

			// if client port is same as restore client port then it is inferred that the embedded ETCD server is yet running
			// and main ETCD cluster is not setup yet. So, retry.
			if parsed.Port() == defaultRestoreClientPort {
				return &errors.EtcdError{
					Message: "embedded ETCD for restoration is still running. Retry again to reset the restoration indicator lease",
				}
			}
		}
		clusterRestoreLease := &v1.Lease{}
		if err := clientSet.Get(ctx, k8sClient.ObjectKey{
			Namespace: namespace,
			Name:      options.ClusterRestoreLeaseName,
		}, clusterRestoreLease); err != nil {
			return &errors.EtcdError{
				Message: fmt.Sprintf("indicator for cluster restoration could not be fetched while resetting the restoration indicator lease : %v", err),
			}
		}

		updateLease := clusterRestoreLease.DeepCopy()
		updateTime := time.Now()
		updateLease.Spec.RenewTime = &metav1.MicroTime{Time: updateTime}
		updateLease.Spec.HolderIdentity = pointer.StringPtr(strconv.Itoa(brtypes.UNRESTORED))
		b.logger.Infof("updating indicator for cluster restoration lease %s with holder identity UNRESTORED", options.ClusterRestoreLeaseName)

		if err := clientSet.Patch(ctx, updateLease, k8sClient.MergeFromWithOptions(clusterRestoreLease, &k8sClient.MergeFromWithOptimisticLock{})); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return fmt.Errorf("failed to to reset indicator for cluster restoration lease: %v", err)
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

// startHTTPServer creates and starts the HTTP handler
// with status 503 (Service Unavailable)
func (b *BackupRestoreServer) startHTTPServer(initializer initializer.Initializer, storageProvider string, etcdConfig *brtypes.EtcdConnectionConfig, ssr *snapshotter.Snapshotter) *HTTPHandler {
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
	}
	handler.SetStatus(http.StatusServiceUnavailable)
	b.logger.Info("Registering the http request handlers...")
	handler.RegisterHandler()
	b.logger.Info("Starting the http server...")
	go handler.Start()

	return handler
}
