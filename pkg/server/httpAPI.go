// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/http/pprof"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	etcdclient "github.com/gardener/etcd-backup-restore/pkg/etcdutil/client"
	"github.com/gardener/etcd-backup-restore/pkg/initializer"
	"github.com/gardener/etcd-backup-restore/pkg/initializer/validator"
	"github.com/gardener/etcd-backup-restore/pkg/member"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/snapshotter"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/yaml"
)

const (
	initializationStatusNew        = "New"
	initializationStatusProgress   = "Progress"
	initializationStatusSuccessful = "Successful"
	initializationStatusFailed     = "Failed"

	// serverReadHeaderTimeout is the timeout for reading the request headers, to avoid slowloris attacks.
	serverReadHeaderTimeout = 5 * time.Second
)

var emptyStruct struct{}

// HandlerAckState denotes the state the handler would be in after sending a stop request to the snapshotter.
type HandlerAckState int32

const (
	// HandlerAckDone is set when handler has been acknowledged of snapshotter termination.
	HandlerAckDone uint32 = 0
	// HandlerAckWaiting is set when handler starts waiting of snapshotter termination.
	HandlerAckWaiting uint32 = 1
)

// HandlerRequest represents the type of request handler makes to the snapshotter.
type HandlerRequest int

const (
	// HandlerSsrAbort is the HandlerRequest to the snapshotter to terminate the snapshot process.
	HandlerSsrAbort HandlerRequest = 0
)

// HTTPHandler is implementation to handle HTTP API exposed by server
type HTTPHandler struct {
	Initializer               initializer.Initializer
	Snapshotter               *snapshotter.Snapshotter
	EtcdConnectionConfig      *brtypes.EtcdConnectionConfig
	AckCh                     chan struct{}
	SnapstoreConfig           *brtypes.SnapstoreConfig
	server                    *http.Server
	Logger                    *logrus.Entry
	HTTPHandlerMutex          *sync.Mutex
	ReqCh                     chan struct{}
	StopCh                    chan struct{}
	StorageProvider           string
	initializationStatus      string
	ServerTLSCertFile         string
	ServerTLSKeyFile          string
	status                    int
	Port                      uint
	initializationStatusMutex sync.Mutex
	AckState                  uint32
	EnableTLS                 bool
	EnableProfiling           bool
}

func (h *HTTPHandler) OnStoppedLeading() {
	h.OnLeadershipChanged(false)
}

func (h *HTTPHandler) OnStartedLeading() {
	h.OnLeadershipChanged(true)
}

// healthCheck contains the HealthStatus of backup restore.
type healthCheck struct {
	HealthStatus bool `json:"health"`
}

// GetStatus returns the current status in the HTTPHandler
func (h *HTTPHandler) GetStatus() int {
	return h.status
}

// SetStatus sets the current status in the HTTPHandler
func (h *HTTPHandler) SetStatus(status int) {
	if h.Logger != nil {
		h.Logger.Infof("Setting status to : %d", status)
	}
	h.status = status
}

// SetSnapshotterToNil sets the current HTTPHandler.Snapshotter to Nil in the HTTPHandler.
func (h *HTTPHandler) SetSnapshotterToNil() {
	h.HTTPHandlerMutex.Lock()
	defer h.HTTPHandlerMutex.Unlock()
	h.Snapshotter = nil
}

// SetSnapshotter sets the current HTTPHandler.Snapshotter in the HTTPHandler.
func (h *HTTPHandler) SetSnapshotter(ssr *snapshotter.Snapshotter) {
	h.HTTPHandlerMutex.Lock()
	defer h.HTTPHandlerMutex.Unlock()
	h.Snapshotter = ssr
}

// RegisterHandler registers the handler for different requests
func (h *HTTPHandler) RegisterHandler() {
	mux := http.NewServeMux()
	if h.EnableProfiling {
		registerPProfHandler(mux)
	}

	h.initializationStatus = "New"
	mux.HandleFunc("/initialization/start", h.serveInitialize)
	mux.HandleFunc("/initialization/status", h.serveInitializationStatus)
	mux.HandleFunc("/snapshot/full", h.serveFullSnapshotTrigger)
	mux.HandleFunc("/snapshot/delta", h.serveDeltaSnapshotTrigger)
	mux.HandleFunc("/snapshot/latest", h.serveLatestSnapshotMetadata)
	mux.HandleFunc("/snapshot/reencrypt", h.serveSnapshotsReencrypt)
	mux.HandleFunc("/snapshot/encstatus", h.serveSnapshotsEncryptionStatus)
	mux.HandleFunc("/snapshot/scan", h.serveSnapshotsScan)
	mux.HandleFunc("/config", h.serveConfig)
	mux.HandleFunc("/healthz", h.serveHealthz)
	mux.Handle("/metrics", promhttp.Handler())

	h.server = &http.Server{
		Addr:              fmt.Sprintf(":%d", h.Port),
		Handler:           mux,
		ReadHeaderTimeout: serverReadHeaderTimeout,
	}
}

// registerPProfHandler registers the PProf handler for profiling.
func registerPProfHandler(mux *http.ServeMux) {
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/heap", pprof.Handler("heap").ServeHTTP)
	mux.HandleFunc("/debug/pprof/goroutine", pprof.Handler("goroutine").ServeHTTP)
	mux.HandleFunc("/debug/pprof/threadcreate", pprof.Handler("threadcreate").ServeHTTP)
	mux.HandleFunc("/debug/pprof/block", pprof.Handler("block").ServeHTTP)
	mux.HandleFunc("/debug/pprof/mutex", pprof.Handler("mutex").ServeHTTP)
}

// checkAndSetSecurityHeaders serves the health status of the server
func (h *HTTPHandler) checkAndSetSecurityHeaders(rw http.ResponseWriter) {
	if h.EnableTLS {
		rw.Header().Set("Strict-Transport-Security", "max-age=31536000")
		rw.Header().Set("Content-Security-Policy", "default-src 'self'")
	}
}

// Start starts the http server to listen for request
func (h *HTTPHandler) Start() {
	h.Logger.Infof("Starting HTTP server at addr: %s", h.server.Addr)

	if !h.EnableTLS {
		err := h.server.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			h.Logger.Fatalf("Failed to start http server: %v", err)
		}
		h.Logger.Infof("HTTP server closed gracefully.")
		return
	}

	h.Logger.Infof("TLS enabled. Starting HTTPS server.")

	err := h.server.ListenAndServeTLS(h.ServerTLSCertFile, h.ServerTLSKeyFile)
	if err != nil && err != http.ErrServerClosed {
		h.Logger.Fatalf("Failed to start HTTPS server: %v", err)
	}
	h.Logger.Infof("HTTPS server closed gracefully.")
}

// Stop stops the http server
func (h *HTTPHandler) Stop() error {
	return h.server.Close()
}

// serveHealthz serves the health status of the server
func (h *HTTPHandler) serveHealthz(rw http.ResponseWriter, _ *http.Request) {
	h.checkAndSetSecurityHeaders(rw)
	rw.WriteHeader(h.GetStatus())
	healthCheck := &healthCheck{
		HealthStatus: func() bool {
			return h.GetStatus() == http.StatusOK
		}(),
	}
	out, err := json.Marshal(healthCheck)
	if err != nil {
		h.Logger.Errorf("Unable to marshal health status to json: %v", err)
		return
	}
	if _, err = rw.Write(out); err != nil {
		h.Logger.Errorf("Unable to write health status response: %v", err)
	}
}

// serveInitialize starts initialization for the configured Initializer
func (h *HTTPHandler) serveInitialize(rw http.ResponseWriter, req *http.Request) {
	h.checkAndSetSecurityHeaders(rw)
	h.Logger.Info("Received start initialization request.")
	h.initializationStatusMutex.Lock()
	defer h.initializationStatusMutex.Unlock()
	if h.initializationStatus == initializationStatusNew {
		h.Logger.Infof("Updating status from %s to %s", h.initializationStatus, initializationStatusProgress)
		h.initializationStatus = initializationStatusProgress
		go func() {
			var mode validator.Mode

			h.SetStatus(http.StatusServiceUnavailable)

			failBelowRevisionStr := req.URL.Query().Get("failbelowrevision")
			h.Logger.Infof("Validation failBelowRevision: %s", failBelowRevisionStr)
			var failBelowRevision int64
			if len(failBelowRevisionStr) != 0 {
				var err error
				failBelowRevision, err = strconv.ParseInt(failBelowRevisionStr, 10, 64)
				if err != nil {
					h.initializationStatusMutex.Lock()
					defer h.initializationStatusMutex.Unlock()
					h.Logger.Errorf("Failed initialization due wrong parameter value `failbelowrevision`: %v", err)
					h.initializationStatus = initializationStatusFailed
					return
				}
			}
			switch modeVal := req.URL.Query().Get("mode"); modeVal {
			case string(validator.Full):
				mode = validator.Full
			case string(validator.Sanity):
				mode = validator.Sanity
			default:
				mode = validator.Full
			}
			h.Logger.Infof("Validation mode: %s", mode)
			err := h.Initializer.Initialize(mode, failBelowRevision)
			h.initializationStatusMutex.Lock()
			defer h.initializationStatusMutex.Unlock()
			if err != nil {
				h.Logger.Errorf("Failed initialization: %v", err)
				h.initializationStatus = initializationStatusFailed
				return
			}
			h.Logger.Info("Successfully initialized data directory for etcd.")
			h.initializationStatus = initializationStatusSuccessful
		}()
	}
	rw.WriteHeader(http.StatusOK)
}

// serveInitializationStatus serves the etcd initialization progress status
func (h *HTTPHandler) serveInitializationStatus(rw http.ResponseWriter, _ *http.Request) {
	h.checkAndSetSecurityHeaders(rw)
	h.initializationStatusMutex.Lock()
	defer h.initializationStatusMutex.Unlock()
	h.Logger.Infof("Responding to status request with: %s", h.initializationStatus)

	rw.WriteHeader(http.StatusOK)

	if _, err := rw.Write([]byte(h.initializationStatus)); err != nil {
		h.Logger.Errorf("Unable to write latest snapshot metadata response: %v", err)
		return
	}

	if h.initializationStatus == initializationStatusSuccessful || h.initializationStatus == initializationStatusFailed {
		h.Logger.Infof("Updating status from %s to %s", h.initializationStatus, initializationStatusNew)
		h.initializationStatus = initializationStatusNew
	}
}

// serveFullSnapshotTrigger triggers an out-of-schedule full snapshot
// for the configured Snapshotter
func (h *HTTPHandler) serveFullSnapshotTrigger(rw http.ResponseWriter, req *http.Request) {
	h.checkAndSetSecurityHeaders(rw)
	if h.Snapshotter == nil {
		if len(h.StorageProvider) > 0 {
			h.Logger.Info("Fowarding the request to take out-of-schedule full snapshot to backup-restore leader")
			h.delegateReqToLeader(rw, req)
			return
		}
		h.Logger.Warnf("Ignoring out-of-schedule full snapshot request as snapshotter is not configured")
		rw.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	isFinal := false
	if finalValue := req.URL.Query().Get("final"); finalValue != "" {
		var err error
		isFinal, err = strconv.ParseBool(finalValue)
		if err != nil {
			h.Logger.Warnf("Could not parse request parameter 'final' to bool: %v", err)
			rw.WriteHeader(http.StatusBadRequest)
			return
		}
	}
	s, err := h.Snapshotter.TriggerFullSnapshot(req.Context(), isFinal)
	if err != nil {
		h.Logger.Warnf("Skipped triggering out-of-schedule full snapshot: %v", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	out, err := json.Marshal(s)
	if err != nil {
		h.Logger.Warnf("Unable to marshal out-of-schedule full snapshot to json: %v", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	rw.WriteHeader(http.StatusOK)
	if _, err = rw.Write(out); err != nil {
		h.Logger.Errorf("Unable to write latest snapshot metadata response: %v", err)
	}
}

// serveDeltaSnapshotTrigger triggers an out-of-schedule delta snapshot
// for the configured Snapshotter
func (h *HTTPHandler) serveDeltaSnapshotTrigger(rw http.ResponseWriter, req *http.Request) {
	h.checkAndSetSecurityHeaders(rw)
	if h.Snapshotter == nil {
		if len(h.StorageProvider) > 0 {
			h.Logger.Info("Fowarding the request to take out-of-schedule delta snapshot to backup-restore leader")
			h.delegateReqToLeader(rw, req)
			return
		}
		h.Logger.Warnf("Ignoring out-of-schedule delta snapshot request as snapshotter is not configured")
		rw.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	s, err := h.Snapshotter.TriggerDeltaSnapshot()
	if err != nil {
		h.Logger.Warnf("Skipped triggering out-of-schedule delta snapshot: %v", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	out, err := json.Marshal(s)
	if err != nil {
		h.Logger.Warnf("Unable to marshal out-of-schedule delta snapshot to json: %v", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	rw.WriteHeader(http.StatusOK)
	if _, err = rw.Write(out); err != nil {
		h.Logger.Errorf("Unable to write latest snapshot metadata response: %v", err)
	}
}

func (h *HTTPHandler) serveLatestSnapshotMetadata(rw http.ResponseWriter, req *http.Request) {
	h.checkAndSetSecurityHeaders(rw)
	if h.Snapshotter == nil {
		if len(h.StorageProvider) > 0 {
			h.Logger.Info("Fowarding the request of latest snapshot metadata to backup-restore leader")
			h.delegateReqToLeader(rw, req)
			return
		}
		h.Logger.Warnf("Ignoring latest snapshot metadata request as snapshotter is not configured")
		rw.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	store, err := snapstore.GetSnapstore(h.SnapstoreConfig)
	if err != nil {
		h.Logger.Warnf("Unable to create snapstore from configured storage provider: %v", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	fullSnap, deltaSnaps, err := miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
	if err != nil {
		h.Logger.Warnf("Unable to fetch latest snapshots from snapstore: %v", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	resp := latestSnapshotMetadataResponse{
		FullSnapshot:   fullSnap,
		DeltaSnapshots: deltaSnaps,
	}

	out, err := json.Marshal(resp)
	if err != nil {
		h.Logger.Warnf("Unable to marshal latest snapshot metadata response to json: %v", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	rw.WriteHeader(http.StatusOK)
	if _, err = rw.Write(out); err != nil {
		h.Logger.Errorf("Unable to write latest snapshot metadata response: %v", err)
	}
}

func (h *HTTPHandler) serveConfig(rw http.ResponseWriter, req *http.Request) {
	inputFileName := miscellaneous.EtcdConfigFilePath
	dir, err := os.UserHomeDir()
	if err != nil {
		h.Logger.Warnf("Unable to get user home dir: %v", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	outputFileName := filepath.Join(dir, "etcd.conf.yaml")
	configYML, err := os.ReadFile(inputFileName) // #nosec G304 -- this is a trusted etcd config file.
	if err != nil {
		h.Logger.Warnf("Unable to read etcd config file: %v", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	config := map[string]interface{}{}
	if err := yaml.Unmarshal([]byte(configYML), &config); err != nil {
		h.Logger.Warnf("Unable to unmarshal etcd config yaml file: %v", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	// fetch pod name from env
	podNS := os.Getenv("POD_NAMESPACE")
	podName := os.Getenv("POD_NAME")

	config["name"] = podName

	// fetch initial-advertise-peer-urls from etcd config file
	initAdPeerURLs, err := miscellaneous.GetMemberPeerURLs(inputFileName)
	if err != nil {
		h.Logger.Warnf("Unable to get initial-advertise-peer-urls from etcd config file: %v", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	config["initial-advertise-peer-urls"] = strings.Join(initAdPeerURLs, ",")

	// fetch advertise-client-urls from etcd config file
	advClientURLs, err := miscellaneous.GetMemberClientURLs(inputFileName)
	if err != nil {
		h.Logger.Warnf("Unable to get advertise-client-urls from etcd config file: %v", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	config["advertise-client-urls"] = strings.Join(advClientURLs, ",")

	config["initial-cluster"] = getInitialCluster(req.Context(), fmt.Sprint(config["initial-cluster"]), *h.EtcdConnectionConfig, *h.Logger, podName)

	clusterSize, err := miscellaneous.GetClusterSize(fmt.Sprint(config["initial-cluster"]))
	if err != nil {
		h.Logger.Warnf("Unable to determine the cluster size: %v", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	state, err := h.getClusterState(req.Context(), clusterSize, podName, podNS)
	if err != nil {
		h.Logger.Warnf("failed to get cluster state %v", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	config["initial-cluster-state"] = state

	data, err := yaml.Marshal(&config)
	if err != nil {
		h.Logger.Warnf("Unable to marshal data to etcd config yaml file: %v", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	if err := os.WriteFile(outputFileName, data, 0600); err != nil {
		h.Logger.Warnf("Unable to write etcd config file: %v", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	http.ServeFile(rw, req, outputFileName)

	h.Logger.Info("Served config for ETCD instance.")
}

// getClusterState returns the Cluster state either `new` or `existing`.
func (h *HTTPHandler) getClusterState(ctx context.Context, clusterSize int, podName string, podNS string) (string, error) {
	if clusterSize == 1 {
		return miscellaneous.ClusterStateNew, nil
	}

	client, err := miscellaneous.GetKubernetesClientSetOrError()
	if err != nil {
		h.Logger.Warnf("Failed to create clientset: %v", err)
		return "", fmt.Errorf("failed to get clusterState: %w", err)
	}

	// clusterSize > 1
	state, err := miscellaneous.GetInitialClusterStateIfScaleup(ctx, *h.Logger, client, podName, podNS)
	if err != nil {
		return "", err
	}

	if state == nil {
		// Not a Scale-up scenario.
		// Either a multi-node bootstrap or a restoration of single member in multi-node.
		m := member.NewMemberControl(h.EtcdConnectionConfig)

		// check whether a learner is present in the cluster
		// if a learner is present then return `ClusterStateExisting` else `ClusterStateNew`.
		if present, err := m.IsLearnerPresent(ctx); present && err == nil {
			return miscellaneous.ClusterStateExisting, nil
		}
		return miscellaneous.ClusterStateNew, nil

	}

	return *state, nil
}

func getInitialCluster(ctx context.Context, initialCluster string, etcdConn brtypes.EtcdConnectionConfig, logger logrus.Entry, podName string) string {
	// INITIAL_CLUSTER served via the etcd config must be tailored to the number of members in the cluster at that point. Else etcd complains with error "member count is unequal"
	// One reason why we might want to have a strict ordering when members are joining the cluster
	// addmember subcommand achieves this by making sure the pod with the previous index is running before attempting to add itself as a learner

	// We want to use the service endpoint since we're only supposed to connect to ready etcd members.
	clientFactory := etcdutil.NewFactory(etcdConn, etcdclient.UseServiceEndpoints(true))
	cli, err := clientFactory.NewCluster()
	if err != nil {
		logger.Warnf("Error with NewCluster() : %v", err)
		return initialCluster
	}
	defer func() {
		if err := cli.Close(); err != nil {
			logger.Errorf("Error closing etcd cluster client: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(ctx, member.EtcdTimeout)
	defer cancel()
	var memList *clientv3.MemberListResponse

	backoff := retry.DefaultBackoff
	backoff.Steps = 2
	backoff.Duration = member.RetryPeriod
	err = retry.OnError(backoff, func(err error) bool {
		return err != nil
	}, func() error {
		memList, err = cli.MemberList(ctx)
		return err
	})
	noOfMembers := 0
	if err != nil {
		logger.Warnf("Could not list members : %v", err)
	} else {
		noOfMembers = len(memList.Members)
	}

	//If no members present or a single node cluster, consider as case of first member bootstraping. No need to edit INITIAL_CLUSTER in that case
	if noOfMembers > 1 {
		initialcluster := strings.Split(initialCluster, ",")

		membrs := make(map[string]bool)
		for _, y := range memList.Members {
			membrs[y.Name] = true
		}
		cluster := ""
		for _, y := range initialcluster {
			// Add member to `initial-cluster` only if already a cluster member
			z := strings.Split(y, "=")
			if membrs[z[0]] || z[0] == podName {
				cluster = cluster + y + ","
			}
		}
		//remove trailing `,`
		initialCluster = cluster[:len(cluster)-1]
	}

	return initialCluster
}

// delegateReqToLeader forwards the incoming http/https request to BackupLeader.
func (h *HTTPHandler) delegateReqToLeader(rw http.ResponseWriter, req *http.Request) {
	// Get the BackupLeader URL
	// Get the ReverseProxy object
	// Delegate the http request to BackupLeader using the reverse proxy.

	factory := etcdutil.NewFactory(*h.EtcdConnectionConfig)
	clientMaintenance, err := factory.NewMaintenance()
	if err != nil {
		h.Logger.Warnf("failed to create etcd maintenance client:  %v", err)
		rw.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	defer func() {
		if err := clientMaintenance.Close(); err != nil {
			h.Logger.Errorf("Error closing etcd maintenance client: %v", err)
		}
	}()

	cl, err := factory.NewCluster()
	if err != nil {
		h.Logger.Warnf("failed to create etcd cluster client:  %v", err)
		rw.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	defer func() {
		if err := cl.Close(); err != nil {
			h.Logger.Errorf("Error closing etcd cluster client: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(req.Context(), h.EtcdConnectionConfig.ConnectionTimeout.Duration)
	defer cancel()

	if len(h.EtcdConnectionConfig.Endpoints) == 0 {
		h.Logger.Warnf("etcd endpoints are not passed correctly")
		rw.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	_, etcdLeaderEndPoint, err := miscellaneous.GetLeader(ctx, clientMaintenance, cl, h.EtcdConnectionConfig.Endpoints[0])
	if err != nil {
		h.Logger.Warnf("Unable to get the etcd leader endpoint: %v", err)
		rw.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	backupLeaderEndPoint, err := miscellaneous.GetBackupLeaderEndPoint(etcdLeaderEndPoint, h.Port)
	if err != nil {
		h.Logger.Warnf("Unable to get the backup leader endpoint: %v", err)
		rw.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	backupLeaderURL, err := url.Parse(backupLeaderEndPoint)
	if err != nil {
		h.Logger.Warnf("Unable to parse backup leader endpoint: %v", err)
		rw.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	isHealthy, err := IsBackupRestoreHealthy(backupLeaderEndPoint+"/healthz", h.EnableTLS, h.EtcdConnectionConfig.CaFile)
	if err != nil {
		h.Logger.Warnf("Unable to check backup leader health: %v", err)
		rw.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// when backup-leader is not in a healthy state.
	if !isHealthy {
		h.Logger.Warnf("backup leader is not healthy: %v", err)
		rw.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	h.Logger.Infof("backup-restore leader with url [%v] is healthy", backupLeaderURL)

	// create the reverse Proxy
	revProxyHandler := httputil.NewSingleHostReverseProxy(backupLeaderURL)

	if h.EnableTLS {
		caCertPool := x509.NewCertPool()

		caCert, err := os.ReadFile(h.EtcdConnectionConfig.CaFile)
		if err != nil {
			return
		}
		caCertPool.AppendCertsFromPEM(caCert)
		revProxyHandler.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{ // #nosec G402 -- TLSClientConfig.MinVersion=1.2 by default.
				RootCAs: caCertPool,
			},
		}
	}

	revProxyHandler.ServeHTTP(rw, req)
}

// IsBackupRestoreHealthy checks whether the backup-restore of given backup-restore URL is healthy or not.
func IsBackupRestoreHealthy(backupRestoreURL string, TLSEnabled bool, rootCA string) (healthy bool, err error) {
	var health healthCheck
	cl := &http.Client{}

	if TLSEnabled {
		caCertPool := x509.NewCertPool()

		caCert, err := os.ReadFile(rootCA) // #nosec G304 -- this is a trusted root CA file.
		if err != nil {
			return false, err
		}
		caCertPool.AppendCertsFromPEM(caCert)

		cl.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{ // #nosec G402 -- TLSClientConfig.MinVersion=1.2 by default.
				RootCAs: caCertPool,
			},
		}
	}

	response, err := cl.Get(backupRestoreURL)
	if err != nil {
		return false, err
	}
	defer func() {
		if err1 := response.Body.Close(); err1 != nil {
			err = errors.Join(err, fmt.Errorf("failed to close response body: %v", err1))
		}
	}()

	responseData, err := io.ReadAll(response.Body)
	if err != nil {
		return false, err
	}

	if err := json.Unmarshal(responseData, &health); err != nil {
		return false, err
	}
	return health.HealthStatus, nil
}

func (h *HTTPHandler) serveSnapshotsReencrypt(rw http.ResponseWriter, req *http.Request) {
	if h.Snapshotter == nil {
		if len(h.StorageProvider) > 0 {
			h.delegateReqToLeader(rw, req)
			return
		}
		h.Logger.Warnf("Ignoring re-encryption request as snapshotter is not configured")
		return
	}

	h.checkAndSetSecurityHeaders(rw)
	h.Logger.Info("Received request to re-encrypt all snapshots.")

	store, err := snapstore.GetSnapstore(h.SnapstoreConfig)
	if err != nil {
		h.Logger.Errorf("Unable to create snapstore for re-encryption: %v", err)
		return
	}
	switch s := store.(type) {
	case *snapstore.S3SnapStore:
		go func() {
			err := s.ReencryptAllSnapshots(h.Logger)
			if err != nil {
				h.Logger.Errorf("Failed to re-encrypt snapshots: %v", err)
			} else {
				h.Logger.Info("Re-encryption of all snapshots completed successfully.")
			}
		}()
		rw.WriteHeader(http.StatusAccepted)
		_, err := rw.Write([]byte("Re-encryption of all snapshots started.\n"))
		if err != nil {
			h.Logger.Errorf("Failed to write response: %v", err)
		}
	default:
		h.Logger.Errorf("Re-encryption is only supported for S3SnapStore")
		return
	}
}

// Update serveSnapshotsEncryptionStatus to return structured JSON
func (h *HTTPHandler) serveSnapshotsEncryptionStatus(rw http.ResponseWriter, req *http.Request) {
	if h.Snapshotter == nil {
		if len(h.StorageProvider) > 0 {
			h.delegateReqToLeader(rw, req)
			return
		}
		h.Logger.Warnf("Ignoring encryption status request as snapshotter is not configured")
		return
	}
	h.checkAndSetSecurityHeaders(rw)
	h.Logger.Info("Received request for SSE-C key usage status.")
	if h.Snapshotter == nil || len(h.StorageProvider) == 0 {
		h.Logger.Errorf("This request can only be served by leader.")
		rw.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	store := h.Snapshotter.GetStore()
	switch s := store.(type) {
	case *snapstore.S3SnapStore:
		usage, upToDate := s.GetSSEKeyUsageWithStatus()
		response := map[string]interface{}{
			"upToDate": upToDate,
			"files":    usage,
		}
		rw.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(rw).Encode(response); err != nil {
			h.Logger.Errorf("Failed to encode SSE key usage: %v", err)
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}
	default:
		h.Logger.Errorf("Encryption status is only supported for S3SnapStore")
		rw.WriteHeader(http.StatusNotImplemented)
		return
	}
}

func (h *HTTPHandler) serveSnapshotsScan(rw http.ResponseWriter, req *http.Request) {
	if h.Snapshotter == nil {
		if len(h.StorageProvider) > 0 {
			h.delegateReqToLeader(rw, req)
			return
		}
		h.Logger.Warnf("Ignoring snapshot scan request as snapshotter is not configured")
		return
	}
	h.checkAndSetSecurityHeaders(rw)
	h.Logger.Info("Received request to scan all snapshots for encryption status.")

	go func() {
		store := h.Snapshotter.GetStore()
		switch s := store.(type) {
		case *snapstore.S3SnapStore:
			err := s.ScanAllSnapshots(h.Logger)
			if err != nil {
				h.Logger.Errorf("Failed to scan snapshots: %v", err)
				return
			}
			h.Logger.Info("Snapshot scan completed successfully.")
		default:
			h.Logger.Errorf("Snapshot scanning is only supported for S3SnapStore")
			return
		}
	}()
	rw.WriteHeader(http.StatusAccepted)

	if _, err := rw.Write([]byte("Snapshot scan started.\n")); err != nil {
		h.Logger.Errorf("Failed to write response: %v", err)
		return
	}
}

// Add this method to your HTTPHandler or wherever you handle leadership changes
func (h *HTTPHandler) OnLeadershipChanged(isLeader bool) {
	if !isLeader {
		// We lost leadership, clear cached SSE key usage data
		h.Logger.Info("Lost leadership, clearing SSE key usage cache")
		if h.Snapshotter != nil && h.Snapshotter.GetStore() != nil {
			switch s := h.Snapshotter.GetStore().(type) {
			case *snapstore.S3SnapStore:
				s.ClearSSEKeyUsageData()
			}
		}
	} else {
		h.Logger.Info("Gained leadership")
	}
}
