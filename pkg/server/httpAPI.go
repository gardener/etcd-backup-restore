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
// limitations under the License

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"net/http/pprof"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	"github.com/gardener/etcd-backup-restore/pkg/etcdutil/client"
	"github.com/gardener/etcd-backup-restore/pkg/initializer"
	"github.com/gardener/etcd-backup-restore/pkg/initializer/validator"
	"github.com/gardener/etcd-backup-restore/pkg/member"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/snapshotter"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/ghodss/yaml"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
	"k8s.io/client-go/util/retry"
)

const (
	initializationStatusNew        = "New"
	initializationStatusProgress   = "Progress"
	initializationStatusSuccessful = "Successful"
	initializationStatusFailed     = "Failed"
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
	StorageProvider           string
	Port                      uint
	server                    *http.Server
	Logger                    *logrus.Entry
	initializationStatusMutex sync.Mutex
	AckState                  uint32
	initializationStatus      string
	status                    int
	StopCh                    chan struct{}
	EnableProfiling           bool
	ReqCh                     chan struct{}
	AckCh                     chan struct{}
	EnableTLS                 bool
	ServerTLSCertFile         string
	ServerTLSKeyFile          string
	HTTPHandlerMutex          *sync.Mutex
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
	mux.HandleFunc("/config", h.serveConfig)
	mux.HandleFunc("/healthz", h.serveHealthz)
	mux.Handle("/metrics", promhttp.Handler())

	h.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", h.Port),
		Handler: mux,
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
func (h *HTTPHandler) serveHealthz(rw http.ResponseWriter, req *http.Request) {
	h.checkAndSetSecurityHeaders(rw)
	rw.WriteHeader(h.GetStatus())
	healthCheck := &healthCheck{
		HealthStatus: func() bool {
			if h.GetStatus() == http.StatusOK {
				return true
			}
			return false
		}(),
	}
	json, err := json.Marshal(healthCheck)
	if err != nil {
		h.Logger.Errorf("Unable to marshal health status to json: %v", err)
		return
	}
	rw.Write([]byte(json))
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

			// This is needed to stop the currently running snapshotter.
			if h.Snapshotter != nil {
				h.SetStatus(http.StatusServiceUnavailable)
				atomic.StoreUint32(&h.AckState, HandlerAckWaiting)
				h.Logger.Info("Changed handler state.")
				h.ReqCh <- emptyStruct
				h.Logger.Info("Waiting for acknowledgment...")
				<-h.AckCh
			}

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
func (h *HTTPHandler) serveInitializationStatus(rw http.ResponseWriter, req *http.Request) {
	h.checkAndSetSecurityHeaders(rw)
	h.initializationStatusMutex.Lock()
	defer h.initializationStatusMutex.Unlock()
	h.Logger.Infof("Responding to status request with: %s", h.initializationStatus)

	rw.WriteHeader(http.StatusOK)

	rw.Write([]byte(h.initializationStatus))

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
	json, err := json.Marshal(s)
	if err != nil {
		h.Logger.Warnf("Unable to marshal out-of-schedule full snapshot to json: %v", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	rw.WriteHeader(http.StatusOK)
	rw.Write(json)
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

	json, err := json.Marshal(s)
	if err != nil {
		h.Logger.Warnf("Unable to marshal out-of-schedule delta snapshot to json: %v", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	rw.WriteHeader(http.StatusOK)
	rw.Write(json)
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

	resp := latestSnapshotMetadataResponse{
		FullSnapshot:   h.Snapshotter.PrevFullSnapshot,
		DeltaSnapshots: h.Snapshotter.PrevDeltaSnapshots,
	}

	json, err := json.Marshal(resp)
	if err != nil {
		h.Logger.Warnf("Unable to marshal latest snapshot metadata response to json: %v", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	rw.WriteHeader(http.StatusOK)
	rw.Write(json)
}

func (h *HTTPHandler) serveConfig(rw http.ResponseWriter, req *http.Request) {
	inputFileName := miscellaneous.EtcdConfigFilePath
	outputFileName := "/etc/etcd.conf.yaml"
	configYML, err := ioutil.ReadFile(inputFileName)
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

	clientSet, err := miscellaneous.GetKubernetesClientSetOrError()
	if err != nil {
		h.Logger.Warnf("Failed to create clientset: %v", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	config["name"] = podName

	initAdPeerURL := config["initial-advertise-peer-urls"]
	protocol, svcName, namespace, peerPort, err := parsePeerURL(fmt.Sprint(initAdPeerURL))
	if err != nil {
		h.Logger.Warnf("Unable to determine service name, namespace, peer port from advertise peer urls : %v", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	domaiName := fmt.Sprintf("%s.%s.%s", svcName, namespace, "svc")
	config["initial-advertise-peer-urls"] = fmt.Sprintf("%s://%s.%s:%s", protocol, podName, domaiName, peerPort)

	advClientURL := config["advertise-client-urls"]
	protocol, svcName, namespace, clientPort, err := parseAdvClientURL(fmt.Sprint(advClientURL))
	if err != nil {
		h.Logger.Warnf("Unable to determine service name, namespace, peer port from advertise client url : %v", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	domaiName = fmt.Sprintf("%s.%s.%s", svcName, namespace, "svc")
	config["advertise-client-urls"] = fmt.Sprintf("%s://%s.%s:%s", protocol, podName, domaiName, clientPort)

	config["initial-cluster-state"] = miscellaneous.GetInitialClusterState(req.Context(), *h.Logger, clientSet, podName, podNS)

	config["initial-cluster"] = getInitialCluster(req.Context(), fmt.Sprint(config["initial-cluster"]), *h.EtcdConnectionConfig, *h.Logger, podName)

	data, err := yaml.Marshal(&config)
	if err != nil {
		h.Logger.Warnf("Unable to marshal data to etcd config yaml file: %v", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	if err := ioutil.WriteFile(outputFileName, data, 0644); err != nil {
		h.Logger.Warnf("Unable to write etcd config file: %v", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	http.ServeFile(rw, req, outputFileName)

	h.Logger.Info("Served config for ETCD instance.")
}

func getInitialCluster(ctx context.Context, initialCluster string, etcdConn brtypes.EtcdConnectionConfig, logger logrus.Entry, podName string) string {
	// INITIAL_CLUSTER served via the etcd config must be tailored to the number of members in the cluster at that point. Else etcd complains with error "member count is unequal"
	// One reason why we might want to have a strict ordering when members are joining the cluster
	// addmember subcommand achieves this by making sure the pod with the previous index is running before attempting to add itself as a learner

	// We want to use the service endpoint since we're only supposed to connect to ready etcd members.
	clientFactory := etcdutil.NewFactory(etcdConn, client.UseServiceEndpoints(true))
	cli, err := clientFactory.NewCluster()
	if err != nil {
		logger.Warnf("Error with NewCluster() : %v", err)
		return initialCluster
	}
	defer cli.Close()

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

func parsePeerURL(peerURL string) (string, string, string, string, error) {
	tokens := strings.Split(peerURL, "@")
	if len(tokens) < 4 {
		return "", "", "", "", fmt.Errorf("total length of tokens is less than four")
	}
	return tokens[0], tokens[1], tokens[2], tokens[3], nil
}

func parseAdvClientURL(advClientURL string) (string, string, string, string, error) {
	tokens := strings.Split(advClientURL, "@")
	if len(tokens) < 4 {
		return "", "", "", "", fmt.Errorf("total length of tokens is less than four")
	}
	return tokens[0], tokens[1], tokens[2], tokens[3], nil
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
	defer clientMaintenance.Close()

	client, err := factory.NewCluster()
	if err != nil {
		h.Logger.Warnf("failed to create etcd cluster client:  %v", err)
		rw.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(req.Context(), h.EtcdConnectionConfig.ConnectionTimeout.Duration)
	defer cancel()

	if len(h.EtcdConnectionConfig.Endpoints) == 0 {
		h.Logger.Warnf("etcd endpoints are not passed correctly")
		rw.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	_, etcdLeaderEndPoint, err := miscellaneous.GetLeader(ctx, clientMaintenance, client, h.EtcdConnectionConfig.Endpoints[0])
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

	isHealthy, err := IsBackupRestoreHealthy(backupLeaderEndPoint + "/healthz")
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
	revProxyHandler := httputil.NewSingleHostReverseProxy(backupLeaderURL)
	revProxyHandler.ServeHTTP(rw, req)
	return
}

// IsBackupRestoreHealthy checks the whether the backup-restore of given backup-restore URL healthy or not.
func IsBackupRestoreHealthy(backupRestoreURL string) (bool, error) {
	var health healthCheck

	response, err := http.Get(backupRestoreURL)
	if err != nil {
		return false, err
	}
	defer response.Body.Close()

	responseData, err := io.ReadAll(response.Body)
	if err != nil {
		return false, err
	}

	if err := json.Unmarshal(responseData, &health); err != nil {
		return false, err
	}
	return health.HealthStatus, nil
}
