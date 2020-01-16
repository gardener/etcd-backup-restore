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
	"net/http"
	"net/http/pprof"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/gardener/etcd-backup-restore/pkg/initializer/validator"

	"github.com/gardener/etcd-backup-restore/pkg/initializer"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/snapshotter"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
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
	Port                      uint
	server                    *http.Server
	Logger                    *logrus.Entry
	initializationStatusMutex sync.Mutex
	AckState                  uint32
	initializationStatus      initializationStatus
	status                    int
	EnableProfiling           bool
	ReqCh                     chan struct{}
	AckCh                     chan struct{}
	EnableTLS                 bool
	ServerTLSCertFile         string
	ServerTLSKeyFile          string
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
	mux.HandleFunc("/healthz", h.serveHealthz)
	mux.Handle("/metrics", promhttp.Handler())

	h.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", h.Port),
		Handler: mux,
	}
	return
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
	rw.Write([]byte(fmt.Sprintf("{\"health\":%v}", h.GetStatus() == http.StatusOK)))
}

// serveInitialize starts initialization for the configured Initializer
func (h *HTTPHandler) serveInitialize(rw http.ResponseWriter, req *http.Request) {
	h.checkAndSetSecurityHeaders(rw)

	h.Logger.Info("Received start initialization request.")
	mode, failBelowRevision, err := parseInitializationRequestParameter(req)
	if err != nil {
		h.Logger.Errorf("failed to parse initialization request parameters: %v", err)
		rw.WriteHeader(http.StatusBadRequest)
		return
	}

	h.Logger.Infof("FailBelowRevision: %d", failBelowRevision)
	h.Logger.Infof("Validation mode: %s", mode)
	h.initializationStatusMutex.Lock()
	defer h.initializationStatusMutex.Unlock()
	if h.initializationStatus == initializationStatusNew {
		h.Logger.Infof("Updating status from %s to %s", h.initializationStatus, initializationStatusProgress)
		h.initializationStatus = initializationStatusProgress
		// Context passed here cannot be same as the req.Context() since this asynchronous call.
		go h.initializeInCoordinationWithSnapshotter(context.TODO(), mode, failBelowRevision)
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
		h.Logger.Warnf("Ignoring out-of-schedule full snapshot request as snapshotter is not configured")
		rw.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	s, err := h.Snapshotter.TriggerFullSnapshot(req.Context())
	if err != nil {
		h.Logger.Warnf("Skipped triggering out-of-schedule full snapshot: %v", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	json, err := json.Marshal(s)
	rw.WriteHeader(http.StatusOK)
	rw.Write(json)
}

// serveDeltaSnapshotTrigger triggers an out-of-schedule delta snapshot
// for the configured Snapshotter
func (h *HTTPHandler) serveDeltaSnapshotTrigger(rw http.ResponseWriter, req *http.Request) {
	h.checkAndSetSecurityHeaders(rw)
	if h.Snapshotter == nil {
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
	return
}

// Acknowledge acknowledges the handler if its waiting for acknowledgement.
func (h *HTTPHandler) Acknowledge() {
	if atomic.CompareAndSwapUint32(&h.AckState, HandlerAckWaiting, HandlerAckDone) {
		h.AckCh <- emptyStruct
	}
}

// parseInitializationRequestParameter parses the initializer request parameter
func parseInitializationRequestParameter(req *http.Request) (validator.Mode, int64, error) {
	var (
		failBelowRevision int64
		mode              validator.Mode
	)

	failBelowRevisionStr := req.URL.Query().Get("failbelowrevision")
	if len(failBelowRevisionStr) != 0 {
		var err error
		failBelowRevision, err = strconv.ParseInt(failBelowRevisionStr, 10, 64)
		if err != nil {
			return mode, failBelowRevision, fmt.Errorf("failed to parse `failbelowrevision` parameter: %v", err)
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

	return mode, failBelowRevision, nil
}

func (h *HTTPHandler) initializeInCoordinationWithSnapshotter(ctx context.Context, mode validator.Mode, failBelowRevision int64) {
	// This is needed to stop the currently running snapshotter.
	if h.Snapshotter != nil {
		atomic.StoreUint32(&h.AckState, HandlerAckWaiting)
		h.Logger.Info("Changed handler state.")
		h.ReqCh <- emptyStruct

		h.Logger.Info("Waiting for acknowledgment...")
		<-h.AckCh
	}

	err := h.Initializer.Initialize(ctx, mode, failBelowRevision)
	h.initializationStatusMutex.Lock()
	defer h.initializationStatusMutex.Unlock()
	if err != nil {
		h.Logger.Errorf("Failed initialization: %v", err)
		h.initializationStatus = initializationStatusFailed
		return
	}
	h.Logger.Info("Successfully initialized data directory for etcd.")
	h.initializationStatus = initializationStatusSuccessful
}
