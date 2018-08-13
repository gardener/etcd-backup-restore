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
	"fmt"
	"net/http"
	"net/http/pprof"
	"sync"
	"sync/atomic"

	"github.com/gardener/etcd-backup-restore/pkg/initializer"
	"github.com/sirupsen/logrus"
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
	EtcdInitializer           initializer.EtcdInitializer
	Port                      int
	server                    *http.Server
	Logger                    *logrus.Logger
	initializationStatusMutex sync.Mutex
	AckState                  uint32
	initializationStatus      string
	Status                    int
	StopCh                    chan struct{}
	EnableProfiling           bool
	ReqCh                     chan struct{}
	AckCh                     chan struct{}
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
	mux.HandleFunc("/healthz", h.serveHealthz)

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

// Start start the http server to listen for request
func (h *HTTPHandler) Start() {
	h.Logger.Infof("Starting Http server at addr: %s", h.server.Addr)
	err := h.server.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		h.Logger.Fatalf("Failed to start http server: %v", err)
	}
	h.Logger.Infof("Http server closed gracefully.")
	return
}

// Stop start the http server to listen for request
func (h *HTTPHandler) Stop() error {
	return h.server.Close()
}

// ServeHTTP serves the http re
func (h *HTTPHandler) serveHealthz(rw http.ResponseWriter, req *http.Request) {

	rw.WriteHeader(h.Status)
	rw.Write([]byte(fmt.Sprintf("{\"health\":%v}", h.Status == http.StatusOK)))
}

// ServeInitialize serves the http re
func (h *HTTPHandler) serveInitialize(rw http.ResponseWriter, req *http.Request) {
	h.Logger.Info("Received start initialization request.")
	h.initializationStatusMutex.Lock()
	defer h.initializationStatusMutex.Unlock()
	if h.initializationStatus == initializationStatusNew {
		h.Logger.Infof("Updating status from %s to %s", h.initializationStatus, initializationStatusProgress)
		h.initializationStatus = initializationStatusProgress
		go func() {
			// This is needed to stop snapshotter.
			atomic.StoreUint32(&h.AckState, HandlerAckWaiting)
			h.Logger.Info("Changed handler state.")
			h.ReqCh <- emptyStruct
			h.Logger.Info("Waiting for acknowledgment...")
			<-h.AckCh
			err := h.EtcdInitializer.Initialize()
			h.initializationStatusMutex.Lock()
			defer h.initializationStatusMutex.Unlock()
			if err != nil {
				h.Logger.Errorf("Failed initialization: %v", err)
				rw.WriteHeader(http.StatusInternalServerError)
				h.initializationStatus = initializationStatusFailed
				// Optional: Event if we send start signal it wi
				// h.ReqCh <- HandlerSsrStart
				return
			}
			h.Logger.Infof("Successfully initialized data directory \"%s\" for etcd.", h.EtcdInitializer.Validator.Config.DataDir)
			h.initializationStatus = initializationStatusSuccessful
		}()
	}
	rw.WriteHeader(http.StatusOK)
}

// ServeInitializationStatus serves the etcd initialization progress status
func (h *HTTPHandler) serveInitializationStatus(rw http.ResponseWriter, req *http.Request) {
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
