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
	"sync"

	"github.com/gardener/etcd-backup-restore/pkg/initializer"
	"github.com/sirupsen/logrus"
)

const (
	initializationStatusNew        = "New"
	initializationStatusProgress   = "Progress"
	initializationStatusSuccessful = "Successful"
	initializationStatusFailed     = "Failed"
)

// HTTPHandler is implementation to handle HTTP API exposed by server
type HTTPHandler struct {
	EtcdInitializer           initializer.EtcdInitializer
	Port                      int
	server                    *http.Server
	Logger                    *logrus.Logger
	initializationStatusMutex sync.Mutex
	initializationStatus      string
	Status                    int
}

// RegisterHandler registers the handler for different requests
func (h *HTTPHandler) RegisterHandler() {
	h.initializationStatus = "New"
	http.HandleFunc("/initialization/start", h.serveInitialize)
	http.HandleFunc("/initialization/status", h.serveInitializationStatus)
	http.HandleFunc("/healthz", h.serveHealthz)
	return
}

// Start start the http server to listen for request
func (h *HTTPHandler) Start() {
	h.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", h.Port),
		Handler: nil,
	}
	err := h.server.ListenAndServe()
	h.Logger.Fatalf("Failed to start http server: %v", err)
	return
}

// Stop start the http server to listen for request
func (h *HTTPHandler) Stop() error {
	return h.server.Close()
}

// ServeHTTP serves the http re
func (h *HTTPHandler) serveHealthz(rw http.ResponseWriter, req *http.Request) {

	rw.WriteHeader(h.Status)
	rw.Write([]byte(fmt.Sprintf("{\"health\":\"%v\"}", h.Status == http.StatusOK)))
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
			err := h.EtcdInitializer.Initialize()
			h.initializationStatusMutex.Lock()
			defer h.initializationStatusMutex.Unlock()
			if err != nil {
				h.Logger.Errorf("Failed initialization: %v", err)
				rw.WriteHeader(http.StatusInternalServerError)
				h.initializationStatus = initializationStatusFailed
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
