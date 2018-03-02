// Copyright © 2018 The Gardener Authors.
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

	"github.com/gardener/etcd-backup-restore/pkg/initializer"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/snapshotter"
	"github.com/sirupsen/logrus"
)

// HTTPHandler is implementation to handle HTTP API exposed by server
type HTTPHandler struct {
	snapShotter     snapshotter.Snapshotter
	EtcdInitializer initializer.EtcdInitializer
	Port            int
	server          *http.Server
	Logger          *logrus.Logger
}

// RegisterHandler registers the handler for different requests
func (h *HTTPHandler) RegisterHandler() {
	http.HandleFunc("/initialize", h.serveInitialize)
	http.HandleFunc("/metrics", h.serveMetrics)
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
func (h *HTTPHandler) serveMetrics(rw http.ResponseWriter, req *http.Request) {

	rw.WriteHeader(http.StatusOK)
	rw.Write([]byte("{\"health\":\"true\"}"))
	//h.Logger.Infof("Received call for metrics.")
}

// ServeInitialize serves the http re
func (h *HTTPHandler) serveInitialize(rw http.ResponseWriter, req *http.Request) {
	//fmt.Fprintf(rw, "initialization request received.")
	err := h.EtcdInitializer.Initialize()
	if err != nil {
		h.Logger.Infof("Failed initialization: %v", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	h.Logger.Infof("Successfully initialized data directory \"%s\" for etcd.", h.EtcdInitializer.Validator.Config.DataDir)
	rw.WriteHeader(http.StatusOK)
}
