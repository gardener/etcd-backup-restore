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
// limitations under the License.

package snapstore_test

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"

	th "github.com/gophercloud/gophercloud/testhelper"
	fake "github.com/gophercloud/gophercloud/testhelper/client"
	"github.com/sirupsen/logrus"
)

var objectMapMutex sync.Mutex

// initializeMockSwiftServer registers the handlers for different operation on swift
func initializeMockSwiftServer(t *testing.T) {
	th.Mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		th.TestHeader(t, r, "X-Auth-Token", fake.TokenID)
		switch r.Method {
		case "GET":
			th.TestMethod(t, r, "GET")
			object := parseObjectNamefromURL(r.URL)
			logrus.Printf("Received get request for object %s", object)
			if len(object) == 0 {
				handleListObjectNames(w, r)
			} else {
				handleDownloadObject(w, r)
			}
		case "PUT":
			th.TestMethod(t, r, "PUT")
			handleCreateTextObject(w, r)
		case "DELETE":
			th.TestMethod(t, r, "DELETE")
			handleDeleteObject(w, r)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
}

// handleCreateTextObject creates an HTTP handler at `/testContainer/testObject` on the test handler mux
// that responds with a `Create` response.
func handleCreateTextObject(w http.ResponseWriter, r *http.Request) {
	var (
		content []byte
		err     error
	)
	key := parseObjectNamefromURL(r.URL)
	if len(key) == 0 {
		logrus.Errorf("object name cannot be empty")
		w.WriteHeader(http.StatusBadRequest)
	}
	if len(r.Header.Get("X-Object-Manifest")) == 0 {
		buf := new(bytes.Buffer)
		if _, err = io.Copy(buf, r.Body); err != nil {
			logrus.Errorf("failed to read content %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		content = buf.Bytes()
	} else {
		content = make([]byte, 0)
	}
	objectMapMutex.Lock()
	objectMap[key] = &content
	objectMapMutex.Unlock()
	hash := md5.New()
	io.WriteString(hash, string(content))
	localChecksum := hash.Sum(nil)
	w.Header().Set("ETag", fmt.Sprintf("%x", localChecksum))
	w.WriteHeader(http.StatusCreated)
}

// handleDownloadObject creates an HTTP handler at `/testContainer/testObject` on the test handler mux that
// responds with a `Download` response.
func handleDownloadObject(w http.ResponseWriter, r *http.Request) {
	prefix := parseObjectNamefromURL(r.URL)
	var contents []byte
	for key, val := range objectMap {
		if strings.HasPrefix(key, prefix) {
			data := *val
			contents = append(contents, data...)
		}
	}

	w.Write(contents)
}

// handleListObjectNames creates an HTTP handler at `/testContainer` on the test handler mux that
// responds with a `List` response when only object names are requested.
func handleListObjectNames(w http.ResponseWriter, r *http.Request) {
	marker := r.URL.Query().Get("marker")

	// To store the keys in slice in sorted order
	var keys, contents []string
	for k := range objectMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, key := range keys {
		if strings.Compare(key, marker) > 0 {
			contents = append(contents, key)
		}
	}
	w.Header().Set("X-Container-Object-Count", fmt.Sprint(len(contents)))
	w.Header().Set("Content-Type", "text/plain")
	list := strings.Join(contents, "\n")
	w.Write([]byte(list))
}

// handleDeleteObject creates an HTTP handler at `/testContainer/testObject` on the test handler mux that
// responds with a `Delete` response.
func handleDeleteObject(w http.ResponseWriter, r *http.Request) {
	key := parseObjectNamefromURL(r.URL)
	if _, ok := objectMap[key]; ok {
		objectMapMutex.Lock()
		delete(objectMap, key)
		objectMapMutex.Unlock()
		w.WriteHeader(http.StatusNoContent)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}
