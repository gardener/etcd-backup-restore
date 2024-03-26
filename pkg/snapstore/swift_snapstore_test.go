// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore_test

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/gophercloud/gophercloud/openstack/objectstorage/v1/objects"
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
			// for backwards compatibility
			if r.URL.RawQuery == "bulk-delete=true" {
				handleBulkDeleteObject(w, r)
			}
			handleDeleteObject(w, r)
		case "POST":
			th.TestMethod(t, r, "POST")
			if r.URL.RawQuery == "bulk-delete=true" {
				handleBulkDeleteObject(w, r)
			}
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
		// segment object
		buf := new(bytes.Buffer)
		if _, err = io.Copy(buf, r.Body); err != nil {
			logrus.Errorf("failed to read content %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		content = buf.Bytes()
		objectMapMutex.Lock()
		objectMap[key] = &content
		objectMapMutex.Unlock()
	} else {
		// manifest object
		content = make([]byte, 0)
		objectMapMutex.Lock()
		objectMap[key] = &content
		objectMapMutex.Unlock()
	}

	hash := md5.New()
	io.WriteString(hash, string(content))
	localChecksum := hash.Sum(nil)
	w.Header().Set("ETag", fmt.Sprintf("%x", localChecksum))
	w.WriteHeader(http.StatusCreated)
}

// handleDownloadObject creates an HTTP handler at `/testContainer/testObject` on the test handler mux that
// responds with a `Download` response.
func handleDownloadObject(w http.ResponseWriter, r *http.Request) {
	objectMapMutex.Lock()
	defer objectMapMutex.Unlock()

	prefix := parseObjectNamefromURL(r.URL)
	var contents []byte
	var sortedKeys []string
	for key := range objectMap {
		if strings.HasPrefix(key, prefix) {
			sortedKeys = append(sortedKeys, key)
		}
	}

	// append segment objects in order
	sort.Strings(sortedKeys)
	for _, key := range sortedKeys {
		data := *objectMap[key]
		contents = append(contents, data...)
	}

	w.Write(contents)
}

// handleListObjectNames creates an HTTP handler at `/testContainer` on the test handler mux that
// responds with a `List` response when only object names are requested.
func handleListObjectNames(w http.ResponseWriter, r *http.Request) {
	objectMapMutex.Lock()
	defer objectMapMutex.Unlock()

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
	objectMapMutex.Lock()
	defer objectMapMutex.Unlock()
	key := parseObjectNamefromURL(r.URL)
	if _, ok := objectMap[key]; ok {
		delete(objectMap, key)
		w.WriteHeader(http.StatusNoContent)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

// handleBulkDeleteObject creates an HTTP handler at `/testContainer/testObject` on the test handler mux that
// responds with a `Delete` response.
func handleBulkDeleteObject(w http.ResponseWriter, r *http.Request) {
	objectMapMutex.Lock()
	defer objectMapMutex.Unlock()

	var bulkDeleteResponse objects.BulkDeleteResponse

	buf := new(bytes.Buffer)
	if _, err := io.Copy(buf, r.Body); err != nil {
		errorMessage := fmt.Sprintf("failed to read content %v", err)
		logrus.Errorf(errorMessage)
		bulkDeleteResponse.Errors = append(bulkDeleteResponse.Errors, []string{errorMessage})
		marshalledResponse, _ := json.Marshal(bulkDeleteResponse)
		w.WriteHeader(http.StatusOK)
		w.Write(marshalledResponse)
		return
	}

	segmentObjects := strings.Split(strings.TrimSpace(string(buf.Bytes())), "\n")
	for _, segmentObject := range segmentObjects {
		segmentObject = "/" + segmentObject
		// objects.BulkDelete() internally calls url.QueryEscape
		unescapedQueryForObject, err := url.QueryUnescape(segmentObject)

		var errorStrings []string
		if err != nil {
			errorMessage := fmt.Sprintf("failed to unescape url %v", err)
			logrus.Errorf(errorMessage)
			errorStrings = append(errorStrings, errorMessage)
		}

		key := parseObjectNamefromURL(&url.URL{Path: unescapedQueryForObject})
		if _, ok := objectMap[key]; ok {
			delete(objectMap, key)
			bulkDeleteResponse.NumberDeleted++
		} else {
			errorMessage := fmt.Sprintf("Resource not found: %s", segmentObject)
			logrus.Errorf(errorMessage)
			bulkDeleteResponse.NumberNotFound++
			errorStrings = append(errorStrings, errorMessage)
		}
		bulkDeleteResponse.Errors = append(bulkDeleteResponse.Errors, errorStrings)
	}

	marshalledResponse, _ := json.Marshal(bulkDeleteResponse)
	w.WriteHeader(http.StatusOK)
	w.Write(marshalledResponse)
}
