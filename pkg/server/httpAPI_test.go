// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthCheckHandler(t *testing.T) {
	// HTTPHandler is implementation to handle HTTP API exposed by server
	healthyHandler := HTTPHandler{}
	healthyHandler.SetStatus(http.StatusOK)
	unhealthyHandler := HTTPHandler{}
	unhealthyHandler.SetStatus(http.StatusInternalServerError)
	if err := healthCheckTest(healthyHandler.serveHealthz, http.StatusOK, true); err != nil {
		t.Fatal(err)
	}
	if err := healthCheckTest(unhealthyHandler.serveHealthz, http.StatusInternalServerError, false); err != nil {
		t.Fatal(err)
	}
}

func healthCheckTest(handlerFunc http.HandlerFunc, expectedStatus int, expectedHealth bool) error {
	// Create a request to pass to our handler. We don't have any query parameters for now, so we'll
	// pass 'nil' as the third parameter.
	req, err := http.NewRequest("GET", "/healthz", nil)
	if err != nil {
		return err
	}
	// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(handlerFunc)

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != expectedStatus {
		return fmt.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	// Check the response body is what we expect.
	expected := fmt.Sprintf(`{"health":%v}`, expectedHealth)
	if rr.Body.String() != expected {
		return fmt.Errorf("handler returned unexpected body: got %v want %v",
			rr.Body.String(), expected)
	}
	return nil
}
