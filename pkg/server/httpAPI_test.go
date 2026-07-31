// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

// fakeLearnerChecker is a test double for learnerChecker.
type fakeLearnerChecker struct {
	err            error
	learnerPresent bool
}

func (f *fakeLearnerChecker) IsLearnerPresent(_ context.Context) (bool, error) {
	return f.learnerPresent, f.err
}

// fakeMemberLister is a test double for memberLister.
type fakeMemberLister struct {
	err     error
	members []*etcdserverpb.Member
}

func (f *fakeMemberLister) ListMembers(_ context.Context) ([]*etcdserverpb.Member, error) {
	return f.members, f.err
}

func TestGetInitialClusterFromMemberList(t *testing.T) {
	etcdConf := `
initial-advertise-peer-urls:
  etcd-main-0:
    - http://10.0.0.1:2380
advertise-client-urls:
  etcd-main-0:
    - http://10.0.0.1:2379
`
	confFile, err := os.CreateTemp("", "etcd-conf-*.yaml")
	if err != nil {
		t.Fatalf("create temp config: %v", err)
	}
	defer os.Remove(confFile.Name())
	if _, err := confFile.WriteString(etcdConf); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	confFile.Close()

	// A non-empty ENDPOINTS file so EndpointsFileConfigured() returns true,
	// which makes GetMemberPeerURLs derive the self URL from POD_IP instead of
	// doing a name lookup in the config map.
	endpointsFile, err := os.CreateTemp("", "endpoints-*")
	if err != nil {
		t.Fatalf("create endpoints file: %v", err)
	}
	defer os.Remove(endpointsFile.Name())
	endpointsFile.Close()

	logger := logrus.New().WithField("suite", "getInitialClusterFromMemberList")

	tests := []struct {
		wantCluster map[string]string
		ml          *fakeMemberLister
		name        string
		podName     string
		podIP       string
		memberName  string
	}{
		{
			name:       "multi-member: adds self if absent",
			podName:    "etcd-main-2",
			podIP:      "10.0.0.3",
			memberName: "etcd-main-2",
			ml: &fakeMemberLister{
				members: []*etcdserverpb.Member{
					{Name: "etcd-main-0", PeerURLs: []string{"http://10.0.0.1:2380"}},
					{Name: "etcd-main-1", PeerURLs: []string{"http://10.0.0.2:2380"}},
				},
			},
			wantCluster: map[string]string{
				"etcd-main-0": "http://10.0.0.1:2380",
				"etcd-main-1": "http://10.0.0.2:2380",
				"etcd-main-2": "http://10.0.0.3:2380",
			},
		},
		{
			name:       "multi-member: self already present, not duplicated",
			podName:    "etcd-main-0",
			podIP:      "10.0.0.1",
			memberName: "etcd-main-0",
			ml: &fakeMemberLister{
				members: []*etcdserverpb.Member{
					{Name: "etcd-main-0", PeerURLs: []string{"http://10.0.0.1:2380"}},
					{Name: "etcd-main-1", PeerURLs: []string{"http://10.0.0.2:2380"}},
				},
			},
			wantCluster: map[string]string{
				"etcd-main-0": "http://10.0.0.1:2380",
				"etcd-main-1": "http://10.0.0.2:2380",
			},
		},
		{
			name:       "ListMembers error falls back to self only",
			podName:    "etcd-main-0",
			podIP:      "10.0.0.1",
			memberName: "etcd-main-0",
			ml:         &fakeMemberLister{err: errors.New("etcd unavailable")},
			wantCluster: map[string]string{
				"etcd-main-0": "http://10.0.0.1:2380",
			},
		},
		{
			// A learner registered but not yet started reports an empty name in
			// MemberList. It must not produce a duplicate or empty-name entry.
			name:       "unstarted learner (empty name) does not duplicate self URL",
			podName:    "etcd-main-1",
			podIP:      "10.0.0.2",
			memberName: "etcd-main-1",
			ml: &fakeMemberLister{
				members: []*etcdserverpb.Member{
					{Name: "etcd-main-0", PeerURLs: []string{"http://10.0.0.1:2380"}},
					{Name: "", PeerURLs: []string{"http://10.0.0.2:2380"}},
				},
			},
			wantCluster: map[string]string{
				"etcd-main-0": "http://10.0.0.1:2380",
				"etcd-main-1": "http://10.0.0.2:2380",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(miscellaneous.EndpointsEnvVar, endpointsFile.Name())
			t.Setenv("POD_NAME", tt.podName)
			t.Setenv("POD_IP", tt.podIP)

			result := getInitialClusterFromMemberList(context.Background(), confFile.Name(), tt.memberName, tt.ml, *logger)

			got := parseInitialCluster(result)
			if len(got) != len(tt.wantCluster) {
				t.Errorf("got %d entries %v, want %d entries %v", len(got), got, len(tt.wantCluster), tt.wantCluster)
				return
			}
			for name, wantURL := range tt.wantCluster {
				if gotURL, ok := got[name]; !ok {
					t.Errorf("missing entry for member %q in result %q", name, result)
				} else if gotURL != wantURL {
					t.Errorf("member %q: got URL %q, want %q", name, gotURL, wantURL)
				}
			}
		})
	}
}

// parseInitialCluster parses a comma-separated "name=url" initial-cluster string
// into a map of member name -> peer URL.
func parseInitialCluster(s string) map[string]string {
	result := make(map[string]string)
	if s == "" {
		return result
	}
	for _, entry := range strings.Split(s, ",") {
		name, url, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		result[name] = url
	}
	return result
}

func TestGetClusterState(t *testing.T) {
	tests := []struct {
		learnerErr     error
		name           string
		expectedState  string
		clusterSize    int
		learnerPresent bool
	}{
		{
			name:          "single node always returns new",
			clusterSize:   1,
			expectedState: miscellaneous.ClusterStateNew,
		},
		{
			name:           "multi-node with learner present returns existing",
			clusterSize:    3,
			learnerPresent: true,
			expectedState:  miscellaneous.ClusterStateExisting,
		},
		{
			name:           "multi-node with no learner returns new",
			clusterSize:    3,
			learnerPresent: false,
			expectedState:  miscellaneous.ClusterStateNew,
		},
		{
			name:          "multi-node fresh bootstrap (0-to-3 scale-out) returns new",
			clusterSize:   3,
			learnerErr:    errors.New("context deadline exceeded"),
			expectedState: miscellaneous.ClusterStateNew,
		},
		{
			name:           "multi-node with IsLearnerPresent error defaults to new",
			clusterSize:    3,
			learnerPresent: false,
			learnerErr:     errors.New("etcd connection refused"),
			expectedState:  miscellaneous.ClusterStateNew,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &HTTPHandler{Logger: logrus.New().WithField("suite", "cluster state")}
			m := &fakeLearnerChecker{learnerPresent: tt.learnerPresent, err: tt.learnerErr}
			state := h.getClusterState(context.Background(), tt.clusterSize, m)
			if state != tt.expectedState {
				t.Errorf("expected state %q, got %q", tt.expectedState, state)
			}
		})
	}
}

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
