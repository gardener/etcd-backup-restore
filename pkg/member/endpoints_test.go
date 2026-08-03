// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package member_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/gardener/etcd-backup-restore/pkg/member"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/etcdserverpb"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type fakeMemberLister struct {
	err     error
	members []*etcdserverpb.Member
}

func (f *fakeMemberLister) ListMembers(_ context.Context) ([]*etcdserverpb.Member, error) {
	return f.members, f.err
}

var _ = Describe("writeEndpointsAtomic", func() {
	var tmpDir string

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "endpoints-test-*")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(os.RemoveAll(tmpDir)).To(Succeed())
	})

	It("writes IPs one per line", func() {
		dest := filepath.Join(tmpDir, "ENDPOINTS")
		Expect(member.WriteEndpointsAtomic(dest, []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"})).To(Succeed())

		data, err := os.ReadFile(dest)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(data)).To(Equal("10.0.0.1\n10.0.0.2\n10.0.0.3\n"))
	})

	It("leaves no partial temp file on success", func() {
		dest := filepath.Join(tmpDir, "ENDPOINTS")
		Expect(member.WriteEndpointsAtomic(dest, []string{"192.168.1.1"})).To(Succeed())

		entries, err := os.ReadDir(tmpDir)
		Expect(err).NotTo(HaveOccurred())
		// Only the final file should remain; no leftover .tmp files
		Expect(entries).To(HaveLen(1))
		Expect(entries[0].Name()).To(Equal("ENDPOINTS"))
	})

	It("overwrites an existing file atomically", func() {
		dest := filepath.Join(tmpDir, "ENDPOINTS")
		Expect(os.WriteFile(dest, []byte("old\n"), 0600)).To(Succeed())
		Expect(member.WriteEndpointsAtomic(dest, []string{"10.1.2.3"})).To(Succeed())

		data, err := os.ReadFile(dest)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(data)).To(Equal("10.1.2.3\n"))
	})
})

func TestRefreshEndpoints(t *testing.T) {
	logger := logrus.New().WithField("suite", "refreshEndpoints")

	cases := []struct {
		listErr     error
		name        string
		wantContent string
		preExisting string
		members     []*etcdserverpb.Member
		wantErr     bool
	}{
		{
			name: "writes IPs extracted from peer URLs",
			members: []*etcdserverpb.Member{
				{PeerURLs: []string{"http://10.0.0.1:2380"}},
				{PeerURLs: []string{"http://10.0.0.2:2380"}},
			},
			wantContent: "10.0.0.1\n10.0.0.2\n",
		},
		{
			name:        "ListMembers error is propagated",
			listErr:     errors.New("etcd unavailable"),
			wantErr:     true,
			preExisting: "10.0.0.1\n",
			wantContent: "10.0.0.1\n",
		},
		{
			name:        "no peer URLs skips write",
			members:     []*etcdserverpb.Member{{PeerURLs: []string{}}},
			preExisting: "10.0.0.1\n",
			wantContent: "10.0.0.1\n",
		},
		{
			name: "unparseable peer URL is skipped, valid one is written",
			members: []*etcdserverpb.Member{
				{PeerURLs: []string{"://bad"}},
				{PeerURLs: []string{"http://10.0.0.3:2380"}},
			},
			wantContent: "10.0.0.3\n",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dest := filepath.Join(t.TempDir(), "ENDPOINTS")
			if tc.preExisting != "" {
				if err := os.WriteFile(dest, []byte(tc.preExisting), 0600); err != nil {
					t.Fatalf("setup: %v", err)
				}
			}

			err := member.RefreshEndpoints(context.Background(), &fakeMemberLister{members: tc.members, err: tc.listErr}, dest, logger)
			if tc.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tc.wantContent != "" {
				data, err := os.ReadFile(dest)
				if err != nil {
					t.Fatalf("reading result file: %v", err)
				}
				if got := string(data); got != tc.wantContent {
					t.Errorf("file content = %q, want %q", got, tc.wantContent)
				}
			}
		})
	}
}
