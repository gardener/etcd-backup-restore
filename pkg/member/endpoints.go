// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package member

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

type memberLister interface {
	ListMembers(context.Context) ([]*etcdserverpb.Member, error)
}

// WriteEndpointsAtomic atomically rewrites the file at filePath with one IP per line
// by writing to a temp file in the same directory, then renaming.
func WriteEndpointsAtomic(filePath string, ips []string) error {
	dir := filepath.Dir(filePath)
	tmpFile, err := os.CreateTemp(dir, ".endpoints-*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temp file for endpoints: %w", err)
	}
	tmpName := tmpFile.Name()

	if _, err := tmpFile.WriteString(strings.Join(ips, "\n") + "\n"); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("failed to write endpoints temp file: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("failed to close endpoints temp file: %w", err)
	}
	if err := os.Rename(tmpName, filePath); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("failed to rename endpoints temp file: %w", err)
	}
	return nil
}

// RefreshEndpointsPeriodically calls MemberList at each tick of etcdConfig.EndpointsRefreshInterval
// and atomically rewrites the ENDPOINTS file with the current member IPs.
// It returns when ctx is cancelled.
func RefreshEndpointsPeriodically(ctx context.Context, etcdConfig *brtypes.EtcdConnectionConfig, logger *logrus.Entry) error {
	endpointsFile, err := miscellaneous.GetEnvVarOrError(miscellaneous.EndpointsEnvVar)
	if err != nil {
		return fmt.Errorf("ENDPOINTS env var not set, cannot refresh endpoints: %w", err)
	}

	m := NewMemberControl(etcdConfig)

	timer := time.NewTimer(etcdConfig.EndpointsRefreshInterval.Duration)
	defer timer.Stop()
	logger.Info("Started endpoints refresh timer")

	for {
		select {
		case <-timer.C:
			if err := RefreshEndpoints(ctx, m, endpointsFile, logger); err != nil {
				logger.Warnf("Failed to refresh endpoints: %v", err)
			}
			timer.Reset(etcdConfig.EndpointsRefreshInterval.Duration)
		case <-ctx.Done():
			logger.Info("Stopped endpoints refresh timer")
			return nil
		}
	}
}

// RefreshEndpoints fetches the current member list and atomically rewrites the ENDPOINTS file with their IPs.
func RefreshEndpoints(ctx context.Context, m memberLister, endpointsFile string, logger *logrus.Entry) error {
	members, err := m.ListMembers(ctx)
	if err != nil {
		return fmt.Errorf("MemberList failed: %w", err)
	}

	var ips []string
	for _, mem := range members {
		for _, peerURL := range mem.GetPeerURLs() {
			u, err := url.Parse(peerURL)
			if err != nil {
				continue
			}
			if host := u.Hostname(); host != "" {
				ips = append(ips, host)
				break
			}
		}
	}

	if len(ips) == 0 {
		logger.Warn("MemberList returned no members with peer URLs; skipping endpoints file update")
		return nil
	}

	if err := WriteEndpointsAtomic(endpointsFile, ips); err != nil {
		return err
	}
	logger.Infof("Refreshed endpoints file %q with %d IPs", endpointsFile, len(ips))
	return nil
}
