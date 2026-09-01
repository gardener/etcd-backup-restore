// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package types_test

import (
	"testing"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/wrappers"

	. "github.com/gardener/etcd-backup-restore/pkg/types"
)

func validConfig() *EtcdConnectionConfig {
	c := NewEtcdConnectionConfig()
	return c
}

func TestValidate_EndpointsRefresh(t *testing.T) {
	tests := []struct {
		name     string
		interval time.Duration
		enabled  bool
		wantErr  bool
	}{
		{
			name:     "disabled with zero interval is ok",
			enabled:  false,
			interval: 0,
			wantErr:  false,
		},
		{
			name:     "enabled with positive interval is ok",
			enabled:  true,
			interval: 10 * time.Second,
			wantErr:  false,
		},
		{
			name:     "enabled with zero interval errors",
			enabled:  true,
			interval: 0,
			wantErr:  true,
		},
		{
			name:     "enabled with negative interval errors",
			enabled:  true,
			interval: -1 * time.Second,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := validConfig()
			c.EndpointsRefreshEnabled = tt.enabled
			c.EndpointsRefreshInterval = wrappers.Duration{Duration: tt.interval}
			err := c.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
