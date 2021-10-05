// Copyright (c) 2021 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
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

package types

import (
	"errors"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/wrappers"

	flag "github.com/spf13/pflag"
)

const (
	// DefaultOwnerCheckInterval is the default time interval between owner checks.
	DefaultOwnerCheckInterval = 30 * time.Second
	// DefaultOwnerCheckTimeout is the default timeout for owner checks.
	DefaultOwnerCheckTimeout = 2 * time.Minute
	// DefaultOwnerCheckDNSCacheTTL is the default DNS cache TTL for owner checks.
	DefaultOwnerCheckDNSCacheTTL = 1 * time.Minute
)

// OwnerCheckConfig holds the configuration for the owner checks.
type OwnerCheckConfig struct {
	OwnerName             string            `json:"ownerName,omitempty"`
	OwnerID               string            `json:"ownerID,omitempty"`
	OwnerCheckInterval    wrappers.Duration `json:"ownerCheckInterval,omitempty"`
	OwnerCheckTimeout     wrappers.Duration `json:"ownerCheckTimeout,omitempty"`
	OwnerCheckDNSCacheTTL wrappers.Duration `json:"ownerCheckDNSCacheTTL,omitempty"`
}

// NewOwnerCheckConfig creates and returns a new OwnerCheckConfig.
func NewOwnerCheckConfig() *OwnerCheckConfig {
	return &OwnerCheckConfig{
		OwnerCheckInterval:    wrappers.Duration{Duration: DefaultOwnerCheckInterval},
		OwnerCheckTimeout:     wrappers.Duration{Duration: DefaultOwnerCheckTimeout},
		OwnerCheckDNSCacheTTL: wrappers.Duration{Duration: DefaultOwnerCheckDNSCacheTTL},
	}
}

// AddFlags adds the flags to flagset.
func (c *OwnerCheckConfig) AddFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.OwnerName, "owner-name", c.OwnerName, "owner domain name")
	fs.StringVar(&c.OwnerID, "owner-id", c.OwnerID, "owner id")
	fs.DurationVar(&c.OwnerCheckInterval.Duration, "owner-check-interval", c.OwnerCheckInterval.Duration, "time interval between owner checks")
	fs.DurationVar(&c.OwnerCheckTimeout.Duration, "owner-check-timeout", c.OwnerCheckTimeout.Duration, "timeout for owner checks")
	fs.DurationVar(&c.OwnerCheckDNSCacheTTL.Duration, "owner-check-dns-cache-ttl", c.OwnerCheckDNSCacheTTL.Duration, "DNS cache TTL for owner checks")
}

// Validate validates the config.
func (c *OwnerCheckConfig) Validate() error {
	if c.OwnerCheckInterval.Duration < 0 {
		return errors.New("parameter owner-check-interval must not be less than 0")
	}
	if c.OwnerCheckTimeout.Duration < 0 {
		return errors.New("parameter owner-check-timeout must not be less than 0")
	}
	if c.OwnerCheckDNSCacheTTL.Duration < 0 {
		return errors.New("parameter owner-check-dns-cache-ttl must not be less than 0")
	}
	return nil
}
