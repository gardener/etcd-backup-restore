// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package types

import (
	"context"
	"fmt"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/wrappers"

	"github.com/sirupsen/logrus"
	flag "github.com/spf13/pflag"
)

const (
	// DefaultReelectionPeriod defines default time period for Reelection.
	DefaultReelectionPeriod = 5 * time.Second
	// DefaultEtcdStatusConnecTimeout defines default ConnectionTimeout for etcd client to get Etcd endpoint status.
	DefaultEtcdStatusConnecTimeout = 5 * time.Second
)

// LeaderCallbacks are callbacks that are triggered to start/stop the snapshottter when leader's currentState changes.
type LeaderCallbacks struct {
	// OnStartedLeading is called when a LeaderElector client starts leading.
	OnStartedLeading func(context.Context)
	// OnStoppedLeading is called when a LeaderElector client stops leading.
	OnStoppedLeading func()
}

// MemberLeaseCallbacks are callbacks that are triggered to start/stop periodic member lease renewel.
type MemberLeaseCallbacks struct {
	// StartLeaseRenewal is called when etcd member moved from StateUnknown to either StateLeader or StateFollower.
	StartLeaseRenewal func()
	// OnStoppedLeading is called when etcd member moved to StateUnknown from any other State.
	StopLeaseRenewal func()
}

// EtcdMemberStatusCallbackFunc is type declaration for callback function to Check Etcd member Status.
type EtcdMemberStatusCallbackFunc func(context.Context, *EtcdConnectionConfig, time.Duration, *logrus.Entry) (bool, bool, error)

// PromoteLearnerCallback is callback which is triggered when backup-restore wants to promote etcd learner to a voting member.
type PromoteLearnerCallback struct {
	Promote func(context.Context, *logrus.Entry)
}

// Config holds the LeaderElection config.
type Config struct {
	// ReelectionPeriod defines the Period after which leadership status is checked.
	ReelectionPeriod wrappers.Duration `json:"reelectionPeriod,omitempty"`
	// EtcdConnectionTimeout defines the timeout duration for etcd client connection during leader election.
	EtcdConnectionTimeout wrappers.Duration `json:"etcdConnectionTimeout,omitempty"`
}

// NewLeaderElectionConfig returns the Config.
func NewLeaderElectionConfig() *Config {
	return &Config{
		ReelectionPeriod:      wrappers.Duration{Duration: DefaultReelectionPeriod},
		EtcdConnectionTimeout: wrappers.Duration{Duration: DefaultEtcdStatusConnecTimeout},
	}
}

// AddFlags adds the flags to flagset.
func (c *Config) AddFlags(fs *flag.FlagSet) {
	fs.DurationVar(&c.EtcdConnectionTimeout.Duration, "etcd-connection-timeout-leader-election", c.EtcdConnectionTimeout.Duration, "timeout duration of etcd client connection during leader election")
	fs.DurationVar(&c.ReelectionPeriod.Duration, "reelection-period", c.ReelectionPeriod.Duration, "period after which election will be re-triggered to check the leadership status")
}

// Validate validates the Config.
func (c *Config) Validate() error {
	if c.ReelectionPeriod.Duration <= time.Duration(1*time.Second) {
		return fmt.Errorf("reelectionPeriod should be greater than 1 second")
	}

	if c.EtcdConnectionTimeout.Duration <= time.Duration(1*time.Second) {
		return fmt.Errorf("etcd connection timeout during leader election should be greater than 1 second")
	}

	return nil
}
