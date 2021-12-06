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

package leaderelection

import (
	"context"
	"sync"
	"time"

	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/gardener/etcd-backup-restore/pkg/wrappers"
	"github.com/sirupsen/logrus"
)

const (
	// StateFollower defines currentState of backup-restore as "Follower".
	StateFollower = "Follower"
	// StateCandidate defines currentState of backup-restore as "Candidate".
	StateCandidate = "Candidate"
	// StateLeader defines currentState of backup-restore as "Leader".
	StateLeader = "Leader"
	// StateUnknown defines currentState of backup-restore as "UnknownState".
	StateUnknown = "UnknownState"

	// DefaultCurrentState defines default currentState of backup-restore as "Follower".
	DefaultCurrentState = StateFollower

	// NoLeaderState defines the state when etcd returns LeaderID as 0.
	NoLeaderState uint64 = 0

	// DefaultReelectionPeriod defines default time period for Reelection.
	DefaultReelectionPeriod = 5 * time.Second
	// DefaultEtcdConnectionTimeout defines default ConnectionTimeout for etcd client.
	DefaultEtcdConnectionTimeout = 5 * time.Second
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

// IsLeaderCallbackFunc is type declaration for callback function to Check LeadershipStatus.
type IsLeaderCallbackFunc func(context.Context, *brtypes.EtcdConnectionConfig, time.Duration, *logrus.Entry) (bool, error)

// Config holds the LeaderElection config.
type Config struct {
	// ReelectionPeriod defines the Period after which leadership status is checked.
	ReelectionPeriod wrappers.Duration `json:"reelectionPeriod,omitempty"`
	// EtcdConnectionTimeout defines the timeout duration for etcd client connection during leader election.
	EtcdConnectionTimeout wrappers.Duration `json:"etcdConnectionTimeout,omitempty"`
}

// LeaderElector holds the all configuration necessary to elect backup-restore Leader.
type LeaderElector struct {
	// CurrentState defines currentState of backup-restore for LeaderElection.
	CurrentState          string
	Config                *Config
	EtcdConnectionConfig  *brtypes.EtcdConnectionConfig
	logger                *logrus.Entry
	Callbacks             *LeaderCallbacks
	LeaseCallbacks        *MemberLeaseCallbacks
	CheckLeadershipStatus IsLeaderCallbackFunc
	ElectionLock          *sync.Mutex
}
