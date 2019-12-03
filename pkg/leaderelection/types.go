// Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
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

	"github.com/sirupsen/logrus"

	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	"github.com/gardener/etcd-backup-restore/pkg/wrappers"
)

// Config defines the configuration for leader election
type Config struct {
	// RetryPeriod is the duration the LeaderElector clients should wait
	// between tries of actions.
	RetryPeriod wrappers.Duration `json:"retryPeriod,omitempty"`
}

// Status defines the current state for leader election
type Status struct {
	observedLeader bool
}

// LeaderCallbacks is struct to pass callbacks associated with leader election
type LeaderCallbacks struct {
	// OnErrorInLeaderElection is called when a LeaderElector client faces error in determining leadership
	OnErrorInLeaderElection func()
	// OnStartedLeading is called when a LeaderElector client starts leading
	OnStartedLeading func(context.Context)
	// OnStoppedLeading is called when a LeaderElector client stops leading
	OnStoppedLeading func()
	// OnNewLeader is called when the client observes a leader that is
	// not the previously observed leader. This includes the first observed
	// leader when the client starts.
	OnNewLeader func(identity string)
}

// LeaderElector holds the details for leader election
type LeaderElector struct {
	logger               *logrus.Entry
	callbacks            *LeaderCallbacks
	config               *Config
	etcdConnectionConfig *etcdutil.EtcdConnectionConfig
	status               *Status
}
