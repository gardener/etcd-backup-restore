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
	"fmt"
	"sync"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/errors"
	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/sirupsen/logrus"
)

// NewLeaderElector returns LeaderElector configurations.
func NewLeaderElector(logger *logrus.Entry, etcdConnectionConfig *brtypes.EtcdConnectionConfig, leaderElectionConfig *Config, callbacks *LeaderCallbacks, memberLeaseCallbacks *MemberLeaseCallbacks) (*LeaderElector, error) {
	return &LeaderElector{
		logger:               logger.WithField("actor", "leader-elector"),
		EtcdConnectionConfig: etcdConnectionConfig,
		CurrentState:         DefaultCurrentState,
		Config:               leaderElectionConfig,
		Callbacks:            callbacks,
		LeaseCallbacks:       memberLeaseCallbacks,
		ElectionLock:         &sync.Mutex{},
	}, nil
}

// Run starts the LeaderElection loop to elect the backup-restore's Leader
// and keep checking the leadership status of backup-restore.
func (le *LeaderElector) Run(ctx context.Context) error {
	le.logger.Infof("Starting leaderElection...")
	var leCtx context.Context
	var leCancel context.CancelFunc

	for {
		select {
		case <-ctx.Done():
			le.logger.Info("Shutting down LeaderElection...")
			if leCancel != nil {
				leCancel()
			}
			return nil
		case <-time.After(le.Config.ReelectionPeriod.Duration):
			isLeader, err := le.IsLeader(ctx)
			if err != nil {
				le.logger.Errorf("failed to elect the backup-restore leader: %v", err)

				// set the CurrentState of backup-restore.
				// stops the Running Snapshotter.
				// stops the Renewal of member lease(if running).
				// wait for Reelection to happen.
				if le.CurrentState != StateUnknown && le.LeaseCallbacks.StopLeaseRenewal != nil {
					le.LeaseCallbacks.StopLeaseRenewal()
				}
				le.CurrentState = StateUnknown
				le.logger.Infof("backup-restore is in: %v", le.CurrentState)
				if leCtx != nil {
					leCancel()
					le.Callbacks.OnStoppedLeading()
				}
				le.logger.Info("waiting for Re-election...")
				continue
			}

			if isLeader && (le.CurrentState == StateFollower || le.CurrentState == StateUnknown || le.CurrentState == StateCandidate) {
				// backup-restore becomes the Leader backup-restore.
				// set the CurrentState of backup-restore.
				// update the snapshotter object with latest snapshotter object.
				// start the snapshotter.

				if le.CurrentState == StateUnknown && le.LeaseCallbacks.StartLeaseRenewal != nil {
					le.LeaseCallbacks.StartLeaseRenewal()
				}
				le.CurrentState = StateLeader
				le.logger.Infof("backup-restore became: %v", le.CurrentState)

				if le.Callbacks.OnStartedLeading != nil {
					leCtx, leCancel = context.WithCancel(ctx)
					le.logger.Info("backup-restore started leading...")
					le.Callbacks.OnStartedLeading(leCtx)
				}
			} else if isLeader && le.CurrentState == StateLeader {
				le.logger.Debug("no change in leadershipStatus...")
			} else if !isLeader && le.CurrentState == StateLeader {
				// backup-restore lost the election and becomes Follower.
				// set the CurrentState of backup-restore.
				// stop the Running snapshotter.
				le.CurrentState = StateFollower
				le.logger.Info("backup-restore lost the election")
				le.logger.Infof("backup-restore became: %v", le.CurrentState)

				if leCtx != nil {
					leCancel()
					le.Callbacks.OnStoppedLeading()
				}
			} else if !isLeader && le.CurrentState == StateUnknown {
				if le.LeaseCallbacks.StartLeaseRenewal != nil {
					le.LeaseCallbacks.StartLeaseRenewal()
				}
				le.CurrentState = StateFollower
				le.logger.Infof("backup-restore changed the state from %v to %v", StateUnknown, le.CurrentState)
			} else if !isLeader && le.CurrentState == StateFollower {
				le.logger.Infof("backup-restore currentState: %v", le.CurrentState)
			}
		}
	}
}

// IsLeader checks whether the current backup-restore is leader or not.
func (le *LeaderElector) IsLeader(ctx context.Context) (bool, error) {
	le.logger.Info("checking the leadershipStatus...")
	var endPoint string

	factory := etcdutil.NewFactory(*le.EtcdConnectionConfig)
	client, err := factory.NewMaintenance()
	if err != nil {
		return false, &errors.EtcdError{
			Message: fmt.Sprintf("failed to create etcd maintenance client: %v", err),
		}
	}
	defer client.Close()

	if len(le.EtcdConnectionConfig.Endpoints) > 0 {
		endPoint = le.EtcdConnectionConfig.Endpoints[0]
	} else {
		return false, fmt.Errorf("etcd endpoints are not passed correctly")
	}

	ctx, cancel := context.WithTimeout(ctx, le.Config.EtcdConnectionTimeout.Duration)
	defer cancel()

	response, err := client.Status(ctx, endPoint)
	if err != nil {
		le.logger.Errorf("failed to get status of etcd endPoint: %v with error: %v", endPoint, err)
		return false, err
	}

	if response.Header.MemberId == response.Leader {
		return true, nil
	} else if response.Leader == NoLeaderState {
		return false, &errors.EtcdError{
			Message: fmt.Sprintf("currently there is no etcd leader present may be due to etcd quorum loss or election is being held"),
		}
	}
	return false, nil
}
