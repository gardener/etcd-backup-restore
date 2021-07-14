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
func NewLeaderElector(logger *logrus.Entry, etcdConnectionConfig *brtypes.EtcdConnectionConfig, leaderElectionConfig *Config, callbacks *LeaderCallbacks) (*LeaderElector, error) {
	return &LeaderElector{
		logger:               logger.WithField("actor", "leader-elector"),
		EtcdConnectionConfig: etcdConnectionConfig,
		CurrentState:         DefaultCurrentState,
		Config:               leaderElectionConfig,
		Callbacks:            callbacks,
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
			leCancel()
			return nil
		case <-time.After(le.Config.ReelectionPeriod.Duration):
			isLeader, err := le.IsLeader(ctx)
			if err != nil {
				le.logger.Errorf("Failed to elect the backup-restore leader: %v", err)

				// set the CurrentState of backup-restore.
				// stops the Running Snapshotter.
				// wait for Reelection to happen.
				le.CurrentState = StateUnknown
				le.logger.Infof("backup-restore is in: %v", le.CurrentState)
				if le.Callbacks.OnStoppedLeading != nil && leCtx != nil {
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

				if le.Callbacks.OnStoppedLeading != nil && leCtx != nil {
					leCancel()
					le.Callbacks.OnStoppedLeading()
				}
			} else if !isLeader && le.CurrentState == StateUnknown {
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
	client, err := etcdutil.GetTLSClientForEtcd(le.EtcdConnectionConfig)
	if err != nil {
		return false, &errors.EtcdError{
			Message: fmt.Sprintf("Failed to create etcd client: %v", err),
		}
	}
	defer client.Close()

	if len(le.EtcdConnectionConfig.Endpoints) > 0 {
		endPoint = le.EtcdConnectionConfig.Endpoints[0]
	} else {
		return false, fmt.Errorf("Etcd endpoints are not passed correctly")
	}

	ctx, cancel := context.WithTimeout(ctx, le.Config.EtcdConnectionTimeout.Duration)
	defer cancel()

	response, err := client.Status(ctx, endPoint)
	if err != nil {
		le.logger.Errorf("Failed to get status of etcd endPoint: %v with error: %v", le.EtcdConnectionConfig.Endpoints[0], err)
		return false, err
	}

	if response.Header.MemberId == response.Leader {
		return true, nil
	} else if response.Leader == NoLeaderID {
		return false, &errors.EtcdError{
			Message: fmt.Sprintf("Currently there is no Etcd Leader present may be due to etcd quorum loss or election is being held."),
		}
	}
	return false, nil
}

// GetLeader will return the LeaderID as well as PeerURLs of etcd leader.
func (le *LeaderElector) GetLeader(ctx context.Context) (uint64, []string, error) {
	le.logger.Info("getting the etcd leaderID...")
	var endPoint string
	client, err := etcdutil.GetTLSClientForEtcd(le.EtcdConnectionConfig)
	if err != nil {
		return NoLeaderID, nil, &errors.EtcdError{
			Message: fmt.Sprintf("Failed to create etcd client: %v", err),
		}
	}
	defer client.Close()

	if len(le.EtcdConnectionConfig.Endpoints) > 0 {
		endPoint = le.EtcdConnectionConfig.Endpoints[0]
	} else {
		return NoLeaderID, nil, &errors.EtcdError{
			Message: fmt.Sprintf("Etcd endpoints are not passed correctly"),
		}
	}

	ctx, cancel := context.WithTimeout(ctx, le.Config.EtcdConnectionTimeout.Duration)
	defer cancel()

	response, err := client.Status(ctx, endPoint)
	if err != nil {
		le.logger.Errorf("Failed to get status of etcd endPoint: %v with error: %v", le.EtcdConnectionConfig.Endpoints[0], err)
		return NoLeaderID, nil, err
	}

	if response.Leader == NoLeaderID {
		return NoLeaderID, nil, &errors.EtcdError{
			Message: fmt.Sprintf("Currently there is no Etcd Leader present may be due to etcd quorum loss."),
		}
	}

	membersInfo, err := client.MemberList(ctx)
	if err != nil {
		le.logger.Errorf("Failed to get memberList of etcd with error: %v", err)
		return response.Leader, nil, err
	}

	for _, member := range membersInfo.Members {
		if response.Leader == member.GetID() {
			return response.Leader, member.GetPeerURLs(), nil
		}
	}
	return response.Leader, nil, nil
}
