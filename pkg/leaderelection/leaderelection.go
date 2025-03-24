// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package leaderelection

import (
	"context"
	"fmt"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/errors"
	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	"github.com/gardener/etcd-backup-restore/pkg/metrics"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

	"github.com/prometheus/client_golang/prometheus"
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
)

// LeaderElector holds the all configuration necessary to elect backup-restore Leader.
type LeaderElector struct {
	// CurrentState defines currentState of backup-restore for LeaderElection.
	CurrentState         string
	Config               *brtypes.Config
	EtcdConnectionConfig *brtypes.EtcdConnectionConfig
	logger               *logrus.Entry
	Callbacks            *brtypes.LeaderCallbacks
	LeaseCallbacks       *brtypes.MemberLeaseCallbacks
	PromoteCallback      *brtypes.PromoteLearnerCallback
	CheckMemberStatus    brtypes.EtcdMemberStatusCallbackFunc
}

// NewLeaderElector returns LeaderElector configurations.
func NewLeaderElector(logger *logrus.Entry, etcdConnectionConfig *brtypes.EtcdConnectionConfig, leaderElectionConfig *brtypes.Config, callbacks *brtypes.LeaderCallbacks, memberLeaseCallbacks *brtypes.MemberLeaseCallbacks, memberStatusFunc brtypes.EtcdMemberStatusCallbackFunc, promoteCallback *brtypes.PromoteLearnerCallback) (*LeaderElector, error) {
	return &LeaderElector{
		logger:               logger.WithField("actor", "leader-elector"),
		EtcdConnectionConfig: etcdConnectionConfig,
		CurrentState:         DefaultCurrentState,
		Config:               leaderElectionConfig,
		Callbacks:            callbacks,
		LeaseCallbacks:       memberLeaseCallbacks,
		CheckMemberStatus:    memberStatusFunc,
		PromoteCallback:      promoteCallback,
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
			isLeader, isLearner, err := le.CheckMemberStatus(ctx, le.EtcdConnectionConfig, le.Config.EtcdConnectionTimeout.Duration, le.logger)
			if err != nil {
				le.logger.Errorf("failed to elect the backup-restore leader: %v", err)

				// set the CurrentState of backup-restore.
				// stops the Running Snapshotter.
				// stops the Renewal of member lease(if running).
				// wait for Reelection to happen.
				if le.CurrentState != StateUnknown && le.LeaseCallbacks.StopLeaseRenewal != nil {
					le.LeaseCallbacks.StopLeaseRenewal()
				}
				if le.CurrentState == StateLeader && leCtx != nil {
					leCancel()
					le.Callbacks.OnStoppedLeading()
				}
				le.CurrentState = StateUnknown
				le.logger.Infof("backup-restore is in: %v", le.CurrentState)
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
				le.logger.Debugf("backup-restore currentState: %v", le.CurrentState)

				// If learner(non-voting member) is present in a cluster(size>1)
				// then promote the learner to voting member.
				if isLearner && le.PromoteCallback != nil {
					metrics.IsLearner.With(prometheus.Labels{}).Set(1)
					le.logger.Info("member is a learner(non-voting) member in the cluster...")
					le.PromoteCallback.Promote(ctx, le.logger)
				}
			}
		}
	}
}

// EtcdMemberStatus checks whether the current instance of backup-restore is leader or not.
// It also returns the boolean indicating the presence of learner(non-voting) member.
func EtcdMemberStatus(ctx context.Context, etcdConnectionConfig *brtypes.EtcdConnectionConfig, etcdConnectionTimeout time.Duration, logger *logrus.Entry) (bool, bool, error) {
	logger.Debug("checking the leadershipStatus...")
	var endPoint string

	factory := etcdutil.NewFactory(*etcdConnectionConfig)
	client, err := factory.NewMaintenance()
	if err != nil {
		return false, false, &errors.EtcdError{
			Message: fmt.Sprintf("failed to create etcd maintenance client: %v", err),
		}
	}
	defer client.Close()

	if len(etcdConnectionConfig.Endpoints) > 0 {
		endPoint = etcdConnectionConfig.Endpoints[0]
	} else {
		return false, false, fmt.Errorf("etcd endpoints are not passed correctly")
	}

	ctx, cancel := context.WithTimeout(ctx, etcdConnectionTimeout)
	defer cancel()

	response, err := client.Status(ctx, endPoint)
	if err != nil {
		logger.Errorf("failed to get status of etcd endPoint: %v with error: %v", endPoint, err)
		return false, false, err
	}

	if response.Header.MemberId == response.Leader {
		return true, false, nil
	} else if response.Leader == NoLeaderState {
		return false, false, &errors.EtcdError{
			Message: fmt.Sprintf("currently there is no etcd leader present may be due to etcd quorum loss or election is being held"),
		}
	} else if response.IsLearner {
		return false, true, nil
	}

	return false, false, nil
}
