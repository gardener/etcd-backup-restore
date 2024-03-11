// Copyright (c) 2023 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
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

package member

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	utilError "github.com/gardener/etcd-backup-restore/pkg/errors"
	"github.com/gardener/etcd-backup-restore/pkg/etcdaccess"
	"github.com/gardener/etcd-backup-restore/pkg/metrics"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
	"go.etcd.io/etcd/etcdserver/etcdserverpb"
	v1 "k8s.io/api/coordination/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// RetryPeriod is the peroid after which an operation is retried
	RetryPeriod = 2 * time.Second

	// RetrySteps is the no. of steps for exponential backoff.
	RetrySteps = 4

	// EtcdTimeout is timeout for etcd operations
	EtcdTimeout = 5 * time.Second
)

var (
	// ErrMissingMember is a sentient error to describe a case of a member being missing from the member list
	ErrMissingMember = errors.New("member missing from member list")
)

// Control interface defines the functionalities needed to manipulate a member in the etcd cluster
type Control interface {
	// AddMemberAsLearner add a new member as a learner to the etcd cluster
	AddMemberAsLearner(context.Context) error

	// IsClusterScaledUp determines whether a etcd cluster is getting scale-up or not.
	IsClusterScaledUp(context.Context, client.Client) (bool, error)

	// IsMemberInCluster checks is the current members peer URL is already part of the etcd cluster.
	IsMemberInCluster(context.Context) (bool, error)

	// WasMemberInCluster checks whether current members was part of the etcd cluster or not.
	WasMemberInCluster(context.Context, client.Client) bool

	// PromoteMember promotes an etcd member from a learner to a voting member of the cluster.
	// This will succeed if and only if learner is in a healthy state and the learner is in sync with leader.
	PromoteMember(context.Context) error

	// UpdateMemberPeerURL updates the passed in peer URL of the member.
	UpdateMemberPeerURL(context.Context, etcdaccess.ClusterCloser, string) error

	// RemoveMember removes the member from the etcd cluster.
	RemoveMember(context.Context) error

	// IsLearnerPresent checks for the learner(non-voting) member in a cluster.
	IsLearnerPresent(context.Context) (bool, error)

	// GetMemberPeerURL returns the current peer URL of the member.
	GetMemberPeerURL(context.Context) (string, error)
}

// memberControl holds the configuration for the mechanism of adding a new member to the cluster.
type memberControl struct {
	clientFactory etcdaccess.Factory
	logger        logrus.Entry
	podName       string
	podNamespace  string
}

// NewMemberControl returns new ExponentialBackoff.
func NewMemberControl(etcdConnConfig *brtypes.EtcdConnectionConfig) Control {
	logger := logrus.New().WithField("actor", "member-add")
	etcdConn := *etcdConnConfig

	// We want to use the service endpoint since we're only supposed to connect to ready etcd members.
	clientFactory := etcdaccess.NewFactory(etcdConn, etcdaccess.UseServiceEndpoints(true))
	podName, err := miscellaneous.GetEnvVarOrError("POD_NAME")
	if err != nil {
		logger.Fatalf("Error reading POD_NAME env var : %v", err)
	}

	podNamespace, err := miscellaneous.GetEnvVarOrError("POD_NAMESPACE")
	if err != nil {
		logger.Fatalf("Error reading POD_NAMESPACE env var : %v", err)
	}

	return &memberControl{
		clientFactory: clientFactory,
		logger:        *logger,
		podName:       podName,
		podNamespace:  podNamespace,
	}
}

// AddMemberAsLearner add a member as a learner to the etcd cluster
func (m *memberControl) AddMemberAsLearner(ctx context.Context) error {
	//Add member as learner to cluster
	memberPeerURL, err := miscellaneous.GetMemberPeerURLFromConfig()
	if err != nil {
		m.logger.Fatalf("Error fetching etcd member URL : %v", err)
	}

	cli, err := m.clientFactory.NewCluster()
	if err != nil {
		return fmt.Errorf("failed to build etcd cluster client : %v", err)
	}
	defer func() {
		_ = cli.Close()
	}()

	memAddCtx, cancel := context.WithTimeout(ctx, EtcdTimeout)
	defer cancel()
	start := time.Now()
	response, err := cli.MemberAddAsLearner(memAddCtx, []string{memberPeerURL})
	if err != nil {
		if errors.Is(err, rpctypes.Error(rpctypes.ErrGRPCPeerURLExist)) || errors.Is(err, rpctypes.Error(rpctypes.ErrGRPCMemberExist)) {
			m.logger.Infof("Member %s already part of etcd cluster", memberPeerURL)
			return nil
		} else if errors.Is(err, rpctypes.Error(rpctypes.ErrGRPCTooManyLearners)) {
			m.logger.Infof("Unable to add member %s as a learner because the cluster already has a learner", m.podName)
			return rpctypes.Error(rpctypes.ErrGRPCTooManyLearners)
		}
		metrics.IsLearnerCountTotal.With(prometheus.Labels{metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Inc()
		metrics.AddLearnerDurationSeconds.With(prometheus.Labels{metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Observe(time.Since(start).Seconds())
		return fmt.Errorf("error while adding member as a learner: %v", err)
	}

	metrics.IsLearnerCountTotal.With(prometheus.Labels{metrics.LabelSucceeded: metrics.ValueSucceededTrue}).Inc()
	metrics.IsLearner.With(prometheus.Labels{}).Set(1)
	metrics.AddLearnerDurationSeconds.With(prometheus.Labels{metrics.LabelSucceeded: metrics.ValueSucceededTrue}).Observe(time.Since(start).Seconds())
	m.logger.Infof("Added member %v to cluster as a learner", strconv.FormatUint(response.Member.GetID(), 16))
	return nil
}

// IsMemberInCluster checks is the current members peer URL is already part of the etcd cluster
func (m *memberControl) IsMemberInCluster(ctx context.Context) (bool, error) {
	m.logger.Infof("Checking if member %s is part of a running cluster", m.podName)
	// Check if an etcd is already available

	backoff := miscellaneous.CreateBackoff(RetryPeriod, RetrySteps)
	err := retry.OnError(backoff, func(err error) bool {
		return err != nil
	}, func() error {
		etcdProbeCtx, cancel := context.WithTimeout(ctx, EtcdTimeout)
		defer cancel()
		return miscellaneous.ProbeEtcd(etcdProbeCtx, m.clientFactory, &m.logger)
	})
	if err != nil {
		return false, err
	}

	cli, err := m.clientFactory.NewCluster()
	if err != nil {
		m.logger.Errorf("failed to build etcd cluster client")
		return false, err
	}
	defer func() {
		if err = cli.Close(); err != nil {
			m.logger.Errorf("error while closing etcd client : %v", err)
		}
	}()

	// List members in cluster
	var etcdMemberList *clientv3.MemberListResponse
	err = retry.OnError(backoff, func(err error) bool {
		return err != nil
	}, func() error {
		memListCtx, cancel := context.WithTimeout(ctx, EtcdTimeout)
		defer cancel()
		etcdMemberList, err = cli.MemberList(memListCtx)
		return err
	})
	if err != nil {
		return false, fmt.Errorf("could not list any etcd members %w", err)
	}

	for _, member := range etcdMemberList.Members {
		if member.GetName() == m.podName {
			m.logger.Infof("Member %s part of running cluster", m.podName)
			return true, nil
		}
	}

	m.logger.Infof("Member %v not part of any running cluster", m.podName)
	m.logger.Infof("Could not find member %v in the list", m.podName)
	return false, nil
}

// PromoteMember promotes an etcd member from a learner to a voting member of the cluster. This will succeed only if its logs are caught up with the leader
func (m *memberControl) PromoteMember(ctx context.Context) error {
	m.logger.Infof("Attempting to promote member %s", m.podName)
	cli, err := m.clientFactory.NewCluster()
	if err != nil {
		return fmt.Errorf("failed to build etcd cluster client : %v", err)
	}
	defer func() {
		if err = cli.Close(); err != nil {
			m.logger.Errorf("error while closing etcd client : %v", err)
		}
	}()

	//List all members in the etcd cluster
	//Member URL will appear in the memberlist call response as soon as the member has been added to the cluster as a learner
	//However, the name of the member will appear only if the member has started running
	memListCtx, memListCtxCancel := context.WithTimeout(ctx, brtypes.DefaultEtcdConnectionTimeout)
	etcdList, err := cli.MemberList(memListCtx)
	defer memListCtxCancel()

	if err != nil {
		return fmt.Errorf("error listing members: %v", err)
	}

	foundMember := findMember(etcdList.Members, m.podName)
	if foundMember == nil {
		return ErrMissingMember
	}

	return miscellaneous.DoPromoteMember(ctx, foundMember, cli, &m.logger)
}

func findMember(existingMembers []*etcdserverpb.Member, memberName string) *etcdserverpb.Member {
	for _, member := range existingMembers {
		if member.GetName() == memberName {
			return member
		}
	}
	return nil
}

// UpdateMemberPeerURL updates the peer address of a specified etcd cluster member.
func (m *memberControl) UpdateMemberPeerURL(ctx context.Context, cli etcdaccess.ClusterCloser, peerURL string) error {
	m.logger.Infof("Attempting to update the member Info: %v", m.podName)
	ctx, cancel := context.WithTimeout(ctx, brtypes.DefaultEtcdConnectionTimeout)
	defer cancel()

	membersInfo, err := cli.MemberList(ctx)
	if err != nil {
		return fmt.Errorf("error listing members: %w", err)
	}

	memberID := membersInfo.Header.GetMemberId()
	if _, err = cli.MemberUpdate(ctx, memberID, []string{peerURL}); err != nil {
		return fmt.Errorf("error updating member peer URL: %w", err)
	}
	m.logger.Infof("Successfully updated the member peer URL %s", peerURL)
	return nil
}

// RemoveMember removes the member from the etcd cluster.
func (m *memberControl) RemoveMember(ctx context.Context) error {
	m.logger.Infof("Removing the %s member from cluster", m.podName)

	cli, err := m.clientFactory.NewCluster()
	if err != nil {
		return fmt.Errorf("failed to build etcd cluster client : %v", err)
	}
	defer func() {
		if err = cli.Close(); err != nil {
			m.logger.Errorf("error while closing etcd client : %v", err)
		}
	}()

	memRemoveCtx, memRemoveCtxCancel := context.WithTimeout(ctx, brtypes.DefaultEtcdConnectionTimeout)
	defer memRemoveCtxCancel()

	memberInfo, err := cli.MemberList(memRemoveCtx)
	if err != nil {
		return fmt.Errorf("error listing members: %v", err)
	}

	foundMember := findMember(memberInfo.Members, m.podName)
	if foundMember == nil {
		return nil
	}

	return miscellaneous.RemoveMemberFromCluster(memRemoveCtx, cli, foundMember.GetID(), &m.logger)
}

// IsLearnerPresent checks for the learner(non-voting) member in a cluster.
func (m *memberControl) IsLearnerPresent(ctx context.Context) (bool, error) {
	m.logger.Infof("checking the presence of a learner in a cluster...")

	cli, err := m.clientFactory.NewCluster()
	if err != nil {
		return false, fmt.Errorf("failed to build etcd cluster client : %v", err)
	}
	defer func() {
		if err = cli.Close(); err != nil {
			m.logger.Errorf("error while closing etcd client : %v", err)
		}
	}()

	learnerCtx, learnerCtxCancel := context.WithTimeout(ctx, brtypes.DefaultEtcdConnectionTimeout)
	defer learnerCtxCancel()

	return miscellaneous.CheckIfLearnerPresent(learnerCtx, cli)
}

// IsClusterScaledUp determines whether a etcd cluster is getting scale-up or not and returns a boolean
func (m *memberControl) IsClusterScaledUp(ctx context.Context, clientSet client.Client) (bool, error) {
	m.logger.Info("Checking whether etcd cluster is marked for scale-up")

	// First, try to determine scale-up case by checking whether member is already part of cluster or not.
	// In case of failure in checking the presence of member in a cluster then
	// check for `ScaledToMultiNodeAnnotationKey` annotation in etcd statefulset.

	if isEtcdMemberPresent, err := m.IsMemberInCluster(ctx); err != nil {
		m.logger.Errorf("unable to check presence of member in cluster: %v", err)
	} else {
		return !isEtcdMemberPresent, nil
	}

	etcdsts, err := miscellaneous.GetStatefulSet(ctx, clientSet, m.podNamespace, m.podName)
	if err != nil {
		m.logger.Errorf("unable to fetch etcd statefulset: %v", err)
	} else {
		if miscellaneous.IsAnnotationPresent(etcdsts, miscellaneous.ScaledToMultiNodeAnnotationKey) {
			return true, nil
		}
	}
	return false, nil
}

// WasMemberInCluster checks whether etcd member was part of etcd cluster.
func (m *memberControl) WasMemberInCluster(ctx context.Context, clientSet client.Client) bool {

	if etcdMemberPresent, err := m.IsMemberInCluster(ctx); err != nil {
		m.logger.Errorf("unable to check member presence via api call: %v", err)
	} else {
		return etcdMemberPresent
	}

	m.logger.Info("fetching the member lease associated with etcd member")
	memberLease := &v1.Lease{}
	if err := clientSet.Get(ctx, client.ObjectKey{
		Namespace: m.podNamespace,
		Name:      m.podName,
	}, memberLease); err != nil {
		m.logger.Errorf("couldn't fetch member lease while checking if the member was part of the cluster: %v", err)
		return false
	}

	if memberLease.Spec.HolderIdentity == nil {
		return false
	}
	return true
}

func (m *memberControl) GetMemberPeerURL(ctx context.Context) (string, error) {
	cli, err := m.clientFactory.NewCluster()
	if err != nil {
		return "", fmt.Errorf("failed to create etcd cluster client : %w", err)
	}
	memberListResp, err := cli.MemberList(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to list members : %w", err)
	}
	foundMember := findMember(memberListResp.Members, m.podName)
	if foundMember == nil {
		return "", fmt.Errorf("failed to determine peer URL: %w", ErrMissingMember)
	}
	return foundMember.GetPeerURLs()[0], nil
}

// AddLearnerWithRetry add a new member as a learner with exponential backoff.
func AddLearnerWithRetry(ctx context.Context, m Control, retrySteps int, dataDir string) error {
	backoff := miscellaneous.CreateBackoff(RetryPeriod, retrySteps)

	return retry.OnError(backoff, utilError.IsErrNotNil, func() error {
		// Remove data-dir(if exist) before adding a learner as a additional safety check.
		if err := miscellaneous.RemoveDir(dataDir); err != nil {
			return err
		}
		if err := m.AddMemberAsLearner(ctx); err != nil {
			if errors.Is(err, rpctypes.Error(rpctypes.ErrGRPCTooManyLearners)) {
				if err = miscellaneous.SleepWithContext(ctx, EtcdTimeout); err != nil {
					return err
				}
			}
			return err
		}
		return nil
	})
}
