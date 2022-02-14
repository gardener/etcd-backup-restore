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

package membergarbagecollector

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/errors"
	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	utils "github.com/gardener/etcd-backup-restore/pkg/snapstore"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/gardener/etcd-backup-restore/pkg/wrappers"
	"github.com/sirupsen/logrus"
	appsv1 "k8s.io/api/apps/v1"
	coordv1 "k8s.io/api/coordination/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	etcdclient "github.com/gardener/etcd-backup-restore/pkg/etcdutil/client"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	podNamespace = "POD_NAMESPACE"
	podName      = "POD_NAME"
)

// MGCChecker garbage collects the members from the etcd cluster if its corresponding pod is not present. (Used for unit tests only)
type MGCChecker interface {
	RemoveSuperfluousMembers(ctx context.Context) error
}

// MemberGarbageCollector contains information to regularly cleanup superfluous ETCD members
type MemberGarbageCollector struct {
	logger                *logrus.Entry
	gcTimer               *time.Timer
	k8sClient             client.Client
	podNamespace          string
	stsName               string
	etcdConnectionTimeout wrappers.Duration
}

// NewMemberGarbageCollector returns the member garbage collect object
func NewMemberGarbageCollector(logger *logrus.Entry, clientSet client.Client, etcdTimeout wrappers.Duration) (*MemberGarbageCollector, error) {
	if clientSet == nil {
		return nil, &errors.EtcdError{
			Message: "nil kubernetes clientset passed, can not create etcd member garbage collector",
		}
	}
	namespace, err := utils.GetEnvVarOrError(podNamespace)
	if err != nil {
		logger.Fatalf("POD_NAMESPACE env var not present: %v", err)
	}
	podName, err := utils.GetEnvVarOrError(podName)
	if err != nil {
		logger.Fatalf("POD_NAME env var not present: %v", err)
	}
	return &MemberGarbageCollector{
		logger:                logger.WithField("actor", "etcd-membergc"),
		k8sClient:             clientSet,
		podNamespace:          namespace,
		stsName:               podName[:strings.LastIndex(podName, "-")],
		etcdConnectionTimeout: etcdTimeout,
	}, nil
}

// RunMemberGarbageCollectorPeriodically periodically calls the member garbage collector
func RunMemberGarbageCollectorPeriodically(ctx context.Context, hconfig *brtypes.HealthConfig, logger *logrus.Entry, etcdConfig *brtypes.EtcdConnectionConfig) {
	clientSet, err := miscellaneous.GetKubernetesClientSetOrError()
	if err != nil {
		logger.Errorf("failed to create kubernetes clientset: %v", err)
		return
	}
	mgc, err := NewMemberGarbageCollector(logger, clientSet, etcdConfig.ConnectionTimeout)
	if err != nil {
		logger.Errorf("failed to initialize new etcd member garbage collector: %v", err)
		return
	}
	mgc.gcTimer = time.NewTimer(hconfig.MemberGCDuration.Duration)
	defer mgc.gcTimer.Stop()
	mgc.logger.Info("Started etcd member garbage collector")

	//Create etcd client to get etcd cluster member list
	clientFactory := etcdutil.NewFactory(*etcdConfig)
	etcdCluster, err := clientFactory.NewCluster()
	if err != nil {
		mgc.logger.Errorf("Failed to create etcd cluster client: %v", err)
	}
	defer etcdCluster.Close()

	for {
		select {
		case <-mgc.gcTimer.C:
			err := mgc.RemoveSuperfluousMembers(ctx, etcdCluster)
			if err != nil {
				mgc.logger.Warn(err)
			}
			mgc.gcTimer.Reset(hconfig.MemberGCDuration.Duration)
		case <-ctx.Done():
			mgc.logger.Info("Stopping etcd member garbage collector")
			return
		}
	}
}

// RemoveSuperfluousMembers removes a member from the etcd cluster if its corresponding pod is not present
func (mgc *MemberGarbageCollector) RemoveSuperfluousMembers(ctx context.Context, etcdCluster etcdclient.ClusterCloser) error {
	sts := &appsv1.StatefulSet{}
	if err := mgc.k8sClient.Get(ctx, client.ObjectKey{
		Namespace: mgc.podNamespace,
		Name:      mgc.stsName,
	}, sts); err != nil {
		return &errors.EtcdError{
			Message: fmt.Sprintf("Could not fetch etcd statefulset %v with error: %v", mgc.stsName, err),
		}
	}
	stsSpecReplicas := *sts.Spec.Replicas
	stsStatusReplicas := sts.Status.Replicas

	listCtx, listCtxCancel := context.WithTimeout(ctx, mgc.etcdConnectionTimeout.Duration)
	defer listCtxCancel()
	etcdMemberListResponse, err := etcdCluster.MemberList(listCtx)
	if err != nil {
		return &errors.EtcdError{
			Message: fmt.Sprintf("Could not list etcd cluster members: %v", err),
		}
	}

	if stsSpecReplicas >= stsStatusReplicas && stsSpecReplicas >= int32(len(etcdMemberListResponse.Members)) {
		//Return if number of replicas in sts and etcd cluster match or cluster is in scale up scenario
		return nil
	}

	for _, member := range etcdMemberListResponse.Members {
		memberPod := &v1.Pod{}
		podErr := mgc.k8sClient.Get(ctx, client.ObjectKey{
			Namespace: mgc.podNamespace,
			Name:      member.Name,
		}, memberPod)
		memberLease := &coordv1.Lease{}
		leaseErr := mgc.k8sClient.Get(ctx, client.ObjectKey{
			Namespace: mgc.podNamespace,
			Name:      member.Name,
		}, memberLease)
		if (apierrors.IsNotFound(podErr) && apierrors.IsNotFound(leaseErr)) || member.Name == "" {
			//Remove member from etcd cluster if corresponding pod cannot be found and if corresponding member lease is not found
			//Or remove a learner member
			removeCtx, removeCtxCancel := context.WithTimeout(ctx, mgc.etcdConnectionTimeout.Duration)
			defer removeCtxCancel()
			if _, err = etcdCluster.MemberRemove(removeCtx, member.ID); err != nil {
				return &errors.EtcdError{
					Message: fmt.Sprintf("Error removing member %v from the cluster: %v", member.Name, err),
				}
			}
			mgc.logger.Info("Removed superfluous member ", member.Name, ":", member.ID)
		}
	}
	return nil
}
