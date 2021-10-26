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
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/errors"
	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	utils "github.com/gardener/etcd-backup-restore/pkg/snapstore"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	podNamespace = "POD_NAMESPACE"
)

// MemberGarbageCollector contains information to regularly cleanup superfluous ETCD members
type MemberGarbageCollector struct {
	logger       *logrus.Entry
	gcTimer      *time.Timer
	etcdConfig   *brtypes.EtcdConnectionConfig
	k8sClient    client.Client
	podNamespace string
}

// NewMemberGarbageCollector returns the member garbage collect object
func NewMemberGarbageCollector(logger *logrus.Entry, etcdConfig *brtypes.EtcdConnectionConfig, clientSet client.Client) (*MemberGarbageCollector, error) {
	if etcdConfig == nil {
		return nil, &errors.EtcdError{
			Message: "nil etcd config passed, can not create heartbeat",
		}
	}
	if clientSet == nil {
		return nil, &errors.EtcdError{
			Message: "nil kubernetes clientset passed, can not create heartbeat",
		}
	}
	namespace, err := utils.GetEnvVarOrError(podNamespace)
	if err != nil {
		logger.Fatalf("POD_NAMESPACE env var not present: %v", err)
	}
	return &MemberGarbageCollector{
		logger:       logger.WithField("actor", "membergc"),
		etcdConfig:   etcdConfig,
		k8sClient:    clientSet,
		podNamespace: namespace,
	}, nil
}

// RunMemberGarbageCollectorPeriodically periodically calls the member garbage collector
func RunMemberGarbageCollectorPeriodically(ctx context.Context, hconfig *brtypes.HealthConfig, logger *logrus.Entry, etcdConfig *brtypes.EtcdConnectionConfig) {

	clientSet, err := miscellaneous.GetKubernetesClientSetOrError()
	if err != nil {
		logger.Errorf("failed to create kubernetes clientset: %v", err)
		return
	}
	mgc, err := NewMemberGarbageCollector(logger, etcdConfig, clientSet)
	if err != nil {
		logger.Errorf("failed to initialize new heartbeat: %v", err)
		return
	}
	mgc.gcTimer = time.NewTimer(hconfig.MemberGCDuration.Duration)
	defer mgc.gcTimer.Stop()
	mgc.logger.Info("Started etcd member garbage collection timer")

	for {
		select {
		case <-mgc.gcTimer.C:
			err := mgc.RemoveSuperfluousMembers(ctx)
			if err != nil {
				mgc.logger.Warn(err)
			}
			mgc.gcTimer.Reset(hconfig.MemberGCDuration.Duration)
		case <-ctx.Done():
			mgc.logger.Info("Stopped etcd member garbage collection timer")
			return
		}
	}
}

// RemoveSuperfluousMembers removes a member from the etcd cluster if its corresponding pod is not present
func (mgc *MemberGarbageCollector) RemoveSuperfluousMembers(ctx context.Context) error {
	//TODO
	//To be done only if memberlist size > desired cluster size

	//Create etcd client to get etcd cluster member list
	etcdClient, err := etcdutil.GetTLSClientForEtcd(mgc.etcdConfig)
	if err != nil {
		return &errors.EtcdError{
			Message: fmt.Sprintf("Failed to create etcd client: %v", err),
		}
	}
	defer etcdClient.Close()

	etcdMemberListResponse, err := etcdClient.MemberList(ctx)
	if err != nil {
		return &errors.EtcdError{
			Message: fmt.Sprintf("Could not list etcd cluster members: %v", err),
		}
	}
	//etcdMemberList := etcdMemberListResponse.Members
	for _, x := range etcdMemberListResponse.Members {
		memberPod := &v1.Pod{}
		err := mgc.k8sClient.Get(ctx, client.ObjectKey{
			Namespace: mgc.podNamespace,
			Name:      x.Name,
		}, memberPod)
		if apierrors.IsNotFound(err) {
			//Remove member from etcd cluster if corresponding pod cannot be found
			mgc.logger.Info("Removing superfluous member ", x.ID, " : ", err)
			etcdClient.MemberRemove(ctx, x.ID)
		}
	}
	return nil
}
