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
	druidv1alpha1 "github.com/gardener/etcd-druid/api/v1alpha1"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	podNamespace = "POD_NAMESPACE"
	podName      = "POD_NAME"
)

// MemberGarbageCollector contains information to regularly cleanup superfluous ETCD members
type MemberGarbageCollector struct {
	logger       *logrus.Entry
	gcTimer      *time.Timer
	etcdConfig   *brtypes.EtcdConnectionConfig
	k8sClient    client.Client
	podNamespace string
	etcdName     string
}

func init() {
	druidv1alpha1.AddToScheme(scheme.Scheme)
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
	podName, err := utils.GetEnvVarOrError(podName)
	if err != nil {
		logger.Fatalf("POD_NAME env var not present: %v", err)
	}
	// druidv1alpha1.AddToScheme(scheme.Scheme)
	return &MemberGarbageCollector{
		logger:       logger.WithField("actor", "membergc"),
		etcdConfig:   etcdConfig,
		k8sClient:    clientSet,
		podNamespace: namespace,
		etcdName:     podName[:strings.LastIndex(podName, "-")],
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
	mgc.logger.Info("Started etcd member garbage collector")

	for {
		select {
		case <-mgc.gcTimer.C:
			err := mgc.RemoveSuperfluousMembers(ctx)
			if err != nil {
				mgc.logger.Warn(err)
			}
			mgc.gcTimer.Reset(hconfig.MemberGCDuration.Duration)
		case <-ctx.Done():
			mgc.logger.Info("Stopped etcd member garbage collector")
			return
		}
	}
}

// RemoveSuperfluousMembers removes a member from the etcd cluster if its corresponding pod is not present
func (mgc *MemberGarbageCollector) RemoveSuperfluousMembers(ctx context.Context) error {
	mgc.logger.Warnf("IN MEMBER GC")
	etcd := &druidv1alpha1.Etcd{}
	if err := mgc.k8sClient.Get(ctx, client.ObjectKey{
		Namespace: mgc.podNamespace,
		Name:      mgc.etcdName,
	}, etcd); err != nil {
		return &errors.EtcdError{
			Message: fmt.Sprintf("Could not fetch etcd %v with error: %v", mgc.etcdName, err),
		}
	}
	// Return if no etcd member present
	if len(etcd.Status.Members) < 1 {
		return &errors.EtcdError{
			Message: "There are no members present in the etcd status. Nothing to clean",
		}
	}

	// Return if memberlist size <= desired cluster size. Cleanup not needed
	if len(etcd.Status.Members) <= etcd.Spec.Replicas {
		return nil
	}

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

	//Create a map of members in the etcd status
	statusMembers := make(map[string]bool)
	for _, member := range etcd.Status.Members {
		statusMembers[member.Name] = true
	}

	for _, member := range etcdMemberListResponse.Members {
		memberPod := &v1.Pod{}
		err := mgc.k8sClient.Get(ctx, client.ObjectKey{
			Namespace: mgc.podNamespace,
			Name:      member.Name,
		}, memberPod)
		if apierrors.IsNotFound(err) && !statusMembers[member.Name] {
			//Remove member from etcd cluster if corresponding pod cannot be found and if member not present in etcd.Status.Members
			mgc.logger.Info("Removing superfluous member ", member.ID)
			if _, err = etcdClient.MemberRemove(ctx, member.ID); err != nil {
				return &errors.EtcdError{
					Message: fmt.Sprintf("Error removing member %v from the cluster: %v", member.ID, err),
				}
			}
		}
	}
	return nil
}
