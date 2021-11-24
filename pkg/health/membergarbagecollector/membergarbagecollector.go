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

	etcdclient "github.com/gardener/etcd-backup-restore/pkg/etcdutil/client"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	podNamespace = "POD_NAMESPACE"
	podName      = "POD_NAME"
)

// MGCChecker is an interface used to unit test functions in this package
type MGCChecker interface {
	RemoveSuperfluousMembers(ctx context.Context) error
}

// MemberGarbageCollector contains information to regularly cleanup superfluous ETCD members
type MemberGarbageCollector struct {
	logger       *logrus.Entry
	gcTimer      *time.Timer
	k8sClient    client.Client
	podNamespace string
	etcdName     string
}

func init() {
	druidv1alpha1.AddToScheme(scheme.Scheme)
}

// NewMemberGarbageCollector returns the member garbage collect object
func NewMemberGarbageCollector(logger *logrus.Entry /*etcdConfig *brtypes.EtcdConnectionConfig,*/, clientSet client.Client) (*MemberGarbageCollector, error) {
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
	return &MemberGarbageCollector{
		logger:       logger.WithField("actor", "membergc"),
		k8sClient:    clientSet,
		podNamespace: namespace,
		etcdName:     podName[:strings.LastIndex(podName, "-")], //TODO Evaluate if this will always be sufficient
	}, nil
}

// RunMemberGarbageCollectorPeriodically periodically calls the member garbage collector
func RunMemberGarbageCollectorPeriodically(ctx context.Context, hconfig *brtypes.HealthConfig, logger *logrus.Entry, etcdConfig *brtypes.EtcdConnectionConfig) {

	clientSet, err := miscellaneous.GetKubernetesClientSetOrError()
	if err != nil {
		logger.Errorf("failed to create kubernetes clientset: %v", err)
		return
	}
	mgc, err := NewMemberGarbageCollector(logger, clientSet)
	if err != nil {
		logger.Errorf("failed to initialize new heartbeat: %v", err)
		return
	}
	mgc.gcTimer = time.NewTimer(hconfig.MemberGCDuration.Duration)
	defer mgc.gcTimer.Stop()
	mgc.logger.Info("Started etcd member garbage collector")

	for {
		//Create etcd client to get etcd cluster member list
		clientFactory := etcdutil.NewFactory(*etcdConfig)
		etcdCluster, err := clientFactory.NewCluster()
		if err != nil {
			mgc.logger.Errorf("Failed to create etcd cluster client: %v", err)
		}
		defer etcdCluster.Close()
		select {
		case <-mgc.gcTimer.C:
			err := mgc.RemoveSuperfluousMembers(ctx, etcdCluster)
			if err != nil {
				mgc.logger.Warn(err)
			}
			mgc.gcTimer.Reset(hconfig.MemberGCDuration.Duration)
		case <-ctx.Done():
			mgc.logger.Info("Stopped etcd member garbage collector")
			etcdCluster.Close()
			return
		}
	}
}

// RemoveSuperfluousMembers removes a member from the etcd cluster if its corresponding pod is not present
func (mgc *MemberGarbageCollector) RemoveSuperfluousMembers(ctx context.Context, etcdCluster etcdclient.ClusterCloser) error {
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

	etcdMemberListResponse, err := etcdCluster.MemberList(ctx)
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
			if _, err = etcdCluster.MemberRemove(ctx, member.ID); err != nil {
				return &errors.EtcdError{
					Message: fmt.Sprintf("Error removing member %v from the cluster: %v", member.Name, err),
				}
			}
			mgc.logger.Info("Removed superfluous member ", member.Name)
		}
	}
	return nil
}
