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

package heartbeat

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/errors"
	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	utils "github.com/gardener/etcd-backup-restore/pkg/snapstore"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	podName      = "POD_NAME"
	podNamespace = "POD_NAMESPACE"
	// PeerURLTLSEnabledKey is the name of the annotation that will be added to the lease and will indicate whether TLS has been enabled for peer URL
	PeerURLTLSEnabledKey = "member.etcd.gardener.cloud/tls-enabled"
)

// Heartbeat contains information to perform regular heart beats in a Kubernetes cluster.
type Heartbeat struct {
	logger         *logrus.Entry
	heartbeatTimer *time.Timer
	etcdConfig     *brtypes.EtcdConnectionConfig
	k8sClient      client.Client
	podName        string
	podNamespace   string
	metadata       map[string]string // metadata is currently added as annotations to the k8s lease object
}

// NewHeartbeat returns the heartbeat object.
func NewHeartbeat(logger *logrus.Entry, etcdConfig *brtypes.EtcdConnectionConfig, clientSet client.Client, metadata map[string]string) (*Heartbeat, error) {
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
	memberName, err := utils.GetEnvVarOrError(podName)
	if err != nil {
		logger.Fatalf("POD NAME env var not present: %v", err)
	}
	namespace, err := utils.GetEnvVarOrError(podNamespace)
	if err != nil {
		logger.Fatalf("POD_NAMESPACE env var not present: %v", err)
	}
	return &Heartbeat{
		logger:       logger.WithField("actor", "heartbeat"),
		etcdConfig:   etcdConfig,
		k8sClient:    clientSet,
		podName:      memberName,
		podNamespace: namespace,
		metadata:     metadata,
	}, nil
}

// RenewMemberLease renews the member lease under the pod's identity.
func (hb *Heartbeat) RenewMemberLease(ctx context.Context) error {
	if hb.k8sClient == nil {
		return &errors.EtcdError{
			Message: "nil clientset passed",
		}
	}

	ctx, cancel := context.WithTimeout(ctx, hb.etcdConfig.ConnectionTimeout.Duration)
	defer cancel()

	//Fetch lease associated with member
	memberLease := &v1.Lease{}
	err := hb.k8sClient.Get(ctx, client.ObjectKey{
		Namespace: hb.podNamespace,
		Name:      hb.podName,
	}, memberLease)
	if err != nil {
		return &errors.EtcdError{
			Message: fmt.Sprintf("Could not fetch member lease: %v", err),
		}
	}

	//Create etcd client maintenance to get etcd ID
	clientFactory := etcdutil.NewFactory(*hb.etcdConfig)
	etcdClient, err := clientFactory.NewMaintenance()
	if err != nil {
		return &errors.EtcdError{
			Message: fmt.Sprintf("Failed to create etcd maintenance client: %v", err),
		}
	}
	defer etcdClient.Close()

	response, err := etcdClient.Status(ctx, hb.etcdConfig.Endpoints[0])
	if err != nil {
		return &errors.EtcdError{
			Message: fmt.Sprintf("Failed to get status of etcd endPoint [ %v ] with error: %v", hb.etcdConfig.Endpoints[0], err),
		}
	}

	memberID := strconv.FormatUint(response.Header.GetMemberId(), 16)
	if response.Header.GetMemberId() == response.Leader {
		memberID += ":Leader"
	} else {
		memberID += ":Member"
	}

	//Change HolderIdentity and RenewTime of lease
	renewedMemberLease := memberLease.DeepCopy()
	renewedMemberLease.Spec.HolderIdentity = &memberID
	renewedTime := time.Now()
	renewedMemberLease.Spec.RenewTime = &metav1.MicroTime{Time: renewedTime}
	// Update only keys from metadata
	if renewedMemberLease.Annotations == nil {
		renewedMemberLease.Annotations = map[string]string{}
	}
	for k, v := range hb.metadata {
		renewedMemberLease.Annotations[k] = v
	}

	err = hb.k8sClient.Patch(ctx, renewedMemberLease, client.MergeFrom(memberLease))
	if err != nil {
		return &errors.EtcdError{
			Message: fmt.Sprintf("Failed to renew member lease: %v", err),
		}
	}

	hb.logger.Debugf("Renewed member lease for etcd memberID: %v", memberID)

	return nil
}

// UpdateFullSnapshotLease renews the full snapshot lease and updates the holderIdentity field with the last revision in the latest full snapshot
func UpdateFullSnapshotLease(ctx context.Context, logger *logrus.Entry, fullSnapshot *brtypes.Snapshot, k8sClientset client.Client, fullSnapshotLeaseName string) error {
	if k8sClientset == nil {
		return &errors.EtcdError{
			Message: "nil clientset passed",
		}
	}

	if fullSnapshot == nil {
		return &errors.EtcdError{
			Message: "can not update full snapshot lease, nil snapshot passed",
		}
	}

	namespace, err := utils.GetEnvVarOrError(podNamespace)
	if err != nil {
		return &errors.EtcdError{
			Message: fmt.Sprintf("Pod namespace env var not present: %v", err),
		}
	}

	// Retry on conflict is necessary because multiple actors update the full snapshot lease.
	if err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		fullSnapLease := &v1.Lease{}
		if err := k8sClientset.Get(ctx, client.ObjectKey{
			Namespace: namespace,
			Name:      fullSnapshotLeaseName,
		}, fullSnapLease); err != nil {
			return &errors.EtcdError{
				Message: fmt.Sprintf("Failed to fetch full snapshot lease: %v", err),
			}
		}

		var rev int64
		logString := "Renewed full snapshot lease"

		// Do not renew lease if revision in existing Lease is already higher.
		if fullSnapLease.Spec.HolderIdentity != nil {
			rev, err = strconv.ParseInt(*fullSnapLease.Spec.HolderIdentity, 10, 64)
			if err != nil {
				return err
			}
			if rev > fullSnapshot.LastRevision {
				return nil
			}
		}

		renewedLease := fullSnapLease.DeepCopy()
		renewedTime := time.Now()
		renewedLease.Spec.RenewTime = &metav1.MicroTime{Time: renewedTime}
		// Update revisions in fullSnapLease.Spec.HolderIdentity only when its value is less than latest fullSnap.LastRevision
		if fullSnapLease.Spec.HolderIdentity == nil || rev < fullSnapshot.LastRevision {
			actor := strconv.FormatInt(fullSnapshot.LastRevision, 10)
			renewedLease.Spec.HolderIdentity = &actor
			logString += " with revision " + actor
		}

		if err := k8sClientset.Patch(ctx, renewedLease, client.MergeFromWithOptions(fullSnapLease, &client.MergeFromWithOptimisticLock{})); err != nil {
			return err
		}

		logger.Info(logString, " at time ", renewedTime)
		return nil
	}); err != nil {
		return &errors.EtcdError{
			Message: fmt.Sprintf("Failed to update full snapshot lease: %v", err),
		}
	}

	return nil
}

// UpdateDeltaSnapshotLease renews delta snapshot lease and updates the holderIdentity field with the total number or revisions stored in delta snapshots since the last full snapshot was taken
func UpdateDeltaSnapshotLease(ctx context.Context, logger *logrus.Entry, prevDeltaSnapshots brtypes.SnapList, k8sClientset client.Client, deltaSnapshotLeaseName string) error {
	if k8sClientset == nil {
		return &errors.EtcdError{
			Message: "nil clientset passed",
		}
	}

	namespace, err := utils.GetEnvVarOrError(podNamespace)
	if err != nil {
		return &errors.EtcdError{
			Message: fmt.Sprintf("Pod namespace env var not present: %v", err),
		}
	}

	logString := "Renewed delta snapshot lease"

	deltaSnapLease := &v1.Lease{}
	if err := k8sClientset.Get(ctx, client.ObjectKey{
		Namespace: namespace,
		Name:      deltaSnapshotLeaseName,
	}, deltaSnapLease); err != nil {
		return &errors.EtcdError{
			Message: fmt.Sprintf("Failed to fetch delta snapshot lease: %v", err),
		}
	}

	renewedLease := deltaSnapLease.DeepCopy()
	renewedTime := time.Now()
	renewedLease.Spec.RenewTime = &metav1.MicroTime{Time: renewedTime}

	// Update revisions in deltaSnapLease.Spec.HolderIdentity only when its value is less than latest deltaSnap.LastRevision
	if len(prevDeltaSnapshots) > 0 {
		deltaSnap := prevDeltaSnapshots[len(prevDeltaSnapshots)-1]
		var deltaRev int64
		if deltaSnapLease.Spec.HolderIdentity != nil {
			deltaRev, err = strconv.ParseInt(*deltaSnapLease.Spec.HolderIdentity, 10, 64)
			if err != nil {
				return err
			}
		}
		if deltaSnapLease.Spec.HolderIdentity == nil || deltaRev < deltaSnap.LastRevision {
			actor := strconv.FormatInt(deltaSnap.LastRevision, 10)
			renewedLease.Spec.HolderIdentity = &actor
			logString += " with revision " + actor
		}
	} else if deltaSnapLease.Spec.HolderIdentity == nil {
		// Special case: Set revisions in deltaSnapLease.Spec.HolderIdentity to 0 if no delta snaps taken at all
		actor := strconv.FormatInt(0, 10)
		renewedLease.Spec.HolderIdentity = &actor
	}

	err = k8sClientset.Patch(ctx, renewedLease, client.MergeFrom(deltaSnapLease))
	if err != nil {
		return &errors.EtcdError{
			Message: fmt.Sprintf("Failed to update delta snapshot lease: %v", err),
		}
	}
	logger.Info(logString, " at time ", renewedTime)

	return nil
}

// FullSnapshotCaseLeaseUpdate Updates the fullsnapshot lease and the deltasnapshot lease as needed when a full snapshot is taken
func FullSnapshotCaseLeaseUpdate(ctx context.Context, logger *logrus.Entry, fullSnapshot *brtypes.Snapshot, k8sClientset client.Client, fullSnapshotLeaseName string, deltaSnapshotLeaseName string) error {
	if err := UpdateFullSnapshotLease(ctx, logger, fullSnapshot, k8sClientset, fullSnapshotLeaseName); err != nil {
		return &errors.EtcdError{
			Message: fmt.Sprintf("Failed to update full snapshot lease: %v", err),
		}
	}

	return nil
}

// DeltaSnapshotCaseLeaseUpdate Updates the fullsnapshot lease and the deltasnapshot lease as needed when a delta snapshot is taken
func DeltaSnapshotCaseLeaseUpdate(ctx context.Context, logger *logrus.Entry, k8sClientset client.Client, deltaSnapshotLeaseName string, store brtypes.SnapStore) error {
	_, latestDeltaSnapshotList, err := miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
	if err == nil {
		if err = UpdateDeltaSnapshotLease(ctx, logger, latestDeltaSnapshotList, k8sClientset, deltaSnapshotLeaseName); err != nil {
			return &errors.EtcdError{
				Message: fmt.Sprintf("Failed to update delta snapshot lease with error: %v", err),
			}
		}
	} else {
		return &errors.EtcdError{
			Message: fmt.Sprintf("failed to get latest snapshots from store with error: %v", err),
		}
	}
	return nil
}

// RenewMemberLeasePeriodically has a timer and will periodically call RenewMemberLeases to renew the member lease until stopped
func RenewMemberLeasePeriodically(ctx context.Context, stopCh chan struct{}, hconfig *brtypes.HealthConfig, logger *logrus.Entry, etcdConfig *brtypes.EtcdConnectionConfig, peerURLTLSEnabled bool) error {

	clientSet, err := miscellaneous.GetKubernetesClientSetOrError()
	if err != nil {
		return fmt.Errorf("failed to create clientset: %v", err)
	}

	metadata := map[string]string{
		PeerURLTLSEnabledKey: strconv.FormatBool(peerURLTLSEnabled),
	}

	hb, err := NewHeartbeat(logger, etcdConfig, clientSet, metadata)
	if err != nil {
		return fmt.Errorf("failed to initialize new heartbeat: %v", err)
	}

	hb.heartbeatTimer = time.NewTimer(hconfig.HeartbeatDuration.Duration)
	defer hb.heartbeatTimer.Stop()
	hb.logger.Info("Started member lease renewal timer")

	for {
		select {
		case <-hb.heartbeatTimer.C:
			err := hb.RenewMemberLease(ctx)
			if err != nil {
				hb.logger.Warn(err)
			}
			hb.heartbeatTimer.Reset(hconfig.HeartbeatDuration.Duration)
		case <-ctx.Done():
			hb.logger.Info("Stopped member lease renewal timer")
			return nil
		case <-stopCh:
			hb.logger.Info("Stoping the member lease renewal")
			return nil
		}
	}
}
