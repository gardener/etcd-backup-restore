package etcdutil

import (
	"context"
	"fmt"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/errors"
	"github.com/gardener/etcd-backup-restore/pkg/etcdaccess"
	"github.com/gardener/etcd-backup-restore/pkg/metrics"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

// DefragmentData calls the defragmentation on each etcd followers endPoints
// then calls the defragmentation on etcd leader endPoints.
func DefragmentData(defragCtx context.Context, clientMaintenance etcdaccess.MaintenanceCloser, clientCluster etcdaccess.ClusterCloser, etcdEndpoints []string, defragTimeout time.Duration, logger *logrus.Entry) error {
	leaderEtcdEndpoints, followerEtcdEndpoints, err := getEtcdEndPointsSorted(defragCtx, clientMaintenance, clientCluster, etcdEndpoints, logger)
	logger.Debugf("etcdEndpoints: %v", etcdEndpoints)
	logger.Debugf("leaderEndpoints: %v", leaderEtcdEndpoints)
	logger.Debugf("followerEtcdEndpointss: %v", followerEtcdEndpoints)
	if err != nil {
		return err
	}

	if len(followerEtcdEndpoints) > 0 {
		logger.Info("Starting the defragmentation on etcd followers in a rolling manner")
	}
	// Perform the defragmentation on each etcd followers.
	for _, ep := range followerEtcdEndpoints {
		if err := func() error {
			ctx, cancel := context.WithTimeout(defragCtx, defragTimeout)
			defer cancel()
			if err := performDefragmentation(ctx, clientMaintenance, ep, logger); err != nil {
				return err
			}
			return nil
		}(); err != nil {
			return err
		}
	}

	logger.Info("Starting the defragmentation on etcd leader")
	// Perform the defragmentation on etcd leader.
	for _, ep := range leaderEtcdEndpoints {
		if err := func() error {
			ctx, cancel := context.WithTimeout(defragCtx, defragTimeout)
			defer cancel()
			if err := performDefragmentation(ctx, clientMaintenance, ep, logger); err != nil {
				return err
			}
			return nil
		}(); err != nil {
			return err
		}
	}
	return nil
}

// performDefragmentation defragment the data directory of each etcd member.
func performDefragmentation(defragCtx context.Context, client etcdaccess.MaintenanceCloser, endpoint string, logger *logrus.Entry) error {
	var dbSizeBeforeDefrag, dbSizeAfterDefrag int64
	logger.Infof("Defragmenting etcd member[%s]", endpoint)

	if status, err := client.Status(defragCtx, endpoint); err != nil {
		logger.Warnf("Failed to get status of etcd member[%s] with error: %v", endpoint, err)
	} else {
		dbSizeBeforeDefrag = status.DbSize
	}

	start := time.Now()
	if _, err := client.Defragment(defragCtx, endpoint); err != nil {
		metrics.DefragmentationDurationSeconds.With(prometheus.Labels{metrics.LabelSucceeded: metrics.ValueSucceededFalse, metrics.LabelEndPoint: endpoint}).Observe(time.Since(start).Seconds())
		logger.Errorf("failed to defragment etcd member[%s] with error: %v", endpoint, err)
		return err
	}

	metrics.DefragmentationDurationSeconds.With(prometheus.Labels{metrics.LabelSucceeded: metrics.ValueSucceededTrue, metrics.LabelEndPoint: endpoint}).Observe(time.Since(start).Seconds())
	logger.Infof("Finished defragmenting etcd member[%s]", endpoint)
	// Since below request for status races with other etcd operations. So, size returned in
	// status might vary from the precise size just after defragmentation.
	if status, err := client.Status(defragCtx, endpoint); err != nil {
		logger.Warnf("Failed to get status of etcd member[%s] with error: %v", endpoint, err)
	} else {
		dbSizeAfterDefrag = status.DbSize
		logger.Infof("Probable DB size change for etcd member [%s]:  %dB -> %dB after defragmentation", endpoint, dbSizeBeforeDefrag, dbSizeAfterDefrag)
	}
	return nil
}

// getEtcdEndPointsSorted returns the etcd leaderEndpoints and etcd followerEndpoints.
func getEtcdEndPointsSorted(ctx context.Context, clientMaintenance etcdaccess.MaintenanceCloser, clientCluster etcdaccess.ClusterCloser, etcdEndpoints []string, logger *logrus.Entry) ([]string, []string, error) {
	var leaderEtcdEndpoints []string
	var followerEtcdEndpoints []string
	var endPoint string

	ctx, cancel := context.WithTimeout(ctx, brtypes.DefaultEtcdConnectionTimeout)
	defer cancel()

	membersInfo, err := clientCluster.MemberList(ctx)
	if err != nil {
		logger.Errorf("failed to get memberList of etcd with error: %v", err)
		return nil, nil, err
	}

	// to handle the single node etcd case (particularly: single node embedded etcd case)
	if len(membersInfo.Members) == 1 {
		leaderEtcdEndpoints = append(leaderEtcdEndpoints, etcdEndpoints...)
		return leaderEtcdEndpoints, nil, nil
	}

	if len(etcdEndpoints) > 0 {
		endPoint = etcdEndpoints[0]
	} else {
		return nil, nil, &errors.EtcdError{
			Message: fmt.Sprintf("etcd endpoints are not passed correctly"),
		}
	}

	response, err := clientMaintenance.Status(ctx, endPoint)
	if err != nil {
		logger.Errorf("failed to get status of etcd endPoint: %v with error: %v", endPoint, err)
		return nil, nil, err
	}

	for _, member := range membersInfo.Members {
		if member.GetID() == response.Leader {
			leaderEtcdEndpoints = append(leaderEtcdEndpoints, member.GetClientURLs()...)
		} else {
			followerEtcdEndpoints = append(followerEtcdEndpoints, member.GetClientURLs()...)
		}
	}

	return leaderEtcdEndpoints, followerEtcdEndpoints, nil
}
