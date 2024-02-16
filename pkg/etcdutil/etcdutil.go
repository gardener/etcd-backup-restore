// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package etcdutil

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/compressor"
	"github.com/gardener/etcd-backup-restore/pkg/errors"
	"github.com/gardener/etcd-backup-restore/pkg/etcdutil/client"
	"github.com/gardener/etcd-backup-restore/pkg/metrics"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/pkg/transport"
)

// NewFactory returns a Factory that constructs new clients using the supplied ETCD client configuration.
func NewFactory(cfg brtypes.EtcdConnectionConfig, opts ...client.Option) client.Factory {
	options := &client.Options{}
	for _, opt := range opts {
		opt.ApplyTo(options)
	}

	var f = factoryImpl{
		EtcdConnectionConfig: cfg,
		options:              options,
	}

	return &f
}

// factoryImpl implements the client.Factory interface by constructing new client objects.
type factoryImpl struct {
	brtypes.EtcdConnectionConfig
	options *client.Options
}

func (f *factoryImpl) NewClient() (*clientv3.Client, error) {
	return GetTLSClientForEtcd(&f.EtcdConnectionConfig, f.options)
}

func (f *factoryImpl) NewCluster() (client.ClusterCloser, error) {
	return f.NewClient()
}

func (f *factoryImpl) NewKV() (client.KVCloser, error) {
	return f.NewClient()
}

func (f *factoryImpl) NewMaintenance() (client.MaintenanceCloser, error) {
	return f.NewClient()
}

func (f *factoryImpl) NewWatcher() (clientv3.Watcher, error) {
	return f.NewClient()
}

// NewClientFactory returns the Factory using the supplied EtcdConnectionConfig.
func NewClientFactory(fn brtypes.NewClientFactoryFunc, cfg brtypes.EtcdConnectionConfig) client.Factory {
	if fn == nil {
		fn = NewFactory
	}
	return fn(cfg)
}

// GetTLSClientForEtcd creates an etcd client using the TLS config params.
func GetTLSClientForEtcd(tlsConfig *brtypes.EtcdConnectionConfig, options *client.Options) (*clientv3.Client, error) {
	// set tls if any one tls option set
	var cfgtls *transport.TLSInfo
	tlsinfo := transport.TLSInfo{}
	if tlsConfig.CertFile != "" {
		tlsinfo.CertFile = tlsConfig.CertFile
		cfgtls = &tlsinfo
	}

	if tlsConfig.KeyFile != "" {
		tlsinfo.KeyFile = tlsConfig.KeyFile
		cfgtls = &tlsinfo
	}

	if tlsConfig.CaFile != "" {
		tlsinfo.TrustedCAFile = tlsConfig.CaFile
		cfgtls = &tlsinfo
	}

	endpoints := tlsConfig.Endpoints
	if options.UseServiceEndpoints && len(tlsConfig.ServiceEndpoints) > 0 {
		endpoints = tlsConfig.ServiceEndpoints
	}

	cfg := &clientv3.Config{
		Endpoints: endpoints,
		Context:   context.TODO(), // TODO: Use the context comming as parameter.
	}

	if cfgtls != nil {
		clientTLS, err := cfgtls.ClientConfig()
		if err != nil {
			return nil, err
		}
		cfg.TLS = clientTLS
	}

	// if key/cert is not given but user wants secure connection, we
	// should still setup an empty tls configuration for gRPC to setup
	// secure connection.
	if cfg.TLS == nil && !tlsConfig.InsecureTransport {
		cfg.TLS = &tls.Config{}
	}

	// If the user wants to skip TLS verification then we should set
	// the InsecureSkipVerify flag in tls configuration.
	if tlsConfig.InsecureSkipVerify && cfg.TLS != nil {
		cfg.TLS.InsecureSkipVerify = true
	}

	if tlsConfig.Username != "" && tlsConfig.Password != "" {
		cfg.Username = tlsConfig.Username
		cfg.Password = tlsConfig.Password
	}

	return clientv3.New(*cfg)
}

// PerformDefragmentation defragment the data directory of each etcd member.
func PerformDefragmentation(defragCtx context.Context, client client.MaintenanceCloser, endpoint string, logger *logrus.Entry) error {
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

// DefragmentData calls the defragmentation on each etcd followers endPoints
// then calls the defragmentation on etcd leader endPoints.
func DefragmentData(defragCtx context.Context, clientMaintenance client.MaintenanceCloser, clientCluster client.ClusterCloser, etcdEndpoints []string, defragTimeout time.Duration, logger *logrus.Entry) error {
	leaderEtcdEndpoints, followerEtcdEndpoints, err := GetEtcdEndPointsSorted(defragCtx, clientMaintenance, clientCluster, etcdEndpoints, logger)
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
			if err := PerformDefragmentation(ctx, clientMaintenance, ep, logger); err != nil {
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
			if err := PerformDefragmentation(ctx, clientMaintenance, ep, logger); err != nil {
				return err
			}
			return nil
		}(); err != nil {
			return err
		}
	}
	return nil
}

// GetEtcdEndPointsSorted returns the etcd leaderEndpoints and etcd followerEndpoints.
func GetEtcdEndPointsSorted(ctx context.Context, clientMaintenance client.MaintenanceCloser, clientCluster client.ClusterCloser, etcdEndpoints []string, logger *logrus.Entry) ([]string, []string, error) {
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

// TakeAndSaveFullSnapshot takes full snapshot and save it to store
func TakeAndSaveFullSnapshot(ctx context.Context, client client.MaintenanceCloser, store brtypes.SnapStore, lastRevision int64, cc *compressor.CompressionConfig, suffix string, isFinal bool, logger *logrus.Entry) (*brtypes.Snapshot, error) {
	startTime := time.Now()
	rc, err := client.Snapshot(ctx)
	if err != nil {
		return nil, &errors.EtcdError{
			Message: fmt.Sprintf("failed to create etcd snapshot: %v", err),
		}
	}
	timeTaken := time.Since(startTime)
	logger.Infof("Total time taken by Snapshot API: %f seconds.", timeTaken.Seconds())

	if cc.Enabled {
		startTimeCompression := time.Now()
		rc, err = compressor.CompressSnapshot(rc, cc.CompressionPolicy)
		if err != nil {
			return nil, fmt.Errorf("unable to obtain reader for compressed file: %v", err)
		}
		timeTakenCompression := time.Since(startTimeCompression)
		logger.Infof("Total time taken in full snapshot compression: %f seconds.", timeTakenCompression.Seconds())
	}
	defer rc.Close()

	logger.Infof("Successfully opened snapshot reader on etcd")

	// Then save the snapshot to the store.
	snapshot := snapstore.NewSnapshot(brtypes.SnapshotKindFull, 0, lastRevision, suffix, isFinal)
	if err := store.Save(*snapshot, rc); err != nil {
		timeTaken := time.Since(startTime)
		metrics.SnapshotDurationSeconds.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindFull, metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Observe(timeTaken.Seconds())
		return nil, &errors.SnapstoreError{
			Message: fmt.Sprintf("failed to save snapshot: %v", err),
		}
	}

	timeTaken = time.Since(startTime)
	metrics.SnapshotDurationSeconds.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindFull, metrics.LabelSucceeded: metrics.ValueSucceededTrue}).Observe(timeTaken.Seconds())
	logger.Infof("Total time to save full snapshot: %f seconds.", timeTaken.Seconds())

	return snapshot, nil
}
