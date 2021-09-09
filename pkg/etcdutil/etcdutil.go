// Copyright (c) 2018 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
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

package etcdutil

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/pkg/transport"

	"github.com/gardener/etcd-backup-restore/pkg/compressor"
	"github.com/gardener/etcd-backup-restore/pkg/errors"
	"github.com/gardener/etcd-backup-restore/pkg/etcdutil/client"
	"github.com/gardener/etcd-backup-restore/pkg/metrics"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
)

// NewFactory returns a Factory that constructs new clients using the supplied ETCD client configuration.
func NewFactory(cfg EtcdConnectionConfig) client.Factory {
	var f = factoryImpl(cfg)
	return &f
}

// factoryImpl implements the client.Factory interface by constructing new client objects.
type factoryImpl EtcdConnectionConfig

func (f *factoryImpl) NewClient() (*clientv3.Client, error) {
	return GetTLSClientForEtcd((*EtcdConnectionConfig)(f))
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

// GetTLSClientForEtcd creates an etcd client using the TLS config params.
func GetTLSClientForEtcd(tlsConfig *EtcdConnectionConfig) (*clientv3.Client, error) {
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

	cfg := &clientv3.Config{
		Endpoints: tlsConfig.Endpoints,
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

// DefragmentData defragment the data directory of each etcd member.
func DefragmentData(defragCtx context.Context, client *clientv3.Client, endpoints []string, logger *logrus.Entry) error {
	for _, ep := range endpoints {
		var dbSizeBeforeDefrag, dbSizeAfterDefrag int64
		logger.Infof("Defragmenting etcd member[%s]", ep)

		if status, err := client.Status(defragCtx, ep); err != nil {
			logger.Warnf("Failed to get status of etcd member[%s] with error: %v", ep, err)
		} else {
			dbSizeBeforeDefrag = status.DbSize
		}
		start := time.Now()
		if _, err := client.Defragment(defragCtx, ep); err != nil {
			metrics.DefragmentationDurationSeconds.With(prometheus.Labels{metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Observe(time.Since(start).Seconds())
			logger.Errorf("Failed to defragment etcd member[%s] with error: %v", ep, err)
			return err
		}
		metrics.DefragmentationDurationSeconds.With(prometheus.Labels{metrics.LabelSucceeded: metrics.ValueSucceededTrue}).Observe(time.Since(start).Seconds())
		logger.Infof("Finished defragmenting etcd member[%s]", ep)
		// Since below request for status races with other etcd operations. So, size returned in
		// status might vary from the precise size just after defragmentation.
		if status, err := client.Status(defragCtx, ep); err != nil {
			logger.Warnf("Failed to get status of etcd member[%s] with error: %v", ep, err)
		} else {
			dbSizeAfterDefrag = status.DbSize
			logger.Infof("Probable DB size change for etcd member [%s]:  %dB -> %dB after defragmentation", ep, dbSizeBeforeDefrag, dbSizeAfterDefrag)
		}
	}
	return nil
}

// TakeAndSaveFullSnapshot takes full snapshot and save it to store
func TakeAndSaveFullSnapshot(ctx context.Context, client *clientv3.Client, store brtypes.SnapStore, lastRevision int64, cc *compressor.CompressionConfig, suffix string, logger *logrus.Entry) (*brtypes.Snapshot, error) {
	rc, err := client.Snapshot(ctx)
	if err != nil {
		return nil, &errors.EtcdError{
			Message: fmt.Sprintf("failed to create etcd snapshot: %v", err),
		}
	}
	if cc.Enabled {
		rc, err = compressor.CompressSnapshot(rc, cc.CompressionPolicy)
		if err != nil {
			return nil, fmt.Errorf("unable to obtain reader for compressed file: %v", err)
		}
	}
	defer rc.Close()

	logger.Infof("Successfully opened snapshot reader on etcd")

	// Then save the snapshot to the store.
	snapshot := snapstore.NewSnapshot(brtypes.SnapshotKindFull, 0, lastRevision, suffix)
	startTime := time.Now()
	if err := store.Save(*snapshot, rc); err != nil {
		timeTaken := time.Since(startTime)
		metrics.SnapshotDurationSeconds.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindFull, metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Observe(timeTaken.Seconds())
		return nil, &errors.SnapstoreError{
			Message: fmt.Sprintf("failed to save snapshot: %v", err),
		}
	}

	timeTaken := time.Since(startTime)
	metrics.SnapshotDurationSeconds.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindFull, metrics.LabelSucceeded: metrics.ValueSucceededTrue}).Observe(timeTaken.Seconds())
	logger.Infof("Total time to save snapshot: %f seconds.", timeTaken.Seconds())

	return snapshot, nil
}
