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
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	cron "github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
)

// CallbackFunc is type decalration for callback function for defragmentor
type CallbackFunc func(ctx context.Context) error

// defragmentorJob implement the cron.Job for etcd defragmentation.
type defragmentorJob struct {
	ctx                   context.Context
	tlsConfig             *TLSConfig
	etcdConnectionTimeout time.Duration
	logger                *logrus.Entry
	callback              CallbackFunc
}

// NewDefragmentorJob returns the new defragmentor job.
func NewDefragmentorJob(ctx context.Context, tlsConfig *TLSConfig, etcdConnectionTimeout time.Duration, logger *logrus.Entry, callback CallbackFunc) cron.Job {
	return &defragmentorJob{
		ctx:                   ctx,
		tlsConfig:             tlsConfig,
		etcdConnectionTimeout: etcdConnectionTimeout,
		logger:                logger.WithField("job", "defragmentor"),
		callback:              callback,
	}
}

func (d *defragmentorJob) Run() {
	if err := d.defragData(); err != nil {
		d.logger.Warnf("Failed to defrag data with error: %v", err)
	} else {
		if d.callback != nil {
			if err = d.callback(d.ctx); err != nil {
				d.logger.Warnf("defragmentation callback failed with error: %v", err)
			}
		}
	}
}

// defragData defragment the data directory of each etcd member.
func (d *defragmentorJob) defragData() error {
	client, err := GetTLSClientForEtcd(d.tlsConfig)
	if err != nil {
		d.logger.Warnf("failed to create etcd client for defragmentation")
		return err
	}
	defer client.Close()

	for _, ep := range d.tlsConfig.endpoints {
		var dbSizeBeforeDefrag, dbSizeAfterDefrag int64
		d.logger.Infof("Defragmenting etcd member[%s]", ep)
		statusReqCtx, cancel := context.WithTimeout(d.ctx, etcdDialTimeout)
		status, err := client.Status(statusReqCtx, ep)
		cancel()
		if err != nil {
			d.logger.Warnf("Failed to get status of etcd member[%s] with error: %v", ep, err)
		} else {
			dbSizeBeforeDefrag = status.DbSize
		}
		start := time.Now()
		defragCtx, cancel := context.WithTimeout(d.ctx, d.etcdConnectionTimeout)
		_, err = client.Defragment(defragCtx, ep)
		cancel()
		if err != nil {
			metrics.DefragmentationDurationSeconds.With(prometheus.Labels{metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Observe(time.Now().Sub(start).Seconds())
			d.logger.Errorf("Failed to defragment etcd member[%s] with error: %v", ep, err)
			return err
		}
		metrics.DefragmentationDurationSeconds.With(prometheus.Labels{metrics.LabelSucceeded: metrics.ValueSucceededTrue}).Observe(time.Now().Sub(start).Seconds())
		d.logger.Infof("Finished defragmenting etcd member[%s]", ep)
		// Since below request for status races with other etcd operations. So, size returned in
		// status might vary from the precise size just after defragmentation.
		statusReqCtx, cancel = context.WithTimeout(d.ctx, etcdDialTimeout)
		status, err = client.Status(statusReqCtx, ep)
		cancel()
		if err != nil {
			d.logger.Warnf("Failed to get status of etcd member[%s] with error: %v", ep, err)
		} else {
			dbSizeAfterDefrag = status.DbSize
			d.logger.Infof("Probable DB size change for etcd member [%s]:  %dB -> %dB after defragmentation", ep, dbSizeBeforeDefrag, dbSizeAfterDefrag)
		}
	}
	return nil
}

// DefragDataPeriodically defragments the data directory of each etcd member.
func DefragDataPeriodically(ctx context.Context, tlsConfig *TLSConfig, defragmentationSchedule cron.Schedule, etcdConnectionTimeout time.Duration, callback CallbackFunc, logger *logrus.Entry) {
	defragmentorJob := NewDefragmentorJob(ctx, tlsConfig, etcdConnectionTimeout, logger, callback)
	jobRunner := cron.New(cron.WithChain(cron.SkipIfStillRunning(cron.DefaultLogger)))
	jobRunner.Schedule(defragmentationSchedule, defragmentorJob)

	jobRunner.Start()

	<-ctx.Done()
	jobRunnerCtx := jobRunner.Stop()
	<-jobRunnerCtx.Done()
}
