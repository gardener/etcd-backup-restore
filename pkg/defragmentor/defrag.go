// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package defragmentor

import (
	"context"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

	cron "github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
)

// CallbackFunc is type decalration for callback function for defragmentor
type CallbackFunc func(ctx context.Context, isFinal bool) (*brtypes.Snapshot, error)

// defragmentorJob implement the cron.Job for etcd defragmentation.
type defragmentorJob struct {
	ctx                  context.Context
	etcdConnectionConfig *brtypes.EtcdConnectionConfig
	logger               *logrus.Entry
	callback             CallbackFunc
}

// NewDefragmentorJob returns the new defragmentor job.
func NewDefragmentorJob(ctx context.Context, etcdConnectionConfig *brtypes.EtcdConnectionConfig, logger *logrus.Entry, callback CallbackFunc) cron.Job {
	return &defragmentorJob{
		ctx:                  ctx,
		etcdConnectionConfig: etcdConnectionConfig,
		logger:               logger.WithField("job", "defragmentor"),
		callback:             callback,
	}
}

func (d *defragmentorJob) Run() {
	clientFactory := etcdutil.NewFactory(*d.etcdConnectionConfig)

	clientMaintenance, err := clientFactory.NewMaintenance()
	if err != nil {
		d.logger.Warnf("failed to create etcd maintenance client")
	}
	defer clientMaintenance.Close()

	client, err := clientFactory.NewCluster()
	if err != nil {
		d.logger.Warnf("failed to create etcd cluster client")
	}
	defer client.Close()

	ticker := time.NewTicker(brtypes.DefragRetryPeriod)
	defer ticker.Stop()

waitLoop:
	for {
		select {
		case <-d.ctx.Done():
			return
		case <-ticker.C:
			etcdEndpoints, err := miscellaneous.GetAllEtcdEndpoints(d.ctx, client, d.etcdConnectionConfig, d.logger)
			if err != nil {
				d.logger.Errorf("failed to get endpoints of all members of etcd cluster: %v", err)
				continue
			}
			d.logger.Infof("All etcd members endPoints: %v", etcdEndpoints)

			isClusterHealthy, err := miscellaneous.IsEtcdClusterHealthy(d.ctx, clientMaintenance, d.etcdConnectionConfig, etcdEndpoints, d.logger)
			if err != nil {
				d.logger.Errorf("failed to defrag as all members of etcd cluster are not healthy: %v", err)
				continue
			}

			if isClusterHealthy {
				d.logger.Infof("Starting the defragmentation as all members of etcd cluster are in healthy state")
				err = etcdutil.DefragmentData(d.ctx, clientMaintenance, client, etcdEndpoints, d.etcdConnectionConfig.DefragTimeout.Duration, d.logger)
				if err != nil {
					d.logger.Warnf("failed to defrag data with error: %v", err)
				} else {
					if d.callback != nil {
						if _, err = d.callback(d.ctx, false); err != nil {
							d.logger.Warnf("defragmentation callback failed with error: %v", err)
						}
					}
					break waitLoop
				}
			}
		}
	}

}

// DefragDataPeriodically defragments the data directory of each etcd member.
func DefragDataPeriodically(ctx context.Context, etcdConnectionConfig *brtypes.EtcdConnectionConfig, defragmentationSchedule cron.Schedule, callback CallbackFunc, logger *logrus.Entry) {
	defragmentorJob := NewDefragmentorJob(ctx, etcdConnectionConfig, logger, callback)
	// TODO: Sync logrus logger to cron logger
	jobRunner := cron.New(cron.WithChain(cron.SkipIfStillRunning(cron.DefaultLogger)))
	jobRunner.Schedule(defragmentationSchedule, defragmentorJob)

	jobRunner.Start()

	<-ctx.Done()
	logger.Info("Closing defragmentor.")
	jobRunnerCtx := jobRunner.Stop()
	<-jobRunnerCtx.Done()
}
