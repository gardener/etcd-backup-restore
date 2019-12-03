// Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
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

package leaderelection

import (
	"context"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	"github.com/sirupsen/logrus"
)

// NewLeaderElector returns the new object of LeaderElector
func NewLeaderElector(logger *logrus.Entry, config *Config, etcdConnectionConfig *etcdutil.EtcdConnectionConfig, callbacks *LeaderCallbacks) *LeaderElector {
	return &LeaderElector{
		logger:               logger.WithField("actor", "elector"),
		config:               config,
		etcdConnectionConfig: etcdConnectionConfig,
		callbacks:            callbacks,
		status: &Status{
			observedLeader: false,
		},
	}
}

// Run runs the leader elector job
func (le *LeaderElector) Run(ctx context.Context) error {
	// retryTimer := time.NewTimer(le.config.RetryPeriod.Duration)
	le.logger.Infof("Leader election retry time: %s", le.config.RetryPeriod.Duration.String())
	for {
		select {
		case <-ctx.Done():
			le.logger.Infof("Received stop signal. Closing leader election.")
			return nil
		case <-time.After(le.config.RetryPeriod.Duration):
			// TODO: Retry-backoff
			le.logger.Infof("Rechecking the leader..")
			isLeader, err := le.isLeader(ctx)
			if err != nil {
				le.logger.Errorf("Failed to check leader.")
				if le.callbacks.OnErrorInLeaderElection != nil {
					le.callbacks.OnErrorInLeaderElection()
				}
				return err
			}
			if isLeader != le.status.observedLeader {
				le.logger.Infof("Leader change observed.")
				le.status.observedLeader = isLeader
				if isLeader {
					le.logger.Infof("Associated etcd is leader")
					if le.callbacks.OnStartedLeading != nil {
						go le.callbacks.OnStartedLeading(ctx)
					}
				} else if le.callbacks.OnStoppedLeading != nil {
					le.callbacks.OnStoppedLeading()
				}
			} else {
				le.logger.Infof("No leader change observed.")
			}
		}
	}
}

func (le *LeaderElector) isLeader(ctx context.Context) (bool, error) {
	client, err := etcdutil.GetTLSClientForEtcd(le.etcdConnectionConfig)
	if err != nil {
		return false, err
	}

	for _, ep := range client.Endpoints() {
		conCtx, cancel := context.WithTimeout(ctx, le.etcdConnectionConfig.ConnectionTimeout.Duration)
		sr, err := client.Status(conCtx, ep)
		cancel()
		if err != nil {
			return false, err
		}
		if sr.Leader == sr.Header.MemberId {
			return true, nil
		}
	}
	return false, nil
}
