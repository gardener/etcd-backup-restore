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

	"github.com/sirupsen/logrus"
)

// defragData defragments the data directory of each etcd member.
func defragData(tlsConfig *TLSConfig, etcdConnectionTimeout time.Duration) {
	client, err := GetTLSClientForEtcd(tlsConfig)
	if err != nil {
		logrus.Warnf("failed to create etcd client for defragmentation: %v", err)
		return
	}

	for _, ep := range tlsConfig.endpoints {
		logrus.Infof("Defragmenting etcd member[%s]", ep)
		ctx, cancel := context.WithTimeout(context.TODO(), etcdConnectionTimeout)
		status, err := client.Status(ctx, ep)
		cancel()
		if err != nil {
			logrus.Warnf("Failed to get status of etcd member[%s] with error: %v", ep, err)
			continue
		}
		dbSizeBeforeDefrag := status.DbSize
		ctx, cancel = context.WithTimeout(context.TODO(), etcdConnectionTimeout)
		_, err = client.Defragment(ctx, ep)
		cancel()
		if err != nil {
			logrus.Warnf("Failed to defragment etcd member[%s] with error: %v", ep, err)
		} else {
			logrus.Infof("Finished defragmenting etcd member[%s]", ep)
		}
		ctx, cancel = context.WithTimeout(context.TODO(), etcdConnectionTimeout)
		status, err = client.Status(ctx, ep)
		cancel()
		if err != nil {
			logrus.Warnf("Failed to get status of etcd member[%s] with error: %v", ep, err)
			continue
		}
		dbSizeAfterDefrag := status.DbSize
		logrus.Infof("DB size for etcd member [%s] changed from %dB to %dB by defragmentation", ep, dbSizeBeforeDefrag, dbSizeAfterDefrag)
	}
}

// DefragDataPeriodically defragments the data directory of each etcd member.
func DefragDataPeriodically(stopCh <-chan struct{}, tlsConfig *TLSConfig, defragmentationPeriod, etcdConnectionTimeout time.Duration, callback func()) {
	if defragmentationPeriod <= time.Hour {
		logrus.Infof("Disabling defragmentation since defragmentation period [%d] is less than 1", defragmentationPeriod)
		return
	}
	logrus.Infof("Defragmentation period :%d hours", defragmentationPeriod/time.Hour)
	for {
		select {
		case <-stopCh:
			logrus.Infof("Stopping the defragmentation thread.")
			return
		case <-time.After(defragmentationPeriod):
			defragData(tlsConfig, etcdConnectionTimeout)
			callback()
		}
	}
}
