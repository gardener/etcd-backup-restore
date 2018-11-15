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
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
)

// defragData defragments the data directory of each etcd member.
func defragData(tlsConfig *TLSConfig, etcdConnectionTimeout time.Duration) error {
	client, err := GetTLSClientForEtcd(tlsConfig)
	if err != nil {
		return fmt.Errorf("failed to create etcd client for defragmentation: %v", err)
	}
	defer client.Close()

	for _, ep := range tlsConfig.endpoints {
		var dbSizeBeforeDefrag, dbSizeAfterDefrag int64
		logrus.Infof("Defragmenting etcd member[%s]", ep)
		ctx, cancel := context.WithTimeout(context.TODO(), etcdDialTimeout)
		status, err := client.Status(ctx, ep)
		cancel()
		if err != nil {
			logrus.Warnf("Failed to get status of etcd member[%s] with error: %v", ep, err)
		} else {
			dbSizeBeforeDefrag = status.DbSize
		}
		ctx, cancel = context.WithTimeout(context.TODO(), etcdConnectionTimeout)
		_, err = client.Defragment(ctx, ep)
		cancel()
		if err != nil {
			return fmt.Errorf("Failed to defragment etcd member[%s] with error: %v", ep, err)
		}
		logrus.Infof("Finished defragmenting etcd member[%s]", ep)
		// Since below request for status races with other etcd operations. So, size returned in
		// status might vary from the precise size just after defragmentation.
		ctx, cancel = context.WithTimeout(context.TODO(), etcdDialTimeout)
		status, err = client.Status(ctx, ep)
		cancel()
		if err != nil {
			logrus.Warnf("Failed to get status of etcd member[%s] with error: %v", ep, err)
		} else {
			dbSizeAfterDefrag = status.DbSize
		}
		logrus.Infof("Probable DB size change for etcd member [%s]:  %dB -> %dB after defragmentation", ep, dbSizeBeforeDefrag, dbSizeAfterDefrag)
	}
	return nil
}

// DefragDataPeriodically defragments the data directory of each etcd member.
func DefragDataPeriodically(stopCh <-chan struct{}, tlsConfig *TLSConfig, defragmentationPeriod, etcdConnectionTimeout time.Duration, callback func()) {
	logrus.Infof("Defragmentation period :%d hours", defragmentationPeriod/time.Hour)
	for {
		select {
		case <-stopCh:
			logrus.Infof("Stopping the defragmentation thread.")
			return
		case <-time.After(defragmentationPeriod):
			err := defragData(tlsConfig, etcdConnectionTimeout)
			if err != nil {
				logrus.Warnf("Failed to defrag data with error: %v", err)
			} else {
				callback()
			}
		}
	}
}
