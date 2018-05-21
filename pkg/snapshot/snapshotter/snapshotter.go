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

package snapshotter

import (
	"context"
	"crypto/tls"
	"fmt"
	"path"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/pkg/transport"
	"github.com/gardener/etcd-backup-restore/pkg/errors"
	"github.com/gardener/etcd-backup-restore/pkg/retry"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	"github.com/robfig/cron"
	"github.com/sirupsen/logrus"
)

// NewSnapshotter returns the snapshotter object.
func NewSnapshotter(
	schedule string,
	store snapstore.SnapStore,
	logger *logrus.Logger,
	maxBackups int,
	etcdConnectionTimeout time.Duration,
	tlsConfig *TLSConfig) (*Snapshotter, error) {
	logger.Printf("Validating schedule...")
	sdl, err := cron.ParseStandard(schedule)
	if err != nil {
		return nil, fmt.Errorf("invalid schedule provied %s : %v", schedule, err)
	}

	return &Snapshotter{
		logger:                logger,
		schedule:              sdl,
		store:                 store,
		maxBackups:            maxBackups,
		etcdConnectionTimeout: etcdConnectionTimeout,
		tlsConfig:             tlsConfig,
	}, nil
}

// NewTLSConfig returns the TLSConfig object.
func NewTLSConfig(
	cert string,
	key string,
	cacert string,
	insecureTr bool,
	skipVerify bool,
	endpoints []string,
) *TLSConfig {
	return &TLSConfig{
		cert:       cert,
		key:        key,
		cacert:     cacert,
		insecureTr: insecureTr,
		skipVerify: skipVerify,
		endpoints:  endpoints,
	}
}

// Run process loop for scheduled backup
func (ssr *Snapshotter) Run(stopCh <-chan struct{}) error {
	now := time.Now()
	effective := ssr.schedule.Next(now)
	if effective.IsZero() {
		ssr.logger.Infoln("There are no backup scheduled for future. Stopping now.")
		return nil
	}
	config := &retry.Config{
		Attempts: 6,
		Delay:    1,
		Units:    time.Second,
		Logger:   ssr.logger,
	}
	ssr.logger.Infof("Will take next snapshot at time: %s", effective)
	for {
		select {
		case <-stopCh:
			//TODO: Cleanup work here
			ssr.logger.Infof("Stop signal received. Terminating scheduled snapshot.")
			return nil
		case <-time.After(effective.Sub(now)):
			err := retry.Do(func() error {
				ssr.logger.Infof("Taking scheduled snapshot for time: %s", time.Now().Local())
				err := ssr.TakeFullSnapshot()
				if err != nil {
					ssr.logger.Infof("Taking scheduled snapshot failed: %v", err)
				}
				return err
			}, config)
			if err != nil {
				// As per design principle, in business critical service if backup is not working,
				// it's better to fail the process. So, we are quiting here.
				return err
			}
			now = time.Now()
			effective = ssr.schedule.Next(now)

			ssr.garbageCollector()
			if effective.IsZero() {
				ssr.logger.Infoln("There are no backup scheduled for future. Stopping now.")
				return nil
			}
			ssr.logger.Infof("Will take next snapshot at time: %s", effective)
		}
	}
}

// TakeFullSnapshot will store full snapshot of etcd to snapstore.
// It basically will connect to etcd. Then ask for snapshot. And finally
// store it to underlying snapstore on the fly.
func (ssr *Snapshotter) TakeFullSnapshot() error {
	var err error
	client, err := GetTLSClientForEtcd(ssr.tlsConfig)
	if err != nil {
		return &errors.EtcdError{
			Message: fmt.Sprintf("failed to create etcd client: %v", err),
		}
	}

	ctx, cancel := context.WithTimeout(context.TODO(), ssr.etcdConnectionTimeout*time.Second)
	defer cancel()
	resp, err := client.Get(ctx, "", clientv3.WithLastRev()...)
	if err != nil {
		return &errors.EtcdError{
			Message: fmt.Sprintf("failed to get etcd latest revision: %v", err),
		}
	}
	lastRevision := resp.Header.Revision
	rc, err := client.Snapshot(ctx)
	if err != nil {
		return &errors.EtcdError{
			Message: fmt.Sprintf("failed to create etcd snapshot: %v", err),
		}
	}
	ssr.logger.Infof("Successfully opened snapshot reader on etcd")
	s := snapstore.Snapshot{
		Kind:          snapstore.SnapshotKindFull,
		CreatedOn:     time.Now(),
		StartRevision: 0,
		LastRevision:  lastRevision,
	}
	s.GenerateSnapshotDirectory()
	s.GenerateSnapshotName()

	err = ssr.store.Save(s, rc)
	if err != nil {
		return &errors.SnapstoreError{
			Message: fmt.Sprintf("failed to save snapshot: %v", err),
		}
	}
	ssr.logger.Infof("Successfully saved full snapshot at: %s", path.Join(s.SnapDir, s.SnapName))
	return nil
}

// garbageCollector basically consider the older backups as garbage and deletes it
func (ssr *Snapshotter) garbageCollector() {
	ssr.logger.Infoln("Executing garbage collection...")
	snapList, err := ssr.store.List()
	if err != nil {
		ssr.logger.Warnf("Failed to list snapshots: %v", err)
		return
	}
	snapLen := len(snapList)
	for i := 0; i < (snapLen - ssr.maxBackups); i++ {
		ssr.logger.Infof("Deleting old snapshot: %s", path.Join(snapList[i].SnapDir, snapList[i].SnapName))
		err = ssr.store.Delete(*snapList[i])
		if err != nil {
			ssr.logger.Warnf("Failed to delete snapshot: %s", path.Join(snapList[i].SnapDir, snapList[i].SnapName))
		}
	}
}

// GetTLSClientForEtcd creates an etcd client using the TLS config params.
func GetTLSClientForEtcd(tlsConfig *TLSConfig) (*clientv3.Client, error) {
	// set tls if any one tls option set
	var cfgtls *transport.TLSInfo
	tlsinfo := transport.TLSInfo{}
	if tlsConfig.cert != "" {
		tlsinfo.CertFile = tlsConfig.cert
		cfgtls = &tlsinfo
	}

	if tlsConfig.key != "" {
		tlsinfo.KeyFile = tlsConfig.key
		cfgtls = &tlsinfo
	}

	if tlsConfig.cacert != "" {
		tlsinfo.CAFile = tlsConfig.cacert
		cfgtls = &tlsinfo
	}

	cfg := &clientv3.Config{
		Endpoints: tlsConfig.endpoints,
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
	if cfg.TLS == nil && !tlsConfig.insecureTr {
		cfg.TLS = &tls.Config{}
	}

	// If the user wants to skip TLS verification then we should set
	// the InsecureSkipVerify flag in tls configuration.
	if tlsConfig.skipVerify && cfg.TLS != nil {
		cfg.TLS.InsecureSkipVerify = true
	}

	return clientv3.New(*cfg)
}
