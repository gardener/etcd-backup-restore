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
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/pkg/transport"
	"github.com/gardener/etcd-backup-restore/pkg/errors"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	"github.com/robfig/cron"
	"github.com/sirupsen/logrus"
)

// NewSnapshotterConfig returns a config for the snapshotter.
func NewSnapshotterConfig(schedule string, store snapstore.SnapStore, maxBackups, deltaSnapshotIntervalSeconds int, etcdConnectionTimeout, garbageCollectionPeriodSeconds time.Duration, garbageCollectionPolicy string, tlsConfig *TLSConfig) (*Config, error) {
	sdl, err := cron.ParseStandard(schedule)
	if err != nil {
		return nil, fmt.Errorf("invalid schedule provied %s : %v", schedule, err)
	}
	if maxBackups < 1 {
		return nil, fmt.Errorf("maximum backups limit should be greater than zero. Input MaxBackups: %d", maxBackups)
	}
	return &Config{
		schedule: sdl,
		store:    store,
		deltaSnapshotIntervalSeconds:   deltaSnapshotIntervalSeconds,
		etcdConnectionTimeout:          etcdConnectionTimeout,
		garbageCollectionPeriodSeconds: garbageCollectionPeriodSeconds,
		garbageCollectionPolicy:        garbageCollectionPolicy,
		maxBackups:                     maxBackups,
		tlsConfig:                      tlsConfig,
	}, nil
}

// NewSnapshotter returns the snapshotter object.
func NewSnapshotter(logger *logrus.Logger, config *Config) *Snapshotter {

	// Create dummy previous snapshot
	prevSnapshot := &snapstore.Snapshot{
		Kind:          snapstore.SnapshotKindFull,
		StartRevision: 0,
		LastRevision:  0,
		CreatedOn:     time.Now().UTC(),
	}
	prevSnapshot.GenerateSnapshotDirectory()
	prevSnapshot.GenerateSnapshotName()
	return &Snapshotter{
		logger:        logger,
		prevSnapshot:  prevSnapshot,
		config:        config,
		SsrState:      SnapshotterInactive,
		SsrStateMutex: &sync.Mutex{},
	}
}

// NewTLSConfig returns the TLSConfig object.
func NewTLSConfig(cert, key, caCert string, insecureTr, skipVerify bool, endpoints []string) *TLSConfig {
	return &TLSConfig{
		cert:       cert,
		key:        key,
		caCert:     caCert,
		insecureTr: insecureTr,
		skipVerify: skipVerify,
		endpoints:  endpoints,
	}
}

// Run process loop for scheduled backup
func (ssr *Snapshotter) Run(stopCh <-chan struct{}, startFullSnapshot bool) error {
	if startFullSnapshot {
		ssr.fullSnapshotTimer = time.NewTimer(0)
	}
	ssr.deltaSnapshotTimer = time.NewTimer(time.Duration(ssr.config.deltaSnapshotIntervalSeconds))
	if err := ssr.snapshotEventHandler(stopCh); err != nil {
		return err
	}

	defer func() {
		ssr.cancelWatch()
		ssr.SsrStateMutex.Lock()
		ssr.SsrState = SnapshotterInactive
		ssr.SsrStateMutex.Unlock()

	}()
	return nil
}

// TakeFullSnapshotAndResetTimer takes a full snapshot and resets the full snapshot
// timer as per the schedule.
func (ssr *Snapshotter) TakeFullSnapshotAndResetTimer() error {
	ssr.logger.Infof("Taking scheduled snapshot for time: %s", time.Now().Local())
	if err := ssr.takeFullSnapshot(); err != nil {
		// As per design principle, in business critical service if backup is not working,
		// it's better to fail the process. So, we are quiting here.
		ssr.logger.Infof("Taking scheduled snapshot failed: %v", err)
		return err
	}
	now := time.Now()
	effective := ssr.config.schedule.Next(now)
	if effective.IsZero() {
		ssr.logger.Info("There are no backup scheduled for future. Stopping now.")
		return fmt.Errorf("error in full snapshot schedule")
	}
	if ssr.fullSnapshotTimer == nil {
		ssr.fullSnapshotTimer = time.NewTimer(effective.Sub(now))
	} else {
		ssr.logger.Infof("Stopping full snapshot...")
		ssr.fullSnapshotTimer.Stop()
		ssr.logger.Infof("Reseting full snapshot to run after %d secs.", effective.Sub(now))
		ssr.fullSnapshotTimer.Reset(effective.Sub(now))
	}
	ssr.logger.Infof("Will take next full snapshot at time: %s", effective)
	return nil
}

// takeFullSnapshot will store full snapshot of etcd to snapstore.
// It basically will connect to etcd. Then ask for snapshot. And finally
// store it to underlying snapstore on the fly.
func (ssr *Snapshotter) takeFullSnapshot() error {
	client, err := GetTLSClientForEtcd(ssr.config.tlsConfig)
	if err != nil {
		return &errors.EtcdError{
			Message: fmt.Sprintf("failed to create etcd client: %v", err),
		}
	}

	ctx, cancel := context.WithTimeout(context.TODO(), ssr.config.etcdConnectionTimeout*time.Second)
	// Note: Although Get and snapshot call are not atomic, so revision number in snapshot file
	// may be ahead of the revision found from GET call. But currently this is the only workaround available
	// Refer: https://github.com/coreos/etcd/issues/9037
	resp, err := client.Get(ctx, "", clientv3.WithLastRev()...)
	cancel()
	if err != nil {
		return &errors.EtcdError{
			Message: fmt.Sprintf("failed to get etcd latest revision: %v", err),
		}
	}
	lastRevision := resp.Header.Revision
	ctx, cancel = context.WithTimeout(context.TODO(), ssr.config.etcdConnectionTimeout*time.Second)
	defer cancel()
	rc, err := client.Snapshot(ctx)
	if err != nil {
		return &errors.EtcdError{
			Message: fmt.Sprintf("failed to create etcd snapshot: %v", err),
		}
	}
	ssr.logger.Infof("Successfully opened snapshot reader on etcd")
	s := &snapstore.Snapshot{
		Kind:          snapstore.SnapshotKindFull,
		CreatedOn:     time.Now().UTC(),
		StartRevision: 0,
		LastRevision:  lastRevision,
	}
	s.GenerateSnapshotDirectory()
	s.GenerateSnapshotName()
	startTime := time.Now()
	if err := ssr.config.store.Save(*s, rc); err != nil {
		return &errors.SnapstoreError{
			Message: fmt.Sprintf("failed to save snapshot: %v", err),
		}
	}
	logrus.Infof("Total time to save snapshot: %f seconds.", time.Now().Sub(startTime).Seconds())
	ssr.prevSnapshot = s
	ssr.logger.Infof("Successfully saved full snapshot at: %s", path.Join(s.SnapDir, s.SnapName))
	//make event array empty as any event prior to full snapshot should not be uploaded in delta.
	ssr.events = []*event{}
	watchCtx, cancelWatch := context.WithCancel(context.TODO())
	ssr.cancelWatch = cancelWatch
	ssr.watchCh = client.Watch(watchCtx, "", clientv3.WithPrefix(), clientv3.WithRev(ssr.prevSnapshot.LastRevision+1))
	ssr.logger.Infof("Applied watch on etcd from revision: %08d", ssr.prevSnapshot.LastRevision+1)

	return nil
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

	if tlsConfig.caCert != "" {
		tlsinfo.CAFile = tlsConfig.caCert
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

func (ssr *Snapshotter) takeDeltaSnapshotAndResetTimer() error {
	if ssr.deltaSnapshotTimer == nil {
		ssr.deltaSnapshotTimer = time.NewTimer(time.Second * time.Duration(ssr.config.deltaSnapshotIntervalSeconds))
	} else {
		ssr.logger.Infof("Stopping delta snapshot...")
		ssr.deltaSnapshotTimer.Stop()
		ssr.logger.Infof("Reseting delta snapshot to run after %d secs.", ssr.config.deltaSnapshotIntervalSeconds)
		ssr.deltaSnapshotTimer.Reset(time.Second * time.Duration(ssr.config.deltaSnapshotIntervalSeconds))
	}
	if err := ssr.takeDeltaSnapshot(); err != nil {
		// As per design principle, in business critical service if backup is not working,
		// it's better to fail the process. So, we are quiting here.
		ssr.logger.Infof("Taking delta snapshot failed: %v", err)
		return err
	}
	return nil
}

func (ssr *Snapshotter) takeDeltaSnapshot() error {
	ssr.logger.Infof("Taking delta snapshot for time: %s", time.Now().Local())

	if len(ssr.events) == 0 {
		return nil
	}

	snap := &snapstore.Snapshot{
		Kind:          snapstore.SnapshotKindDelta,
		CreatedOn:     time.Now().UTC(),
		StartRevision: ssr.prevSnapshot.LastRevision + 1,
		LastRevision:  ssr.events[len(ssr.events)-1].EtcdEvent.Kv.ModRevision,
		SnapDir:       ssr.prevSnapshot.SnapDir,
	}
	snap.GenerateSnapshotName()
	data, err := json.Marshal(ssr.events)
	ssr.events = []*event{}
	if err != nil {
		return err
	}
	// compute hash
	hash := sha256.New()
	hash.Write(data)
	data = hash.Sum(data)
	dataReader := bytes.NewReader(data)

	if err := ssr.config.store.Save(*snap, dataReader); err != nil {
		ssr.logger.Errorf("Error saving delta snapshots. %v", err)
		return err
	}
	ssr.prevSnapshot = snap
	ssr.logger.Infof("Successfully saved delta snapshot at: %s", path.Join(snap.SnapDir, snap.SnapName))
	return nil
}

func (ssr *Snapshotter) getClientForDeltaSnapshots() (*clientv3.Client, error) {
	client, err := GetTLSClientForEtcd(ssr.config.tlsConfig)
	if err != nil {
		return nil, &errors.EtcdError{
			Message: fmt.Sprintf("failed to create etcd client: %v", err),
		}
	}
	return client, nil
}

func (ssr *Snapshotter) handleDeltaWatchEvents(wr clientv3.WatchResponse) error {

	if err := wr.Err(); err != nil {
		return err
	}

	//aggregate events
	for _, ev := range wr.Events {
		ssr.events = append(ssr.events, newEvent(ev))
	}
	ssr.logger.Debugf("Added events till revision: %d", ssr.events[len(ssr.events)-1].EtcdEvent.Kv.ModRevision)
	return nil
}

func newEvent(e *clientv3.Event) *event {
	return &event{
		EtcdEvent: e,
		Time:      time.Now(),
	}
}

func (ssr *Snapshotter) snapshotEventHandler(stopCh <-chan struct{}) error {
	for {
		select {
		case <-ssr.fullSnapshotTimer.C:
			if err := ssr.TakeFullSnapshotAndResetTimer(); err != nil {
				return err
			}
		case <-ssr.deltaSnapshotTimer.C:
			if err := ssr.takeDeltaSnapshotAndResetTimer(); err != nil {
				return err
			}
		case wr, ok := <-ssr.watchCh:
			if !ok {
				return fmt.Errorf("watch channel closed")
			}
			if err := ssr.handleDeltaWatchEvents(wr); err != nil {
				return err
			}
		case <-stopCh:
			return nil
		}
	}
}
