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
	"encoding/json"
	"fmt"
	"math"
	"path"
	"sync"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/metrics"

	"github.com/coreos/etcd/clientv3"
	"github.com/gardener/etcd-backup-restore/pkg/errors"
	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/robfig/cron"
	"github.com/sirupsen/logrus"
)

// NewSnapshotterConfig returns a config for the snapshotter.
func NewSnapshotterConfig(schedule string, store snapstore.SnapStore, maxBackups, deltaSnapshotIntervalSeconds, deltaSnapshotMemoryLimit int, etcdConnectionTimeout, garbageCollectionPeriodSeconds time.Duration, garbageCollectionPolicy string, tlsConfig *etcdutil.TLSConfig) (*Config, error) {
	logrus.Printf("Validating schedule...")
	sdl, err := cron.ParseStandard(schedule)
	if err != nil {
		return nil, fmt.Errorf("invalid schedule provied %s : %v", schedule, err)
	}

	if garbageCollectionPolicy == GarbageCollectionPolicyLimitBased && maxBackups < 1 {
		logrus.Infof("Found garbage collection policy: [%s], and maximum backup value %d less than 1. Setting it to default: %d ", GarbageCollectionPolicyLimitBased, maxBackups, DefaultMaxBackups)
		maxBackups = DefaultMaxBackups
	}
	if deltaSnapshotIntervalSeconds < 1 {
		logrus.Infof("Found delta snapshot interval %d second less than 1 second. Setting it to default: %d ", deltaSnapshotIntervalSeconds, DefaultDeltaSnapshotIntervalSeconds)
		deltaSnapshotIntervalSeconds = DefaultDeltaSnapshotIntervalSeconds
	}
	if deltaSnapshotMemoryLimit < 1 {
		logrus.Infof("Found delta snapshot memory limit %d bytes less than 1 byte. Setting it to default: %d ", deltaSnapshotMemoryLimit, DefaultDeltaSnapMemoryLimit)
		deltaSnapshotMemoryLimit = DefaultDeltaSnapMemoryLimit
	}

	return &Config{
		schedule:                       sdl,
		store:                          store,
		deltaSnapshotIntervalSeconds:   deltaSnapshotIntervalSeconds,
		deltaSnapshotMemoryLimit:       deltaSnapshotMemoryLimit,
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
	var prevSnapshot *snapstore.Snapshot
	fullSnap, deltaSnapList, err := miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(config.store)
	if err != nil || fullSnap == nil {
		prevSnapshot = snapstore.NewSnapshot(snapstore.SnapshotKindFull, 0, 0)
		metrics.LatestSnapshotTimestamp.With(prometheus.Labels{metrics.LabelKind: prevSnapshot.Kind}).Set(math.NaN())
	} else if len(deltaSnapList) == 0 {
		prevSnapshot = fullSnap
		metrics.LatestSnapshotTimestamp.With(prometheus.Labels{metrics.LabelKind: prevSnapshot.Kind}).Set(float64(prevSnapshot.CreatedOn.Unix()))
	} else {
		prevSnapshot = deltaSnapList[len(deltaSnapList)-1]
		metrics.LatestSnapshotTimestamp.With(prometheus.Labels{metrics.LabelKind: prevSnapshot.Kind}).Set(float64(prevSnapshot.CreatedOn.Unix()))
	}

	metrics.LatestSnapshotRevision.With(prometheus.Labels{metrics.LabelKind: prevSnapshot.Kind}).Set(float64(prevSnapshot.LastRevision))

	return &Snapshotter{
		logger:         logger,
		prevSnapshot:   prevSnapshot,
		config:         config,
		SsrState:       SnapshotterInactive,
		SsrStateMutex:  &sync.Mutex{},
		fullSnapshotCh: make(chan struct{}),
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

	defer ssr.stop()
	return nil
}

// TriggerFullSnapshot sends the events to take full snapshot. This is to
// trigger full snapshot externally out regular schedule.
func (ssr *Snapshotter) TriggerFullSnapshot() {
	ssr.SsrStateMutex.Lock()
	if ssr.SsrState == SnapshotterActive {
		ssr.logger.Info("Triggering out of schedule full snapshot...")
		ssr.fullSnapshotCh <- emptyStruct
	} else {
		ssr.logger.Info("Skipped triggering out of schedule full snapshot, since snapshotter is not active.")
	}
	ssr.SsrStateMutex.Unlock()
}

// stop stops the snapshotter. Once stopped any subsequent calls will
// not have any effect.
func (ssr *Snapshotter) stop() {
	ssr.SsrStateMutex.Lock()
	if ssr.fullSnapshotTimer != nil {
		ssr.fullSnapshotTimer.Stop()
		ssr.fullSnapshotTimer = nil
	}
	if ssr.deltaSnapshotTimer != nil {
		ssr.deltaSnapshotTimer.Stop()
		ssr.deltaSnapshotTimer = nil
	}
	ssr.cancelWatch()
	ssr.SsrState = SnapshotterInactive
	ssr.SsrStateMutex.Unlock()
}

// TakeFullSnapshotAndResetTimer takes a full snapshot and resets the full snapshot
// timer as per the schedule.
func (ssr *Snapshotter) TakeFullSnapshotAndResetTimer() error {
	ssr.logger.Infof("Taking scheduled snapshot for time: %s", time.Now().Local())
	if err := ssr.takeFullSnapshot(); err != nil {
		// As per design principle, in business critical service if backup is not working,
		// it's better to fail the process. So, we are quiting here.
		ssr.logger.Warnf("Taking scheduled snapshot failed: %v", err)
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
	client, err := etcdutil.GetTLSClientForEtcd(ssr.config.tlsConfig)
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

	if ssr.prevSnapshot.Kind == snapstore.SnapshotKindFull && ssr.prevSnapshot.LastRevision == lastRevision {
		ssr.logger.Infof("There are no updates since last snapshot, skipping full snapshot.")
	} else {
		ctx, cancel = context.WithTimeout(context.TODO(), ssr.config.etcdConnectionTimeout*time.Second)
		defer cancel()
		rc, err := client.Snapshot(ctx)
		if err != nil {
			return &errors.EtcdError{
				Message: fmt.Sprintf("failed to create etcd snapshot: %v", err),
			}
		}
		ssr.logger.Infof("Successfully opened snapshot reader on etcd")
		s := snapstore.NewSnapshot(snapstore.SnapshotKindFull, 0, lastRevision)
		startTime := time.Now()
		if err := ssr.config.store.Save(*s, rc); err != nil {
			timeTaken := time.Now().Sub(startTime).Seconds()
			metrics.SnapshotDurationSeconds.With(prometheus.Labels{metrics.LabelKind: snapstore.SnapshotKindFull, metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Observe(timeTaken)
			return &errors.SnapstoreError{
				Message: fmt.Sprintf("failed to save snapshot: %v", err),
			}
		}
		timeTaken := time.Now().Sub(startTime).Seconds()
		metrics.SnapshotDurationSeconds.With(prometheus.Labels{metrics.LabelKind: snapstore.SnapshotKindFull, metrics.LabelSucceeded: metrics.ValueSucceededTrue}).Observe(timeTaken)
		logrus.Infof("Total time to save snapshot: %f seconds.", timeTaken)
		ssr.prevSnapshot = s

		metrics.LatestSnapshotRevision.With(prometheus.Labels{metrics.LabelKind: ssr.prevSnapshot.Kind}).Set(float64(ssr.prevSnapshot.LastRevision))
		metrics.LatestSnapshotTimestamp.With(prometheus.Labels{metrics.LabelKind: ssr.prevSnapshot.Kind}).Set(float64(ssr.prevSnapshot.CreatedOn.Unix()))

		ssr.logger.Infof("Successfully saved full snapshot at: %s", path.Join(s.SnapDir, s.SnapName))
	}

	//make event array empty as any event prior to full snapshot should not be uploaded in delta.
	ssr.events = []*event{}
	ssr.eventMemory = 0
	watchCtx, cancelWatch := context.WithCancel(context.TODO())
	ssr.cancelWatch = cancelWatch
	ssr.watchCh = client.Watch(watchCtx, "", clientv3.WithPrefix(), clientv3.WithRev(ssr.prevSnapshot.LastRevision+1))
	ssr.logger.Infof("Applied watch on etcd from revision: %08d", ssr.prevSnapshot.LastRevision+1)

	return nil
}

func (ssr *Snapshotter) takeDeltaSnapshotAndResetTimer() error {
	if err := ssr.takeDeltaSnapshot(); err != nil {
		// As per design principle, in business critical service if backup is not working,
		// it's better to fail the process. So, we are quiting here.
		ssr.logger.Warnf("Taking delta snapshot failed: %v", err)
		return err
	}
	ssr.eventMemory = 0
	if ssr.deltaSnapshotTimer == nil {
		ssr.deltaSnapshotTimer = time.NewTimer(time.Second * time.Duration(ssr.config.deltaSnapshotIntervalSeconds))
	} else {
		ssr.logger.Infof("Stopping delta snapshot...")
		ssr.deltaSnapshotTimer.Stop()
		ssr.logger.Infof("Reseting delta snapshot to run after %d secs.", ssr.config.deltaSnapshotIntervalSeconds)
		ssr.deltaSnapshotTimer.Reset(time.Second * time.Duration(ssr.config.deltaSnapshotIntervalSeconds))
	}
	return nil
}

func (ssr *Snapshotter) takeDeltaSnapshot() error {
	ssr.logger.Infof("Taking delta snapshot for time: %s", time.Now().Local())

	if len(ssr.events) == 0 {
		ssr.logger.Infof("No events received to save snapshot. Skipping delta snapshot.")
		return nil
	}

	snap := snapstore.NewSnapshot(snapstore.SnapshotKindDelta, ssr.prevSnapshot.LastRevision+1, ssr.events[len(ssr.events)-1].EtcdEvent.Kv.ModRevision)
	snap.SnapDir = ssr.prevSnapshot.SnapDir
	data, err := json.Marshal(ssr.events)
	ssr.events = []*event{}
	if err != nil {
		return err
	}
	// compute hash
	hash := sha256.New()
	if _, err := hash.Write(data); err != nil {
		return fmt.Errorf("failed to compute hash of events: %v", err)
	}
	data = hash.Sum(data)

	dataReader := bytes.NewReader(data)
	startTime := time.Now()
	if err := ssr.config.store.Save(*snap, dataReader); err != nil {
		timeTaken := time.Now().Sub(startTime).Seconds()
		metrics.SnapshotDurationSeconds.With(prometheus.Labels{metrics.LabelKind: snapstore.SnapshotKindDelta, metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Observe(timeTaken)
		ssr.logger.Errorf("Error saving delta snapshots. %v", err)
		return err
	}
	timeTaken := time.Now().Sub(startTime).Seconds()
	metrics.SnapshotDurationSeconds.With(prometheus.Labels{metrics.LabelKind: snapstore.SnapshotKindDelta, metrics.LabelSucceeded: metrics.ValueSucceededTrue}).Observe(timeTaken)
	logrus.Infof("Total time to save delta snapshot: %f seconds.", timeTaken)
	ssr.prevSnapshot = snap
	metrics.LatestSnapshotRevision.With(prometheus.Labels{metrics.LabelKind: ssr.prevSnapshot.Kind}).Set(float64(ssr.prevSnapshot.LastRevision))
	metrics.LatestSnapshotTimestamp.With(prometheus.Labels{metrics.LabelKind: ssr.prevSnapshot.Kind}).Set(float64(ssr.prevSnapshot.CreatedOn.Unix()))
	ssr.logger.Infof("Successfully saved delta snapshot at: %s", path.Join(snap.SnapDir, snap.SnapName))
	return nil
}

func (ssr *Snapshotter) handleDeltaWatchEvents(wr clientv3.WatchResponse) error {
	if err := wr.Err(); err != nil {
		return err
	}
	// aggregate events
	for _, ev := range wr.Events {
		timedEvent := newEvent(ev)
		jsonByte, err := json.Marshal(timedEvent)
		if err != nil {
			return fmt.Errorf("failed to marshal events to json: %v", err)
		}
		ssr.eventMemory = ssr.eventMemory + len(jsonByte)
		ssr.events = append(ssr.events, timedEvent)
	}
	ssr.logger.Debugf("Added events till revision: %d", ssr.events[len(ssr.events)-1].EtcdEvent.Kv.ModRevision)
	if ssr.eventMemory >= ssr.config.deltaSnapshotMemoryLimit {
		ssr.logger.Infof("Delta events memory crossed the memory limit: %d Bytes", ssr.eventMemory)
		return ssr.takeDeltaSnapshotAndResetTimer()
	}
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
		case <-ssr.fullSnapshotCh:
			if err := ssr.TakeFullSnapshotAndResetTimer(); err != nil {
				return err
			}
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
