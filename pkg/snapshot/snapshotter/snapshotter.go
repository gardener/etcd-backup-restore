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
	"io/ioutil"
	"path"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/gardener/etcd-backup-restore/pkg/errors"
	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	"github.com/gardener/etcd-backup-restore/pkg/metrics"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	"github.com/prometheus/client_golang/prometheus"
	cron "github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
)

// NewSnapshotter returns the snapshotter object.
func NewSnapshotter(logger *logrus.Entry, config *Config, store snapstore.SnapStore, etcdConnectionConfig *etcdutil.EtcdConnectionConfig) (*Snapshotter, error) {
	sdl, err := cron.ParseStandard(config.FullSnapshotSchedule)
	if err != nil {
		// Ideally this should be validated before.
		return nil, fmt.Errorf("invalid schedule provied %s : %v", config.FullSnapshotSchedule, err)
	}

	// Create dummy previous snapshot
	var prevSnapshot *snapstore.Snapshot
	fullSnap, deltaSnapList, err := miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
	if err != nil || fullSnap == nil {
		prevSnapshot = snapstore.NewSnapshot(snapstore.SnapshotKindFull, 0, 0)
	} else if len(deltaSnapList) == 0 {
		prevSnapshot = fullSnap
		metrics.LatestSnapshotTimestamp.With(prometheus.Labels{metrics.LabelKind: prevSnapshot.Kind}).Set(float64(prevSnapshot.CreatedOn.Unix()))
	} else {
		prevSnapshot = deltaSnapList[len(deltaSnapList)-1]
		metrics.LatestSnapshotTimestamp.With(prometheus.Labels{metrics.LabelKind: prevSnapshot.Kind}).Set(float64(prevSnapshot.CreatedOn.Unix()))
	}

	metrics.LatestSnapshotRevision.With(prometheus.Labels{metrics.LabelKind: prevSnapshot.Kind}).Set(float64(prevSnapshot.LastRevision))

	return &Snapshotter{
		logger:               logger.WithField("actor", "snapshotter"),
		store:                store,
		config:               config,
		etcdConnectionConfig: etcdConnectionConfig,

		schedule:           sdl,
		prevSnapshot:       prevSnapshot,
		PrevFullSnapshot:   fullSnap,
		PrevDeltaSnapshots: deltaSnapList,
		SsrState:           SnapshotterInactive,
		SsrStateMutex:      &sync.Mutex{},
		fullSnapshotReqCh:  make(chan struct{}),
		deltaSnapshotReqCh: make(chan struct{}),
		fullSnapshotAckCh:  make(chan result),
		deltaSnapshotAckCh: make(chan result),
		cancelWatch:        func() {},
	}, nil
}

// Run process loop for scheduled backup
// Setting startWithFullSnapshot to false will start the snapshotter without
// taking the first full snapshot.
func (ssr *Snapshotter) Run(stopCh <-chan struct{}, startWithFullSnapshot bool) error {
	defer ssr.stop()
	if startWithFullSnapshot {
		ssr.fullSnapshotTimer = time.NewTimer(0)
	} else {
		// for the case when snapshotter is run for the first time on
		// a fresh etcd with startWithFullSnapshot set to false, we need
		// to take the first delta snapshot(s) initially and then set
		// the full snapshot schedule
		if ssr.watchCh == nil {
			ssrStopped, err := ssr.CollectEventsSincePrevSnapshot(stopCh)
			if ssrStopped {
				return nil
			}
			if err != nil {
				return fmt.Errorf("Failed to collect events for first delta snapshot(s): %v", err)
			}
		}
		if err := ssr.resetFullSnapshotTimer(); err != nil {
			return fmt.Errorf("failed to reset full snapshot timer: %v", err)
		}
	}

	ssr.deltaSnapshotTimer = time.NewTimer(DefaultDeltaSnapshotInterval)
	if ssr.config.DeltaSnapshotPeriod.Duration >= deltaSnapshotIntervalThreshold {
		ssr.deltaSnapshotTimer.Stop()
		ssr.deltaSnapshotTimer.Reset(ssr.config.DeltaSnapshotPeriod.Duration)
	}

	return ssr.snapshotEventHandler(stopCh)
}

// TriggerFullSnapshot sends the events to take full snapshot. This is to
// trigger full snapshot externally out of regular schedule.
func (ssr *Snapshotter) TriggerFullSnapshot(ctx context.Context) (*snapstore.Snapshot, error) {
	ssr.SsrStateMutex.Lock()
	defer ssr.SsrStateMutex.Unlock()

	if ssr.SsrState != SnapshotterActive {
		return nil, fmt.Errorf("snapshotter is not active")
	}
	ssr.logger.Info("Triggering out of schedule full snapshot...")
	ssr.fullSnapshotReqCh <- emptyStruct
	res := <-ssr.fullSnapshotAckCh
	if res.Err != nil {
		return nil, res.Err
	}
	return res.Snapshot, nil
}

// TriggerDeltaSnapshot sends the events to take delta snapshot. This is to
// trigger delta snapshot externally out of regular schedule.
func (ssr *Snapshotter) TriggerDeltaSnapshot() (*snapstore.Snapshot, error) {
	ssr.SsrStateMutex.Lock()
	defer ssr.SsrStateMutex.Unlock()

	if ssr.SsrState != SnapshotterActive {
		return nil, fmt.Errorf("snapshotter is not active")
	}
	if ssr.config.DeltaSnapshotPeriod.Duration < deltaSnapshotIntervalThreshold {
		return nil, fmt.Errorf("Found delta snapshot interval %s less than %v. Delta snapshotting is disabled. ", ssr.config.DeltaSnapshotPeriod.Duration, time.Duration(deltaSnapshotIntervalThreshold))
	}
	ssr.logger.Info("Triggering out of schedule delta snapshot...")
	ssr.deltaSnapshotReqCh <- emptyStruct
	res := <-ssr.deltaSnapshotAckCh
	if res.Err != nil {
		return nil, res.Err
	}
	return res.Snapshot, nil
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
	ssr.closeEtcdClient()

	ssr.SsrState = SnapshotterInactive
	ssr.SsrStateMutex.Unlock()
}

func (ssr *Snapshotter) closeEtcdClient() {
	if ssr.cancelWatch != nil {
		ssr.cancelWatch()
		ssr.cancelWatch = nil
	}
	if ssr.watchCh != nil {
		ssr.watchCh = nil
	}

	if ssr.etcdClient != nil {
		if err := ssr.etcdClient.Close(); err != nil {
			ssr.logger.Warnf("Error while closing etcd client connection, %v", err)
		}
		ssr.etcdClient = nil
	}
}

// TakeFullSnapshotAndResetTimer takes a full snapshot and resets the full snapshot
// timer as per the schedule.
func (ssr *Snapshotter) TakeFullSnapshotAndResetTimer() (*snapstore.Snapshot, error) {
	ssr.logger.Infof("Taking scheduled snapshot for time: %s", time.Now().Local())
	s, err := ssr.takeFullSnapshot()
	if err != nil {
		// As per design principle, in business critical service if backup is not working,
		// it's better to fail the process. So, we are quiting here.
		ssr.logger.Warnf("Taking scheduled snapshot failed: %v", err)
		return nil, err
	}

	return s, ssr.resetFullSnapshotTimer()
}

// takeFullSnapshot will store full snapshot of etcd to snapstore.
// It basically will connect to etcd. Then ask for snapshot. And finally
// store it to underlying snapstore on the fly.
func (ssr *Snapshotter) takeFullSnapshot() (*snapstore.Snapshot, error) {
	defer ssr.cleanupInMemoryEvents()
	// close previous watch and client.
	ssr.closeEtcdClient()

	client, err := etcdutil.GetTLSClientForEtcd(ssr.etcdConnectionConfig)
	if err != nil {
		return nil, &errors.EtcdError{
			Message: fmt.Sprintf("failed to create etcd client: %v", err),
		}
	}

	ctx, cancel := context.WithTimeout(context.TODO(), ssr.etcdConnectionConfig.ConnectionTimeout.Duration)
	// Note: Although Get and snapshot call are not atomic, so revision number in snapshot file
	// may be ahead of the revision found from GET call. But currently this is the only workaround available
	// Refer: https://github.com/coreos/etcd/issues/9037
	resp, err := client.Get(ctx, "", clientv3.WithLastRev()...)
	cancel()
	if err != nil {
		return nil, &errors.EtcdError{
			Message: fmt.Sprintf("failed to get etcd latest revision: %v", err),
		}
	}
	lastRevision := resp.Header.Revision

	if ssr.prevSnapshot.Kind == snapstore.SnapshotKindFull && ssr.prevSnapshot.LastRevision == lastRevision {
		ssr.logger.Infof("There are no updates since last snapshot, skipping full snapshot.")
	} else {
		ctx, cancel = context.WithTimeout(context.TODO(), ssr.etcdConnectionConfig.ConnectionTimeout.Duration)
		defer cancel()
		rc, err := client.Snapshot(ctx)
		if err != nil {
			return nil, &errors.EtcdError{
				Message: fmt.Sprintf("failed to create etcd snapshot: %v", err),
			}
		}
		ssr.logger.Infof("Successfully opened snapshot reader on etcd")
		s := snapstore.NewSnapshot(snapstore.SnapshotKindFull, 0, lastRevision)
		startTime := time.Now()
		if err := ssr.store.Save(*s, rc); err != nil {
			timeTaken := time.Now().Sub(startTime).Seconds()
			metrics.SnapshotDurationSeconds.With(prometheus.Labels{metrics.LabelKind: snapstore.SnapshotKindFull, metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Observe(timeTaken)
			return nil, &errors.SnapstoreError{
				Message: fmt.Sprintf("failed to save snapshot: %v", err),
			}
		}
		timeTaken := time.Now().Sub(startTime).Seconds()
		metrics.SnapshotDurationSeconds.With(prometheus.Labels{metrics.LabelKind: snapstore.SnapshotKindFull, metrics.LabelSucceeded: metrics.ValueSucceededTrue}).Observe(timeTaken)
		logrus.Infof("Total time to save snapshot: %f seconds.", timeTaken)
		ssr.prevSnapshot = s
		ssr.PrevFullSnapshot = s
		ssr.PrevDeltaSnapshots = nil

		metrics.LatestSnapshotRevision.With(prometheus.Labels{metrics.LabelKind: ssr.prevSnapshot.Kind}).Set(float64(ssr.prevSnapshot.LastRevision))
		metrics.LatestSnapshotTimestamp.With(prometheus.Labels{metrics.LabelKind: ssr.prevSnapshot.Kind}).Set(float64(ssr.prevSnapshot.CreatedOn.Unix()))

		ssr.logger.Infof("Successfully saved full snapshot at: %s", path.Join(s.SnapDir, s.SnapName))
	}
	// setting `snapshotRequired` to 0 for both full and delta snapshot
	// for the following cases:
	// i.  Skipped full snapshot since no events were collected
	// ii. Successfully took a full snapshot
	metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: snapstore.SnapshotKindFull}).Set(0)
	metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: snapstore.SnapshotKindDelta}).Set(0)

	if ssr.config.DeltaSnapshotPeriod.Duration < time.Second {
		// return without creating a watch on events
		return ssr.prevSnapshot, nil
	}

	watchCtx, cancelWatch := context.WithCancel(context.TODO())
	ssr.cancelWatch = cancelWatch
	ssr.etcdClient = client
	ssr.watchCh = client.Watch(watchCtx, "", clientv3.WithPrefix(), clientv3.WithRev(ssr.prevSnapshot.LastRevision+1))
	ssr.logger.Infof("Applied watch on etcd from revision: %d", ssr.prevSnapshot.LastRevision+1)

	return ssr.prevSnapshot, nil
}

func (ssr *Snapshotter) cleanupInMemoryEvents() {
	ssr.events = []byte{}
	ssr.lastEventRevision = -1
}

func (ssr *Snapshotter) takeDeltaSnapshotAndResetTimer() (*snapstore.Snapshot, error) {
	s, err := ssr.TakeDeltaSnapshot()
	if err != nil {
		// As per design principle, in business critical service if backup is not working,
		// it's better to fail the process. So, we are quiting here.
		ssr.logger.Warnf("Taking delta snapshot failed: %v", err)
		return nil, err
	}

	if ssr.deltaSnapshotTimer == nil {
		ssr.deltaSnapshotTimer = time.NewTimer(ssr.config.DeltaSnapshotPeriod.Duration)
	} else {
		ssr.logger.Infof("Stopping delta snapshot...")
		ssr.deltaSnapshotTimer.Stop()
		ssr.logger.Infof("Resetting delta snapshot to run after %s.", ssr.config.DeltaSnapshotPeriod.Duration.String())
		ssr.deltaSnapshotTimer.Reset(ssr.config.DeltaSnapshotPeriod.Duration)
	}
	return s, nil
}

// TakeDeltaSnapshot takes a delta snapshot that contains
// the etcd events collected up till now
func (ssr *Snapshotter) TakeDeltaSnapshot() (*snapstore.Snapshot, error) {
	defer ssr.cleanupInMemoryEvents()
	ssr.logger.Infof("Taking delta snapshot for time: %s", time.Now().Local())

	if len(ssr.events) == 0 {
		ssr.logger.Infof("No events received to save snapshot. Skipping delta snapshot.")
		return nil, nil
	}
	ssr.events = append(ssr.events, byte(']'))

	snap := snapstore.NewSnapshot(snapstore.SnapshotKindDelta, ssr.prevSnapshot.LastRevision+1, ssr.lastEventRevision)
	snap.SnapDir = ssr.prevSnapshot.SnapDir

	// compute hash
	hash := sha256.New()
	if _, err := hash.Write(ssr.events); err != nil {
		return nil, fmt.Errorf("failed to compute hash of events: %v", err)
	}
	ssr.events = hash.Sum(ssr.events)
	startTime := time.Now()
	if err := ssr.store.Save(*snap, ioutil.NopCloser(bytes.NewReader(ssr.events))); err != nil {
		timeTaken := time.Now().Sub(startTime).Seconds()
		metrics.SnapshotDurationSeconds.With(prometheus.Labels{metrics.LabelKind: snapstore.SnapshotKindDelta, metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Observe(timeTaken)
		ssr.logger.Errorf("Error saving delta snapshots. %v", err)
		return nil, err
	}
	timeTaken := time.Now().Sub(startTime).Seconds()
	metrics.SnapshotDurationSeconds.With(prometheus.Labels{metrics.LabelKind: snapstore.SnapshotKindDelta, metrics.LabelSucceeded: metrics.ValueSucceededTrue}).Observe(timeTaken)
	logrus.Infof("Total time to save delta snapshot: %f seconds.", timeTaken)
	ssr.prevSnapshot = snap
	ssr.PrevDeltaSnapshots = append(ssr.PrevDeltaSnapshots, snap)
	metrics.LatestSnapshotRevision.With(prometheus.Labels{metrics.LabelKind: ssr.prevSnapshot.Kind}).Set(float64(ssr.prevSnapshot.LastRevision))
	metrics.LatestSnapshotTimestamp.With(prometheus.Labels{metrics.LabelKind: ssr.prevSnapshot.Kind}).Set(float64(ssr.prevSnapshot.CreatedOn.Unix()))
	metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: snapstore.SnapshotKindDelta}).Set(0)
	ssr.logger.Infof("Successfully saved delta snapshot at: %s", path.Join(snap.SnapDir, snap.SnapName))
	return snap, nil
}

// CollectEventsSincePrevSnapshot takes the first delta snapshot on etcd startup.
func (ssr *Snapshotter) CollectEventsSincePrevSnapshot(stopCh <-chan struct{}) (bool, error) {
	// close any previous watch and client.
	ssr.closeEtcdClient()
	client, err := etcdutil.GetTLSClientForEtcd(ssr.etcdConnectionConfig)
	if err != nil {
		return false, &errors.EtcdError{
			Message: fmt.Sprintf("failed to create etcd client: %v", err),
		}
	}

	ctx, cancel := context.WithTimeout(context.TODO(), ssr.etcdConnectionConfig.ConnectionTimeout.Duration)
	resp, err := client.Get(ctx, "", clientv3.WithLastRev()...)
	cancel()
	if err != nil {
		return false, &errors.EtcdError{
			Message: fmt.Sprintf("failed to get etcd latest revision: %v", err),
		}
	}
	lastEtcdRevision := resp.Header.Revision

	metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: snapstore.SnapshotKindFull}).Set(0)
	metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: snapstore.SnapshotKindDelta}).Set(0)

	// if etcd revision newer than latest full snapshot revision,
	// set `required` metric for full snapshot to 1
	if ssr.PrevFullSnapshot == nil || ssr.PrevFullSnapshot.LastRevision != lastEtcdRevision {
		metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: snapstore.SnapshotKindFull}).Set(1)
	}

	// TODO: Use parent context. Passing parent context here directly requires some additional management of error handling.
	watchCtx, cancelWatch := context.WithCancel(context.TODO())
	ssr.cancelWatch = cancelWatch
	ssr.etcdClient = client
	ssr.watchCh = client.Watch(watchCtx, "", clientv3.WithPrefix(), clientv3.WithRev(ssr.prevSnapshot.LastRevision+1))
	ssr.logger.Infof("Applied watch on etcd from revision: %d", ssr.prevSnapshot.LastRevision+1)

	if ssr.prevSnapshot.LastRevision == lastEtcdRevision {
		ssr.logger.Infof("No new events since last snapshot. Skipping initial delta snapshot.")
		return false, nil
	}

	// need to take a delta snapshot here, because etcd revision is
	// newer than latest snapshot revision. Also means, a subsequent
	// full snapshot will be required later
	metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: snapstore.SnapshotKindDelta}).Set(1)
	metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: snapstore.SnapshotKindFull}).Set(1)

	for {
		select {
		case wr, ok := <-ssr.watchCh:
			if !ok {
				return false, fmt.Errorf("watch channel closed")
			}
			if err := ssr.handleDeltaWatchEvents(wr); err != nil {
				return false, err
			}

			lastWatchRevision := wr.Events[len(wr.Events)-1].Kv.ModRevision
			if lastWatchRevision >= lastEtcdRevision {
				return false, nil
			}
		case <-stopCh:
			ssr.cleanupInMemoryEvents()
			return true, nil
		}
	}
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
		if len(ssr.events) == 0 {
			ssr.events = append(ssr.events, byte('['))
		} else {
			ssr.events = append(ssr.events, byte(','))
		}
		ssr.events = append(ssr.events, jsonByte...)
		ssr.lastEventRevision = ev.Kv.ModRevision
		metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: snapstore.SnapshotKindFull}).Set(1)
		metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: snapstore.SnapshotKindDelta}).Set(1)
	}
	ssr.logger.Debugf("Added events till revision: %d", ssr.lastEventRevision)
	if len(ssr.events) >= int(ssr.config.DeltaSnapshotMemoryLimit) {
		ssr.logger.Infof("Delta events memory crossed the memory limit: %d Bytes", len(ssr.events))
		_, err := ssr.takeDeltaSnapshotAndResetTimer()
		return err
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
		case <-ssr.fullSnapshotReqCh:
			s, err := ssr.TakeFullSnapshotAndResetTimer()
			res := result{
				Snapshot: s,
				Err:      err,
			}
			if err != nil {
				ssr.fullSnapshotAckCh <- res
				return err
			}
			ssr.fullSnapshotAckCh <- res

		case <-ssr.deltaSnapshotReqCh:
			s, err := ssr.takeDeltaSnapshotAndResetTimer()
			res := result{
				Snapshot: nil,
				Err:      err,
			}
			if err != nil {
				ssr.deltaSnapshotAckCh <- res
				return err
			}
			res.Snapshot = s
			ssr.deltaSnapshotAckCh <- res

		case <-ssr.fullSnapshotTimer.C:
			if _, err := ssr.TakeFullSnapshotAndResetTimer(); err != nil {
				return err
			}

		case <-ssr.deltaSnapshotTimer.C:
			if ssr.config.DeltaSnapshotPeriod.Duration >= time.Second {
				if _, err := ssr.takeDeltaSnapshotAndResetTimer(); err != nil {
					return err
				}
			}

		case wr, ok := <-ssr.watchCh:
			if !ok {
				return fmt.Errorf("watch channel closed")
			}
			if err := ssr.handleDeltaWatchEvents(wr); err != nil {
				return err
			}

		case <-stopCh:
			ssr.cleanupInMemoryEvents()
			return nil
		}
	}
}

func (ssr *Snapshotter) resetFullSnapshotTimer() error {
	now := time.Now()
	effective := ssr.schedule.Next(now)
	if effective.IsZero() {
		ssr.logger.Info("There are no backups scheduled for the future. Stopping now.")
		return fmt.Errorf("error in full snapshot schedule")
	}
	duration := effective.Sub(now)
	if ssr.fullSnapshotTimer == nil {
		ssr.fullSnapshotTimer = time.NewTimer(duration)
	} else {
		ssr.logger.Infof("Stopping full snapshot...")
		ssr.fullSnapshotTimer.Stop()
		ssr.logger.Infof("Resetting full snapshot to run after %s", duration)
		ssr.fullSnapshotTimer.Reset(duration)
	}
	ssr.logger.Infof("Will take next full snapshot at time: %s", effective)

	return nil
}
