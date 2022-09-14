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
	"io"
	"path"
	"sync"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/compressor"
	"github.com/gardener/etcd-backup-restore/pkg/errors"
	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	"github.com/gardener/etcd-backup-restore/pkg/health/heartbeat"
	"github.com/gardener/etcd-backup-restore/pkg/metrics"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/gardener/etcd-backup-restore/pkg/wrappers"

	"github.com/prometheus/client_golang/prometheus"
	cron "github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	emptyStruct   struct{}
	snapstoreHash = make(map[string]interface{})
)

// event is wrapper over etcd event to keep track of time of event
type event struct {
	EtcdEvent *clientv3.Event `json:"etcdEvent"`
	Time      time.Time       `json:"time"`
}

type result struct {
	Snapshot *brtypes.Snapshot `json:"snapshot"`
	Err      error             `json:"error"`
}

// NewSnapshotterConfig returns the snapshotter config.
func NewSnapshotterConfig() *brtypes.SnapshotterConfig {
	return &brtypes.SnapshotterConfig{
		FullSnapshotSchedule:     brtypes.DefaultFullSnapshotSchedule,
		DeltaSnapshotPeriod:      wrappers.Duration{Duration: brtypes.DefaultDeltaSnapshotInterval},
		DeltaSnapshotMemoryLimit: brtypes.DefaultDeltaSnapMemoryLimit,
		GarbageCollectionPeriod:  wrappers.Duration{Duration: brtypes.DefaultGarbageCollectionPeriod},
		GarbageCollectionPolicy:  brtypes.GarbageCollectionPolicyExponential,
		MaxBackups:               brtypes.DefaultMaxBackups,
	}
}

// Snapshotter is a struct for etcd snapshot taker
type Snapshotter struct {
	logger               *logrus.Entry
	etcdConnectionConfig *brtypes.EtcdConnectionConfig
	store                brtypes.SnapStore
	config               *brtypes.SnapshotterConfig
	compressionConfig    *compressor.CompressionConfig
	healthConfig         *brtypes.HealthConfig
	schedule             cron.Schedule
	prevSnapshot         *brtypes.Snapshot
	PrevFullSnapshot     *brtypes.Snapshot
	PrevDeltaSnapshots   brtypes.SnapList
	fullSnapshotReqCh    chan bool
	deltaSnapshotReqCh   chan struct{}
	fullSnapshotAckCh    chan result
	deltaSnapshotAckCh   chan result
	fullSnapshotTimer    *time.Timer
	deltaSnapshotTimer   *time.Timer
	events               []byte
	watchCh              clientv3.WatchChan
	etcdWatchClient      *clientv3.Watcher
	cancelWatch          context.CancelFunc
	SsrStateMutex        *sync.Mutex
	SsrState             brtypes.SnapshotterState
	lastEventRevision    int64
	K8sClientset         client.Client
	snapstoreConfig      *brtypes.SnapstoreConfig
}

// NewSnapshotter returns the snapshotter object.
func NewSnapshotter(logger *logrus.Entry, config *brtypes.SnapshotterConfig, store brtypes.SnapStore, etcdConnectionConfig *brtypes.EtcdConnectionConfig, compressionConfig *compressor.CompressionConfig, healthConfig *brtypes.HealthConfig, storeConfig *brtypes.SnapstoreConfig) (*Snapshotter, error) {
	sdl, err := cron.ParseStandard(config.FullSnapshotSchedule)
	if err != nil {
		// Ideally this should be validated before.
		return nil, fmt.Errorf("invalid schedule provied %s : %v", config.FullSnapshotSchedule, err)
	}

	var prevSnapshot *brtypes.Snapshot
	fullSnap, deltaSnapList, err := miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
	if err != nil {
		return nil, err
	} else if fullSnap != nil && len(deltaSnapList) == 0 {
		prevSnapshot = fullSnap
		// setting timestamps of both full and delta to prev full snapshot's timestamp
		metrics.LatestSnapshotTimestamp.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindFull}).Set(float64(prevSnapshot.CreatedOn.Unix()))
		metrics.LatestSnapshotTimestamp.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindDelta}).Set(float64(prevSnapshot.CreatedOn.Unix()))
	} else if fullSnap != nil && len(deltaSnapList) != 0 {
		prevSnapshot = deltaSnapList[len(deltaSnapList)-1]
		metrics.LatestSnapshotTimestamp.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindFull}).Set(float64(fullSnap.CreatedOn.Unix()))
		metrics.LatestSnapshotTimestamp.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindDelta}).Set(float64(prevSnapshot.CreatedOn.Unix()))
	} else {
		// creating dummy previous snapshot since fullSnap == nil
		prevSnapshot = snapstore.NewSnapshot(brtypes.SnapshotKindFull, 0, 0, "", false)
	}

	metrics.LatestSnapshotRevision.With(prometheus.Labels{metrics.LabelKind: prevSnapshot.Kind}).Set(float64(prevSnapshot.LastRevision))

	//Attempt to create clientset only if `enable-snapshot-lease-renewal` flag of healthConfig is set
	var clientSet client.Client
	if healthConfig.SnapshotLeaseRenewalEnabled {
		clientSet, err = miscellaneous.GetKubernetesClientSetOrError()
		if err != nil {
			return nil, err
		}
	}

	return &Snapshotter{
		logger:               logger.WithField("actor", "snapshotter"),
		store:                store,
		config:               config,
		etcdConnectionConfig: etcdConnectionConfig,
		compressionConfig:    compressionConfig,
		healthConfig:         healthConfig,

		schedule:           sdl,
		prevSnapshot:       prevSnapshot,
		PrevFullSnapshot:   fullSnap,
		PrevDeltaSnapshots: deltaSnapList,
		SsrState:           brtypes.SnapshotterInactive,
		SsrStateMutex:      &sync.Mutex{},
		fullSnapshotReqCh:  make(chan bool),
		deltaSnapshotReqCh: make(chan struct{}),
		fullSnapshotAckCh:  make(chan result),
		deltaSnapshotAckCh: make(chan result),
		cancelWatch:        func() {},
		K8sClientset:       clientSet,
		snapstoreConfig:    storeConfig,
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

	ssr.deltaSnapshotTimer = time.NewTimer(brtypes.DefaultDeltaSnapshotInterval)
	if ssr.config.DeltaSnapshotPeriod.Duration >= brtypes.DeltaSnapshotIntervalThreshold {
		ssr.deltaSnapshotTimer.Stop()
		ssr.deltaSnapshotTimer.Reset(ssr.config.DeltaSnapshotPeriod.Duration)
	}

	return ssr.snapshotEventHandler(stopCh)
}

// TriggerFullSnapshot sends the events to take full snapshot. This is to
// trigger full snapshot externally out of regular schedule.
func (ssr *Snapshotter) TriggerFullSnapshot(ctx context.Context, isFinal bool) (*brtypes.Snapshot, error) {
	ssr.SsrStateMutex.Lock()
	defer ssr.SsrStateMutex.Unlock()

	if ssr.SsrState != brtypes.SnapshotterActive {
		return nil, fmt.Errorf("snapshotter is not active")
	}
	ssr.logger.Info("Triggering out of schedule full snapshot...")
	ssr.fullSnapshotReqCh <- isFinal
	res := <-ssr.fullSnapshotAckCh
	return res.Snapshot, res.Err
}

// TriggerDeltaSnapshot sends the events to take delta snapshot. This is to
// trigger delta snapshot externally out of regular schedule.
func (ssr *Snapshotter) TriggerDeltaSnapshot() (*brtypes.Snapshot, error) {
	ssr.SsrStateMutex.Lock()
	defer ssr.SsrStateMutex.Unlock()

	if ssr.SsrState != brtypes.SnapshotterActive {
		return nil, fmt.Errorf("snapshotter is not active")
	}
	if ssr.config.DeltaSnapshotPeriod.Duration < brtypes.DeltaSnapshotIntervalThreshold {
		return nil, fmt.Errorf("found delta snapshot interval %s less than %v. Delta snapshotting is disabled. ", ssr.config.DeltaSnapshotPeriod.Duration, time.Duration(brtypes.DeltaSnapshotIntervalThreshold))
	}
	ssr.logger.Info("Triggering out of schedule delta snapshot...")
	ssr.deltaSnapshotReqCh <- emptyStruct
	res := <-ssr.deltaSnapshotAckCh
	return res.Snapshot, res.Err
}

// stop stops the snapshotter. Once stopped any subsequent calls will
// not have any effect.
func (ssr *Snapshotter) stop() {
	ssr.logger.Info("Closing the Snapshotter...")
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

	ssr.SsrState = brtypes.SnapshotterInactive
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

	if ssr.etcdWatchClient != nil {
		if err := (*ssr.etcdWatchClient).Close(); err != nil {
			ssr.logger.Warnf("Error while closing etcd watch client connection, %v", err)
		}
		ssr.etcdWatchClient = nil
	}
}

// TakeFullSnapshotAndResetTimer takes a full snapshot and resets the full snapshot
// timer as per the schedule.
func (ssr *Snapshotter) TakeFullSnapshotAndResetTimer(isFinal bool) (*brtypes.Snapshot, error) {
	ssr.logger.Infof("Taking scheduled full snapshot for time: %s", time.Now().Local())
	s, err := ssr.takeFullSnapshot(isFinal)
	if err != nil {
		// As per design principle, in business critical service if backup is not working,
		// it's better to fail the process. So, we are quiting here.
		ssr.logger.Warnf("Taking scheduled full snapshot failed: %v", err)
		return nil, err
	}

	return s, ssr.resetFullSnapshotTimer()
}

// takeFullSnapshot will store full snapshot of etcd to brtypes.
// It basically will connect to etcd. Then ask for snapshot. And finally
// store it to underlying snapstore on the fly.
func (ssr *Snapshotter) takeFullSnapshot(isFinal bool) (*brtypes.Snapshot, error) {
	defer ssr.cleanupInMemoryEvents()
	// close previous watch and client.
	ssr.closeEtcdClient()

	var err error

	// Update the snapstore object before taking every full snapshot
	// Refer: https://github.com/gardener/etcd-backup-restore/issues/422
	ssr.store, err = snapstore.GetSnapstore(ssr.snapstoreConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapstore from configured storage provider: %v", err)
	}

	clientFactory := etcdutil.NewFactory(*ssr.etcdConnectionConfig)
	clientKV, err := clientFactory.NewKV()
	if err != nil {
		return nil, &errors.EtcdError{
			Message: fmt.Sprintf("failed to create etcd KV client: %v", err),
		}
	}
	defer clientKV.Close()

	ctx, cancel := context.WithTimeout(context.TODO(), ssr.etcdConnectionConfig.ConnectionTimeout.Duration)
	// Note: Although Get and snapshot call are not atomic, so revision number in snapshot file
	// may be ahead of the revision found from GET call. But currently this is the only workaround available
	// Refer: https://github.com/coreos/etcd/issues/9037
	resp, err := clientKV.Get(ctx, "", clientv3.WithLastRev()...)
	cancel()
	if err != nil {
		return nil, &errors.EtcdError{
			Message: fmt.Sprintf("failed to get etcd latest revision: %v", err),
		}
	}
	lastRevision := resp.Header.Revision

	if ssr.prevSnapshot.Kind == brtypes.SnapshotKindFull && ssr.prevSnapshot.LastRevision == lastRevision && ssr.prevSnapshot.IsFinal == isFinal {
		ssr.logger.Infof("There are no updates since last snapshot, skipping full snapshot.")
	} else {
		// Note: As FullSnapshot size can be very large, so to avoid context timeout use "SnapshotTimeout" in context.WithTimeout()
		ctx, cancel = context.WithTimeout(context.TODO(), ssr.etcdConnectionConfig.SnapshotTimeout.Duration)
		defer cancel()
		// compressionSuffix is useful in backward compatibility(restoring from uncompressed snapshots).
		// it is also helpful in inferring which compression Policy to be used to decompress the snapshot.
		compressionSuffix, err := compressor.GetCompressionSuffix(ssr.compressionConfig.Enabled, ssr.compressionConfig.CompressionPolicy)
		if err != nil {
			return nil, fmt.Errorf("failed to get compressionSuffix: %v", err)
		}

		clientMaintenance, err := clientFactory.NewMaintenance()
		if err != nil {
			return nil, fmt.Errorf("failed to build etcd maintenance client")
		}
		defer clientMaintenance.Close()

		s, err := etcdutil.TakeAndSaveFullSnapshot(ctx, clientMaintenance, ssr.store, lastRevision, ssr.compressionConfig, compressionSuffix, isFinal, ssr.logger)
		if err != nil {
			return nil, err
		}

		ssr.prevSnapshot = s
		ssr.PrevFullSnapshot = s
		ssr.PrevDeltaSnapshots = nil

		metrics.LatestSnapshotRevision.With(prometheus.Labels{metrics.LabelKind: ssr.prevSnapshot.Kind}).Set(float64(ssr.prevSnapshot.LastRevision))
		metrics.LatestSnapshotTimestamp.With(prometheus.Labels{metrics.LabelKind: ssr.prevSnapshot.Kind}).Set(float64(ssr.prevSnapshot.CreatedOn.Unix()))
		metrics.SnapstoreLatestDeltasTotal.With(prometheus.Labels{}).Set(0)
		metrics.SnapstoreLatestDeltasRevisionsTotal.With(prometheus.Labels{}).Set(0)

		ssr.logger.Infof("Successfully saved full snapshot at: %s", path.Join(s.SnapDir, s.SnapName))
	}
	// setting `snapshotRequired` to 0 for both full and delta snapshot
	// for the following cases:
	// i.  Skipped full snapshot since no events were collected
	// ii. Successfully took a full snapshot
	metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindFull}).Set(0)
	metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindDelta}).Set(0)

	if ssr.config.DeltaSnapshotPeriod.Duration < time.Second {
		// return without creating a watch on events
		return ssr.prevSnapshot, nil
	}

	ssrEtcdWatchClient, err := clientFactory.NewWatcher()
	if err != nil {
		return nil, &errors.EtcdError{
			Message: fmt.Sprintf("failed to create etcd watch client for snapshotter: %v", err),
		}
	}
	watchCtx, cancelWatch := context.WithCancel(context.TODO())
	ssr.cancelWatch = cancelWatch
	ssr.etcdWatchClient = &ssrEtcdWatchClient
	ssr.watchCh = ssrEtcdWatchClient.Watch(watchCtx, "", clientv3.WithPrefix(), clientv3.WithRev(ssr.prevSnapshot.LastRevision+1))
	ssr.logger.Infof("Applied watch on etcd from revision: %d", ssr.prevSnapshot.LastRevision+1)

	return ssr.prevSnapshot, nil
}

func (ssr *Snapshotter) cleanupInMemoryEvents() {
	ssr.events = []byte{}
	ssr.lastEventRevision = -1
}

func (ssr *Snapshotter) takeDeltaSnapshotAndResetTimer() (*brtypes.Snapshot, error) {
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
func (ssr *Snapshotter) TakeDeltaSnapshot() (*brtypes.Snapshot, error) {
	defer ssr.cleanupInMemoryEvents()
	ssr.logger.Infof("Taking delta snapshot for time: %s", time.Now().Local())

	if len(ssr.events) == 0 {
		ssr.logger.Infof("No events received to save snapshot. Skipping delta snapshot.")
		metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindDelta}).Set(0)
		return nil, nil
	}
	ssr.events = append(ssr.events, byte(']'))

	isSecretUpdated := ssr.checkSnapstoreSecretUpdate()
	if isSecretUpdated {
		var err error

		// Update the snapstore object before taking every delta snapshot
		// Refer: https://github.com/gardener/etcd-backup-restore/issues/422
		ssr.store, err = snapstore.GetSnapstore(ssr.snapstoreConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create snapstore from configured storage provider: %v", err)
		}
		ssr.logger.Info("updated the snapstore object with new credentials")
	}

	// compressionSuffix is useful in backward compatibility(restoring from uncompressed snapshots).
	// it is also helpful in inferring which compression Policy to be used to decompress the snapshot.
	compressionSuffix, err := compressor.GetCompressionSuffix(ssr.compressionConfig.Enabled, ssr.compressionConfig.CompressionPolicy)
	if err != nil {
		return nil, fmt.Errorf("failed to get compressionSuffix: %v", err)
	}
	snap := snapstore.NewSnapshot(brtypes.SnapshotKindDelta, ssr.prevSnapshot.LastRevision+1, ssr.lastEventRevision, compressionSuffix, false)

	// compute hash
	hash := sha256.New()
	if _, err := hash.Write(ssr.events); err != nil {
		return nil, fmt.Errorf("failed to compute hash of events: %v", err)
	}
	ssr.events = hash.Sum(ssr.events)

	startTime := time.Now()
	rc := io.NopCloser(bytes.NewReader(ssr.events))

	// if compression is enabled
	//    then compress the snapshot.
	if ssr.compressionConfig.Enabled {
		ssr.logger.Info("start the Compression of delta snapshot")
		rc, err = compressor.CompressSnapshot(rc, ssr.compressionConfig.CompressionPolicy)
		if err != nil {
			return nil, fmt.Errorf("unable to compress delta snapshot: %v", err)
		}
	}
	defer rc.Close()

	if err := ssr.store.Save(*snap, rc); err != nil {
		timeTaken := time.Now().Sub(startTime).Seconds()
		metrics.SnapshotDurationSeconds.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindDelta, metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Observe(timeTaken)
		ssr.logger.Errorf("Error saving delta snapshots. %v", err)
		return nil, err
	}
	timeTaken := time.Now().Sub(startTime).Seconds()
	metrics.SnapshotDurationSeconds.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindDelta, metrics.LabelSucceeded: metrics.ValueSucceededTrue}).Observe(timeTaken)
	logrus.Infof("Total time to save delta snapshot: %f seconds.", timeTaken)
	ssr.prevSnapshot = snap
	ssr.PrevDeltaSnapshots = append(ssr.PrevDeltaSnapshots, snap)

	metrics.LatestSnapshotRevision.With(prometheus.Labels{metrics.LabelKind: ssr.prevSnapshot.Kind}).Set(float64(ssr.prevSnapshot.LastRevision))
	metrics.LatestSnapshotTimestamp.With(prometheus.Labels{metrics.LabelKind: ssr.prevSnapshot.Kind}).Set(float64(ssr.prevSnapshot.CreatedOn.Unix()))
	metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindDelta}).Set(0)
	metrics.SnapstoreLatestDeltasTotal.With(prometheus.Labels{}).Inc()
	metrics.SnapstoreLatestDeltasRevisionsTotal.With(prometheus.Labels{}).Add(float64(snap.LastRevision - snap.StartRevision))

	ssr.logger.Infof("Successfully saved delta snapshot at: %s", path.Join(snap.SnapDir, snap.SnapName))
	return snap, nil
}

// CollectEventsSincePrevSnapshot takes the first delta snapshot on etcd startup.
func (ssr *Snapshotter) CollectEventsSincePrevSnapshot(stopCh <-chan struct{}) (bool, error) {
	// close any previous watch and client.
	ssr.closeEtcdClient()

	clientFactory := etcdutil.NewFactory(*ssr.etcdConnectionConfig)
	clientKV, err := clientFactory.NewKV()
	if err != nil {
		return false, &errors.EtcdError{
			Message: fmt.Sprintf("failed to create etcd KV client: %v", err),
		}
	}
	defer clientKV.Close()

	ctx, cancel := context.WithTimeout(context.TODO(), ssr.etcdConnectionConfig.ConnectionTimeout.Duration)
	resp, err := clientKV.Get(ctx, "", clientv3.WithLastRev()...)
	cancel()
	if err != nil {
		return false, &errors.EtcdError{
			Message: fmt.Sprintf("failed to get etcd latest revision: %v", err),
		}
	}
	lastEtcdRevision := resp.Header.Revision

	metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindFull}).Set(0)
	metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindDelta}).Set(0)

	// if etcd revision newer than latest full snapshot revision,
	// set `required` metric for full snapshot to 1
	if ssr.PrevFullSnapshot == nil || ssr.PrevFullSnapshot.LastRevision != lastEtcdRevision {
		metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindFull}).Set(1)
	}

	ssrEtcdWatchClient, err := clientFactory.NewWatcher()
	if err != nil {
		return false, &errors.EtcdError{
			Message: fmt.Sprintf("failed to create etcd watch client for snapshotter: %v", err),
		}
	}
	// TODO: Use parent context. Passing parent context here directly requires some additional management of error handling.
	watchCtx, cancelWatch := context.WithCancel(context.TODO())
	ssr.cancelWatch = cancelWatch
	ssr.etcdWatchClient = &ssrEtcdWatchClient
	ssr.watchCh = ssrEtcdWatchClient.Watch(watchCtx, "", clientv3.WithPrefix(), clientv3.WithRev(ssr.prevSnapshot.LastRevision+1))
	ssr.logger.Infof("Applied watch on etcd from revision: %d", ssr.prevSnapshot.LastRevision+1)

	if ssr.prevSnapshot.LastRevision == lastEtcdRevision {
		ssr.logger.Infof("No new events since last snapshot. Skipping initial delta snapshot.")
		return false, nil
	}

	// need to take a delta snapshot here, because etcd revision is
	// newer than latest snapshot revision. Also means, a subsequent
	// full snapshot will be required later
	metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindDelta}).Set(1)
	metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindFull}).Set(1)

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
		metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindFull}).Set(1)
		metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindDelta}).Set(1)
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
	leaseUpdateCtx, leaseUpdateCancel := context.WithCancel(context.TODO())
	defer leaseUpdateCancel()
	for {
		select {
		case isFinal := <-ssr.fullSnapshotReqCh:
			s, err := ssr.TakeFullSnapshotAndResetTimer(isFinal)
			res := result{
				Snapshot: s,
				Err:      err,
			}
			ssr.fullSnapshotAckCh <- res
			if err != nil {
				return err
			}
			if ssr.healthConfig.SnapshotLeaseRenewalEnabled {
				ctx, cancel := context.WithTimeout(leaseUpdateCtx, brtypes.LeaseUpdateTimeoutDuration)
				if err = heartbeat.FullSnapshotCaseLeaseUpdate(ctx, ssr.logger, ssr.PrevFullSnapshot, ssr.K8sClientset, ssr.healthConfig.FullSnapshotLeaseName, ssr.healthConfig.DeltaSnapshotLeaseName); err != nil {
					ssr.logger.Warnf("Snapshot lease update failed : %v", err)
				}
				cancel()
			}

		case <-ssr.deltaSnapshotReqCh:
			s, err := ssr.takeDeltaSnapshotAndResetTimer()
			res := result{
				Snapshot: s,
				Err:      err,
			}
			ssr.deltaSnapshotAckCh <- res
			if err != nil {
				return err
			}
			if ssr.healthConfig.SnapshotLeaseRenewalEnabled {
				ctx, cancel := context.WithTimeout(leaseUpdateCtx, brtypes.LeaseUpdateTimeoutDuration)
				if err = heartbeat.DeltaSnapshotCaseLeaseUpdate(ctx, ssr.logger, ssr.K8sClientset, ssr.healthConfig.DeltaSnapshotLeaseName, ssr.store); err != nil {
					ssr.logger.Warnf("Snapshot lease update failed : %v", err)
				}
				cancel()
			}

		case <-ssr.fullSnapshotTimer.C:
			if _, err := ssr.TakeFullSnapshotAndResetTimer(false); err != nil {
				return err
			}
			if ssr.healthConfig.SnapshotLeaseRenewalEnabled {
				ctx, cancel := context.WithTimeout(leaseUpdateCtx, brtypes.LeaseUpdateTimeoutDuration)
				if err := heartbeat.FullSnapshotCaseLeaseUpdate(ctx, ssr.logger, ssr.PrevFullSnapshot, ssr.K8sClientset, ssr.healthConfig.FullSnapshotLeaseName, ssr.healthConfig.DeltaSnapshotLeaseName); err != nil {
					ssr.logger.Warnf("Snapshot lease update failed : %v", err)
				}
				cancel()
			}

		case <-ssr.deltaSnapshotTimer.C:
			if ssr.config.DeltaSnapshotPeriod.Duration >= time.Second {
				if _, err := ssr.takeDeltaSnapshotAndResetTimer(); err != nil {
					return err
				}
				if ssr.healthConfig.SnapshotLeaseRenewalEnabled {
					ctx, cancel := context.WithTimeout(leaseUpdateCtx, brtypes.LeaseUpdateTimeoutDuration)
					if err := heartbeat.DeltaSnapshotCaseLeaseUpdate(ctx, ssr.logger, ssr.K8sClientset, ssr.healthConfig.DeltaSnapshotLeaseName, ssr.store); err != nil {
						ssr.logger.Warnf("Snapshot lease update failed : %v", err)
					}
					cancel()
				}
			}

		case wr, ok := <-ssr.watchCh:
			if !ok {
				return fmt.Errorf("watch channel closed")
			}
			snapshots := len(ssr.PrevDeltaSnapshots)
			if err := ssr.handleDeltaWatchEvents(wr); err != nil {
				return err
			}
			if ssr.healthConfig.SnapshotLeaseRenewalEnabled {
				//Call UpdateDeltaSnapshotLease only if new delta snapshot taken
				if snapshots < len(ssr.PrevDeltaSnapshots) {
					ctx, cancel := context.WithTimeout(leaseUpdateCtx, brtypes.LeaseUpdateTimeoutDuration)
					if err := heartbeat.DeltaSnapshotCaseLeaseUpdate(ctx, ssr.logger, ssr.K8sClientset, ssr.healthConfig.DeltaSnapshotLeaseName, ssr.store); err != nil {
						ssr.logger.Warnf("Snapshot lease update failed : %v", err)
					}
					cancel()
				}
			}

		case <-stopCh:
			leaseUpdateCancel()
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

func (ssr *Snapshotter) checkSnapstoreSecretUpdate() bool {
	ssr.logger.Debug("checking the hash of snapstore secret...")
	newSnapstoreSecretHash, err := snapstore.GetSnapstoreSecretHash(ssr.snapstoreConfig)
	if err != nil {
		return true
	}

	if snapstoreHash[ssr.snapstoreConfig.Provider] == newSnapstoreSecretHash {
		return false
	}

	//update the map with latest newSnapstoreHash
	snapstoreHash[ssr.snapstoreConfig.Provider] = newSnapstoreSecretHash
	return true
}
