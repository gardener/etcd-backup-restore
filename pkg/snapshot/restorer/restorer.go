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

package restorer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/coreos/etcd/etcdserver"
	"github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/etcdserver/membership"
	"github.com/coreos/etcd/lease"
	"github.com/coreos/etcd/mvcc"
	"github.com/coreos/etcd/mvcc/backend"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/snap"
	"github.com/coreos/etcd/store"
	"github.com/coreos/etcd/wal"
	"github.com/coreos/etcd/wal/walpb"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	"github.com/sirupsen/logrus"
)

const (
	numMaxFetchers          = 10
	subsequentFetchDelay    = time.Millisecond * 500
	waitDuration            = time.Second * 2
	tmpDir                  = "/tmp"
	tmpEventsDataFilePrefix = "etcd-restore-"
)

// NewRestorer returns the snapshotter object.
func NewRestorer(store snapstore.SnapStore, logger *logrus.Logger) *Restorer {
	return &Restorer{
		logger: logger,
		store:  store,
	}
}

// Restore restore the etcd data directory as per specified restore options
func (r *Restorer) Restore(ro RestoreOptions) error {
	if err := r.restoreFromBaseSnapshot(ro); err != nil {
		return fmt.Errorf("failed to restore from the base snapshot :%v", err)
	}
	if len(ro.DeltaSnapList) == 0 {
		r.logger.Infof("No delta snapshots present over base snapshot.")
		return nil
	}
	r.logger.Infof("Starting embedded etcd server...")
	e, err := startEmbeddedEtcd(ro)
	if err != nil {
		return err
	}
	defer func() {
		e.Server.Stop()
		e.Close()
	}()

	client, err := clientv3.NewFromURL(e.Clients[0].Addr().String())
	if err != nil {
		return err
	}
	defer client.Close()

	r.logger.Infof("Applying delta snapshots...")
	return r.applyDeltaSnapshots(client, ro.DeltaSnapList)
}

// restoreFromBaseSnapshot restore the etcd data directory from base snapshot
func (r *Restorer) restoreFromBaseSnapshot(ro RestoreOptions) error {
	var err error
	if path.Join(ro.BaseSnapshot.SnapDir, ro.BaseSnapshot.SnapName) == "" {
		r.logger.Warnf("Base snapshot path not provided. Will do nothing.")
		return nil
	}
	r.logger.Infof("Restoring from base snapshot: %s", path.Join(ro.BaseSnapshot.SnapDir, ro.BaseSnapshot.SnapName))
	cfg := etcdserver.ServerConfig{
		InitialClusterToken: ro.ClusterToken,
		InitialPeerURLsMap:  ro.ClusterURLs,
		PeerURLs:            ro.PeerURLs,
		Name:                ro.Name,
	}
	if err := cfg.VerifyBootstrap(); err != nil {
		return err
	}

	cl, err := membership.NewClusterFromURLsMap(ro.ClusterToken, ro.ClusterURLs)
	if err != nil {
		return err
	}

	memberDir := filepath.Join(ro.RestoreDataDir, "member")
	if _, err := os.Stat(memberDir); err == nil {
		return fmt.Errorf("member directory in data directory(%q) exists", memberDir)
	}

	walDir := filepath.Join(ro.RestoreDataDir, "member", "wal")
	snapdir := filepath.Join(ro.RestoreDataDir, "member", "snap")
	if err = makeDB(snapdir, ro.BaseSnapshot, len(cl.Members()), r.store, false); err != nil {
		return err
	}
	return makeWALAndSnap(walDir, snapdir, cl, ro.Name)
}

// makeDB copies the database snapshot to the snapshot directory
func makeDB(snapdir string, snap snapstore.Snapshot, commit int, ss snapstore.SnapStore, skipHashCheck bool) error {
	rc, err := ss.Fetch(snap)
	if err != nil {
		return err
	}
	defer rc.Close()

	if err := fileutil.CreateDirAll(snapdir); err != nil {
		return err
	}

	dbPath := filepath.Join(snapdir, "db")
	db, err := os.OpenFile(dbPath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(db, rc); err != nil {
		return err
	}
	db.Sync()

	off, err := db.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	hasHash := (off % 512) == sha256.Size
	if !hasHash && !skipHashCheck {
		err := fmt.Errorf("snapshot missing hash but --skip-hash-check=false")
		return err
	}

	if hasHash {
		// get snapshot integrity hash
		if _, err = db.Seek(-sha256.Size, io.SeekEnd); err != nil {
			return err
		}
		sha := make([]byte, sha256.Size)
		if _, err := db.Read(sha); err != nil {
			return err
		}
		// truncate away integrity hash
		if err = db.Truncate(off - sha256.Size); err != nil {
			return err
		}

		if !skipHashCheck {
			if _, err := db.Seek(0, io.SeekStart); err != nil {
				return err
			}
			// check for match
			h := sha256.New()
			if _, err = io.Copy(h, db); err != nil {
				return err
			}
			dbSha := h.Sum(nil)
			if !reflect.DeepEqual(sha, dbSha) {
				err := fmt.Errorf("expected sha256 %v, got %v", sha, dbSha)
				return err
			}
		}
	}

	// db hash is OK
	db.Close()
	// update consistentIndex so applies go through on etcdserver despite
	// having a new raft instance
	be := backend.NewDefaultBackend(dbPath)
	// a lessor never timeouts leases
	lessor := lease.NewLessor(be, math.MaxInt64)
	s := mvcc.NewStore(be, lessor, (*initIndex)(&commit))
	txn := s.Write()
	btx := be.BatchTx()
	del := func(k, v []byte) error {
		txn.DeleteRange(k, nil)
		return nil
	}

	// delete stored members from old cluster since using new members
	btx.UnsafeForEach([]byte("members"), del)
	// todo: add back new members when we start to deprecate old snap file.
	btx.UnsafeForEach([]byte("members_removed"), del)
	// trigger write-out of new consistent index
	txn.End()
	s.Commit()
	s.Close()
	be.Close()
	return nil
}

func makeWALAndSnap(waldir, snapdir string, cl *membership.RaftCluster, restoreName string) error {
	if err := fileutil.CreateDirAll(waldir); err != nil {
		return err
	}

	// add members again to persist them to the store we create.
	st := store.New(etcdserver.StoreClusterPrefix, etcdserver.StoreKeysPrefix)
	cl.SetStore(st)
	for _, m := range cl.Members() {
		cl.AddMember(m)
	}

	m := cl.MemberByName(restoreName)
	md := &etcdserverpb.Metadata{NodeID: uint64(m.ID), ClusterID: uint64(cl.ID())}
	metadata, err := md.Marshal()
	if err != nil {
		return err
	}

	w, err := wal.Create(waldir, metadata)
	if err != nil {
		return err
	}
	defer w.Close()

	peers := make([]raft.Peer, len(cl.MemberIDs()))
	for i, id := range cl.MemberIDs() {
		ctx, err := json.Marshal((*cl).Member(id))
		if err != nil {
			return err
		}
		peers[i] = raft.Peer{ID: uint64(id), Context: ctx}
	}

	ents := make([]raftpb.Entry, len(peers))
	nodeIDs := make([]uint64, len(peers))
	for i, p := range peers {
		nodeIDs[i] = p.ID
		cc := raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  p.ID,
			Context: p.Context}
		d, err := cc.Marshal()
		if err != nil {
			return err
		}
		e := raftpb.Entry{
			Type:  raftpb.EntryConfChange,
			Term:  1,
			Index: uint64(i + 1),
			Data:  d,
		}
		ents[i] = e
	}

	commit, term := uint64(len(ents)), uint64(1)

	if err := w.Save(raftpb.HardState{
		Term:   term,
		Vote:   peers[0].ID,
		Commit: commit}, ents); err != nil {
		return err
	}

	b, err := st.Save()
	if err != nil {
		return err
	}

	raftSnap := raftpb.Snapshot{
		Data: b,
		Metadata: raftpb.SnapshotMetadata{
			Index: commit,
			Term:  term,
			ConfState: raftpb.ConfState{
				Nodes: nodeIDs,
			},
		},
	}
	snapshotter := snap.New(snapdir)
	if err := snapshotter.SaveSnap(raftSnap); err != nil {
		panic(err)
	}

	return w.SaveSnapshot(walpb.Snapshot{Index: commit, Term: term})
}

// startEmbeddedEtcd starts the embedded etcd server
func startEmbeddedEtcd(ro RestoreOptions) (*embed.Etcd, error) {
	cfg := embed.NewConfig()
	cfg.Dir = ro.RestoreDataDir
	e, err := embed.StartEtcd(cfg)
	if err != nil {
		return nil, err
	}
	select {
	case <-e.Server.ReadyNotify():
		fmt.Printf("Embedded server is ready!\n")
	case <-time.After(60 * time.Second):
		e.Server.Stop() // trigger a shutdown
		e.Close()
		return nil, fmt.Errorf("server took too long to start")
	}
	return e, nil
}

// applyDeltaSnapshots fetches the events from time sorted list of delta snapshots parallelly and applies them to etcd pseudo-sequentially
func (r *Restorer) applyDeltaSnapshots(client *clientv3.Client, snapList snapstore.SnapList) error {
	startTime := time.Now()
	r.logger.Infof("--- APPLY DELTA SNAPSHOTS")

	for _, snap := range snapList {
		r.logger.Infof("--- APPLY DELTA SNAPSHOTS - snap revision = %d, %d", snap.StartRevision, snap.LastRevision)
	}

	firstDeltaSnap := snapList[0]
	if err := r.applyFirstDeltaSnapshot(client, *firstDeltaSnap); err != nil {
		return err
	}
	if err := verifySnapshotRevision(client, snapList[0]); err != nil {
		return err
	}

	r.logger.Infof("--- APPLY DELTA SNAPSHOTS - first delta snapshot applied")

	errCh := make(chan error, 1)

	remainingSnaps := snapList[1:]                // stores the snaps, excluding the first one
	fetchedEventsInfo := make(chan eventsInfo, 1) // channel that passes fetched event data from snaps
	restoreFinished := make(chan bool, 1)         // channel used to indicate that restoration is finished

	r.logger.Infof("--- APPLY DELTA SNAPSHOTS - channels created")

	// snapLocations is an array to record pointers to temp file locations to which the events objects are stored as json strings.
	snapLocations := make([]string, len(remainingSnaps))

	go r.fetchListener(snapLocations, fetchedEventsInfo, errCh)
	r.logger.Infof("--- APPLY DELTA SNAPSHOTS - fetchListener goroutine started")
	go r.startFetchers(remainingSnaps, snapLocations, fetchedEventsInfo, errCh)
	r.logger.Infof("--- APPLY DELTA SNAPSHOTS - startFetchers goroutine started")
	go r.applier(client, remainingSnaps, snapLocations, restoreFinished, errCh)
	r.logger.Infof("--- APPLY DELTA SNAPSHOTS - applier goroutine started")

	select {
	case <-restoreFinished:
		r.logger.Infof("--- APPLY DELTA SNAPSHOTS - select restoreFinished")
		endTime := time.Now()
		restorationTime := endTime.Sub(startTime)
		r.logger.Warnf("RESTORATION TIME: %v", restorationTime)
		return nil
	case err := <-errCh:
		r.logger.Infof("--- APPLY DELTA SNAPSHOTS - select errCh")
		return err
	}
}

// coordinator coordinates the actions of the fetcher routines and the applier routine. It ensures that events are applied in the right order, regardless of the order in which snaps are fetched
func (r *Restorer) fetchListener(snapLocations []string, fetchedEventsInfo chan eventsInfo, errCh chan error) {
	r.logger.Infof("--- FETCHLISTENER")
	for {
		eventsInfo := <-fetchedEventsInfo
		r.logger.Infof("--- FETCHLISTENER - received on fetchedEventsInfo channel")

		tmpFile, err := ioutil.TempFile(tmpDir, tmpEventsDataFilePrefix)
		if err != nil {
			errCh <- fmt.Errorf("failed to create temp file for delta snapshot %s : %v", eventsInfo.Snap.SnapName, err)
			break
		}
		defer tmpFile.Close()

		filePath := tmpFile.Name()

		r.logger.Infof("--- FETCHLISTENER - created temp file at %s for delta snapshot %s", filePath, eventsInfo.Snap.SnapName)

		_, err = tmpFile.Write(eventsInfo.Data)
		if err != nil {
			errCh <- fmt.Errorf("failed to write events data into temp file for delta snapshot %s : %v", eventsInfo.Snap.SnapName, err)
			break
		}
		snapLocations[eventsInfo.SnapIndex] = filePath
		r.logger.Infof("--- FETCHLISTENER - filePath written to snapLocations[%d]", eventsInfo.SnapIndex)
	}
}

// startFetchers starts a number of fetcher goroutines pseudo-parallelly, with time delays between the goroutine starts
func (r *Restorer) startFetchers(remainingSnaps snapstore.SnapList, snapLocations []string, fetchedEventsInfo chan eventsInfo, errCh chan error) {
	r.logger.Infof("--- START FETCHERS")
	numSnaps := len(remainingSnaps)

	if numMaxFetchers < 1 {
		errCh <- fmt.Errorf("Invalid number of fetcher routines")
	}

	numFetchers := numSnaps
	if numFetchers < numMaxFetchers {
		numFetchers = numMaxFetchers
	}

	snapsPerFetcher, snapIndicesPerFetcher := assignSnapsToFetchers(numFetchers, remainingSnaps)

	for i := 0; i < numFetchers; i++ {
		go r.fetcher(snapsPerFetcher[i], snapIndicesPerFetcher[i], fetchedEventsInfo, errCh)
		time.Sleep(subsequentFetchDelay)
	}
}

// assignSnapsToFetchers assigns delta snapshots to fetchers
func assignSnapsToFetchers(numFetchers int, remainingSnaps snapstore.SnapList) ([][]snapstore.Snapshot, [][]int) {
	snapsPerFetcher := make([][]snapstore.Snapshot, numFetchers)
	snapIndicesPerFetcher := make([][]int, numFetchers)
	fetcherIndex := 0
	for i, snap := range remainingSnaps {
		snapsPerFetcher[fetcherIndex] = append(snapsPerFetcher[fetcherIndex], *snap)
		snapIndicesPerFetcher[fetcherIndex] = append(snapIndicesPerFetcher[fetcherIndex], i)
		fetcherIndex++
		if fetcherIndex == numFetchers {
			fetcherIndex = 0
		}
	}
	return snapsPerFetcher, snapIndicesPerFetcher
}

// fetcher fetches the delta snapshots assigned to it by startFetchers, and runs as a goroutine
func (r *Restorer) fetcher(snaps []snapstore.Snapshot, snapIndices []int, fetchedEventsInfo chan eventsInfo, errCh chan error) {
	for i, snap := range snaps {
		r.logger.Infof("Fetching delta snapshot %s", path.Join(snap.SnapDir, snap.SnapName))
		eventsData, err := getEventsDataFromDeltaSnapshot(r.store, snap)
		if err != nil {
			errCh <- fmt.Errorf("failed to read events data from delta snapshot %s : %v", snap.SnapName, err)
			break
		}
		r.logger.Infof("--- FETCHER - %s - fetched events data", snap.SnapName)
		eventsInfo := eventsInfo{
			Data:      eventsData,
			Snap:      snap,
			SnapIndex: snapIndices[i],
		}
		r.logger.Infof("--- FETCHER - %s - eventsInfo object created", snap.SnapName)
		fetchedEventsInfo <- eventsInfo
		r.logger.Infof("--- FETCHER - %s - fetchedEventsInfo data sent on channel", snap.SnapName)
	}
}

func (r *Restorer) applier(client *clientv3.Client, remainingSnaps snapstore.SnapList, snapLocations []string, restoreFinished chan bool, errCh chan error) {
	r.logger.Infof("--- APPLIER")

	currSnapIndex := 0
	for {
		if snapLocations[currSnapIndex] != "" {
			r.logger.Infof("--- APPLIER - snapshot %d found", currSnapIndex)
			filePath := snapLocations[currSnapIndex]
			currSnap := remainingSnaps[currSnapIndex]
			eventsData, err := ioutil.ReadFile(filePath)
			if err != nil {
				errCh <- fmt.Errorf("failed to read events data from file for delta snapshot %s : %v", currSnap.SnapName, err)
				break
			}
			defer os.Remove(filePath)

			events := []event{}
			err = json.Unmarshal(eventsData, &events)
			if err != nil {
				errCh <- fmt.Errorf("failed to read events from events data for delta snapshot %s : %v", currSnap.SnapName, err)
				break
			}

			if err := applyEventsToEtcd(client, events); err != nil {
				errCh <- fmt.Errorf("failed to apply events to etcd for delta snapshot %s : %v", currSnap.SnapName, err)
				break
			}
			r.logger.Infof("--- APPLIER - applied events to etcd")

			if err := verifySnapshotRevision(client, currSnap); err != nil {
				errCh <- fmt.Errorf("snapshot revision verification failed for delta snapshot %s : %v", currSnap.SnapName, err)
				break
			}
			r.logger.Infof("--- APPLIER - verified snapshot revision")

			currSnapIndex++
			r.logger.Infof("--- APPLIER - currSnapIndex incremented to %d", currSnapIndex)

			if currSnapIndex == len(remainingSnaps) {
				r.logger.Infof("--- APPLIER - currSnapIndex = len(remainingSnaps)")
				restoreFinished <- true
				r.logger.Infof("--- APPLIER - true sent on restoreFinished")
				break
			}
		} else {
			r.logger.Infof("--- APPLIER - waiting for snapIndex %d", currSnapIndex)
			time.Sleep(waitDuration)
		}
	}
}

// applyFirstDeltaSnapshot applies the events from first delta snapshot to etcd
func (r *Restorer) applyFirstDeltaSnapshot(client *clientv3.Client, snap snapstore.Snapshot) error {
	r.logger.Infof("Applying first delta snapshot %s", path.Join(snap.SnapDir, snap.SnapName))
	events, err := getEventsFromDeltaSnapshot(r.store, snap)
	if err != nil {
		return fmt.Errorf("failed to read events from delta snapshot %s : %v", snap.SnapName, err)
	}

	// Note: Since revision in full snapshot file name might be lower than actual revision stored in snapshot.
	// This is because of issue refereed below. So, as per workaround used in our logic of taking delta snapshot,
	// latest revision from full snapshot may overlap with first few revision on first delta snapshot
	// Hence, we have to additionally take care of that.
	// Refer: https://github.com/coreos/etcd/issues/9037
	ctx := context.TODO()
	resp, err := client.Get(ctx, "", clientv3.WithLastRev()...)
	if err != nil {
		return fmt.Errorf("failed to get etcd latest revision: %v", err)
	}
	lastRevision := resp.Header.Revision

	var newRevisionIndex int
	for index, event := range events {
		if event.EtcdEvent.Kv.ModRevision > lastRevision {
			newRevisionIndex = index
			break
		}
	}

	return applyEventsToEtcd(client, events[newRevisionIndex:])
}

// applyDeltaSnapshot applies the events from delta snapshot to etcd
func (r *Restorer) applyDeltaSnapshot(client *clientv3.Client, snap snapstore.Snapshot) error {
	r.logger.Infof("Applying delta snapshot %s", path.Join(snap.SnapDir, snap.SnapName))
	events, err := getEventsFromDeltaSnapshot(r.store, snap)
	if err != nil {
		return fmt.Errorf("failed to read events from delta snapshot %s : %v", snap.SnapName, err)
	}
	return applyEventsToEtcd(client, events)
}

// getEventsFromDeltaSnapshot decodes the events from snapshot file.
func getEventsFromDeltaSnapshot(store snapstore.SnapStore, snap snapstore.Snapshot) ([]event, error) {
	rc, err := store.Fetch(snap)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	buf := new(bytes.Buffer)
	bufSize, err := buf.ReadFrom(rc)
	if err != nil {
		return nil, err
	}
	sha := buf.Bytes()

	if bufSize <= sha256.Size {
		return nil, fmt.Errorf("delta snapshot is missing hash")
	}
	data := sha[:bufSize-sha256.Size]
	snapHash := sha[bufSize-sha256.Size:]

	// check for match
	h := sha256.New()
	if _, err := h.Write(data); err != nil {
		return nil, err
	}
	copmutedSha := h.Sum(nil)
	if !reflect.DeepEqual(snapHash, copmutedSha) {
		return nil, fmt.Errorf("expected sha256 %v, got %v", snapHash, copmutedSha)
	}
	events := []event{}
	if err := json.Unmarshal(data, &events); err != nil {
		return nil, err
	}
	return events, nil
}

// getEventsDataFromDeltaSnapshot fetches the data from snapshot file.
func getEventsDataFromDeltaSnapshot(store snapstore.SnapStore, snap snapstore.Snapshot) ([]byte, error) {
	rc, err := store.Fetch(snap)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	buf := new(bytes.Buffer)
	bufSize, err := buf.ReadFrom(rc)
	if err != nil {
		return nil, err
	}
	sha := buf.Bytes()

	if bufSize <= sha256.Size {
		return nil, fmt.Errorf("delta snapshot is missing hash")
	}
	data := sha[:bufSize-sha256.Size]
	snapHash := sha[bufSize-sha256.Size:]

	// check for match
	h := sha256.New()
	if _, err := h.Write(data); err != nil {
		return nil, err
	}
	copmutedSha := h.Sum(nil)
	if !reflect.DeepEqual(snapHash, copmutedSha) {
		return nil, fmt.Errorf("expected sha256 %v, got %v", snapHash, copmutedSha)
	}
	return data, nil
}

// applyEventsToEtcd performss operations in events sequentially
func applyEventsToEtcd(client *clientv3.Client, events []event) error {
	var (
		lastRev int64
		ops     = []clientv3.Op{}
		ctx     = context.TODO()
	)

	for _, e := range events {
		ev := e.EtcdEvent
		nextRev := ev.Kv.ModRevision
		if lastRev != 0 && nextRev > lastRev {
			if _, err := client.Txn(ctx).Then(ops...).Commit(); err != nil {
				return err
			}
			ops = []clientv3.Op{}
		}
		lastRev = nextRev
		switch ev.Type {
		case mvccpb.PUT:
			ops = append(ops, clientv3.OpPut(string(ev.Kv.Key), string(ev.Kv.Value))) //, clientv3.WithLease(clientv3.LeaseID(ev.Kv.Lease))))

		case mvccpb.DELETE:
			ops = append(ops, clientv3.OpDelete(string(ev.Kv.Key)))
		default:
			return fmt.Errorf("Unexpected event type")
		}
	}
	_, err := client.Txn(ctx).Then(ops...).Commit()
	return err
}

func verifySnapshotRevision(client *clientv3.Client, snap *snapstore.Snapshot) error {
	ctx := context.TODO()
	getResponse, err := client.Get(ctx, "foo")
	if err != nil {
		return fmt.Errorf("failed to connect to client: %v", err)
	}
	etcdRevision := getResponse.Header.GetRevision()
	if snap.LastRevision != etcdRevision {
		return fmt.Errorf("mismatched event revision while applying delta snapshot, expected %d but applied %d ", snap.LastRevision, etcdRevision)
	}
	return nil
}
