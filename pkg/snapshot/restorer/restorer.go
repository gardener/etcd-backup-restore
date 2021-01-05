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
	"sync"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/compressor"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/etcdserver"
	"go.etcd.io/etcd/etcdserver/api/membership"
	"go.etcd.io/etcd/etcdserver/api/snap"
	store "go.etcd.io/etcd/etcdserver/api/v2store"
	"go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/lease"
	"go.etcd.io/etcd/mvcc"
	"go.etcd.io/etcd/mvcc/backend"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.etcd.io/etcd/pkg/fileutil"
	"go.etcd.io/etcd/pkg/traceutil"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal"
	"go.etcd.io/etcd/wal/walpb"
	"go.uber.org/zap"
)

const (
	tmpDir                  = "/tmp"
	tmpEventsDataFilePrefix = "etcd-restore-"
)

// Restorer is a struct for etcd data directory restorer
type Restorer struct {
	logger    *logrus.Entry
	zapLogger *zap.Logger
	store     snapstore.SnapStore
}

// NewRestorer returns the restorer object.
func NewRestorer(store snapstore.SnapStore, logger *logrus.Entry) *Restorer {
	zapLogger, _ := zap.NewProduction()
	return &Restorer{
		logger:    logger.WithField("actor", "restorer"),
		zapLogger: zapLogger,
		store:     store,
	}
}

// Restore restore the etcd data directory as per specified restore options.
func (r *Restorer) Restore(ro brtypes.RestoreOptions) error {
	if err := r.RestoreFromBaseSnapshot(ro); err != nil {
		return fmt.Errorf("failed to restore from the base snapshot :%v", err)
	}
	if len(ro.DeltaSnapList) == 0 {
		r.logger.Infof("No delta snapshots present over base snapshot.")
		return nil
	}
	r.logger.Infof("Starting embedded etcd server...")
	e, err := miscellaneous.StartEmbeddedEtcd(r.logger, &ro)
	if err != nil {
		return err
	}
	defer func() {
		e.Server.Stop()
		e.Close()
	}()

	cfg := clientv3.Config{MaxCallSendMsgSize: ro.Config.MaxCallSendMsgSize, Endpoints: []string{e.Clients[0].Addr().String()}}
	client, err := clientv3.New(cfg)
	if err != nil {
		return err
	}
	defer client.Close()

	r.logger.Infof("Applying delta snapshots...")
	return r.ApplyDeltaSnapshots(client, ro)
}

// RestoreFromBaseSnapshot restore the etcd data directory from base snapshot.
func (r *Restorer) RestoreFromBaseSnapshot(ro brtypes.RestoreOptions) error {
	var err error
	if path.Join(ro.BaseSnapshot.SnapDir, ro.BaseSnapshot.SnapName) == "" {
		r.logger.Warnf("Base snapshot path not provided. Will do nothing.")
		return nil
	}
	r.logger.Infof("Restoring from base snapshot: %s", path.Join(ro.BaseSnapshot.SnapDir, ro.BaseSnapshot.SnapName))
	cfg := etcdserver.ServerConfig{
		InitialClusterToken: ro.Config.InitialClusterToken,
		InitialPeerURLsMap:  ro.ClusterURLs,
		PeerURLs:            ro.PeerURLs,
		Name:                ro.Config.Name,
	}
	if err := cfg.VerifyBootstrap(); err != nil {
		return err
	}

	cl, err := membership.NewClusterFromURLsMap(r.zapLogger, ro.Config.InitialClusterToken, ro.ClusterURLs)
	if err != nil {
		return err
	}

	memberDir := filepath.Join(ro.Config.RestoreDataDir, "member")
	if _, err := os.Stat(memberDir); err == nil {
		return fmt.Errorf("member directory in data directory(%q) exists", memberDir)
	}

	walDir := filepath.Join(memberDir, "wal")
	snapdir := filepath.Join(memberDir, "snap")
	if err = r.makeDB(snapdir, ro.BaseSnapshot, len(cl.Members()), false); err != nil {
		return err
	}
	return makeWALAndSnap(r.zapLogger, walDir, snapdir, cl, ro.Config.Name)
}

// makeDB copies the database snapshot to the snapshot directory.
func (r *Restorer) makeDB(snapdir string, snap *snapstore.Snapshot, commit int, skipHashCheck bool) error {
	rc, err := r.store.Fetch(*snap)
	if err != nil {
		return err
	}

	startTime := time.Now()
	isCompressed, compressionPolicy, err := compressor.IsSnapshotCompressed(snap.CompressionSuffix)
	if err != nil {
		return err
	}
	if isCompressed {
		// decompress the snapshot
		rc, err = compressor.DecompressSnapshot(rc, compressionPolicy)
		if err != nil {
			return fmt.Errorf("unable to decompress the snapshot: %v", err)
		}
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
	totalTime := time.Now().Sub(startTime).Seconds()

	if isCompressed {
		r.logger.Infof("successfully fetched data of base snapshot in %v seconds [CompressionPolicy:%v]", totalTime, compressionPolicy)
	} else {
		r.logger.Infof("successfully fetched data of base snapshot in %v seconds", totalTime)
	}

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
			return fmt.Errorf("failed to read sha from db %v", err)
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
	// a lessor that never times out leases
	lessor := lease.NewLessor(r.zapLogger, be, lease.LessorConfig{MinLeaseTTL: math.MaxInt64})
	s := mvcc.NewStore(r.zapLogger, be, lessor, (*brtypes.InitIndex)(&commit), mvcc.StoreConfig{})
	trace := traceutil.New("write", r.zapLogger)

	txn := s.Write(trace)
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

func makeWALAndSnap(logger *zap.Logger, waldir, snapdir string, cl *membership.RaftCluster, restoreName string) error {
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

	w, err := wal.Create(logger, waldir, metadata)
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
				Voters: nodeIDs,
			},
		},
	}
	snapshotter := snap.New(logger, snapdir)
	if err := snapshotter.SaveSnap(raftSnap); err != nil {
		panic(err)
	}

	return w.SaveSnapshot(walpb.Snapshot{Index: commit, Term: term})
}

// ApplyDeltaSnapshots fetches the events from delta snapshots in parallel and applies them to the embedded etcd sequentially.
func (r *Restorer) ApplyDeltaSnapshots(client *clientv3.Client, ro brtypes.RestoreOptions) error {
	snapList := ro.DeltaSnapList
	numMaxFetchers := ro.Config.MaxFetchers

	firstDeltaSnap := snapList[0]

	if err := r.applyFirstDeltaSnapshot(client, *firstDeltaSnap); err != nil {
		return err
	}
	if err := verifySnapshotRevision(client, snapList[0]); err != nil {
		return err
	}

	// no more delta snapshots available
	if len(snapList) == 1 {
		return nil
	}

	var (
		remainingSnaps  = snapList[1:]
		numSnaps        = len(remainingSnaps)
		numFetchers     = int(math.Min(float64(numMaxFetchers), float64(numSnaps)))
		snapLocationsCh = make(chan string, numSnaps)
		errCh           = make(chan error, numFetchers+1)
		fetcherInfoCh   = make(chan brtypes.FetcherInfo, numSnaps)
		applierInfoCh   = make(chan brtypes.ApplierInfo, numSnaps)
		stopCh          = make(chan bool)
		wg              sync.WaitGroup
	)

	go r.applySnaps(client, remainingSnaps, applierInfoCh, errCh, stopCh, &wg)

	for f := 0; f < numFetchers; f++ {
		go r.fetchSnaps(f, fetcherInfoCh, applierInfoCh, snapLocationsCh, errCh, stopCh, &wg)
	}

	for i, snap := range remainingSnaps {
		fetcherInfo := brtypes.FetcherInfo{
			Snapshot:  *snap,
			SnapIndex: i,
		}
		fetcherInfoCh <- fetcherInfo
	}
	close(fetcherInfoCh)

	err := <-errCh
	r.cleanup(snapLocationsCh, stopCh, &wg)
	if err == nil {
		r.logger.Infof("Restoration complete.")
	} else {
		r.logger.Errorf("Restoration failed.")
	}

	return err
}

// cleanup stops all running goroutines and removes the persisted snapshot files from disk.
func (r *Restorer) cleanup(snapLocationsCh chan string, stopCh chan bool, wg *sync.WaitGroup) {
	close(stopCh)

	wg.Wait()

	close(snapLocationsCh)

	for filePath := range snapLocationsCh {
		if _, err := os.Stat(filePath); err == nil && !os.IsNotExist(err) {
			if err = os.Remove(filePath); err != nil {
				r.logger.Warnf("Unable to remove file, file: %s, err: %v", filePath, err)
			}
		}
	}
	r.logger.Infof("Cleanup complete")
}

// fetchSnaps fetches delta snapshots as events and persists them onto disk.
func (r *Restorer) fetchSnaps(fetcherIndex int, fetcherInfoCh <-chan brtypes.FetcherInfo, applierInfoCh chan<- brtypes.ApplierInfo, snapLocationsCh chan<- string, errCh chan<- error, stopCh chan bool, wg *sync.WaitGroup) {
	defer wg.Done()
	wg.Add(1)

	for fetcherInfo := range fetcherInfoCh {
		select {
		case _, more := <-stopCh:
			if !more {
				return
			}
		default:
			r.logger.Infof("Fetcher #%d fetching delta snapshot %s", fetcherIndex+1, path.Join(fetcherInfo.Snapshot.SnapDir, fetcherInfo.Snapshot.SnapName))

			eventsData, err := r.getEventsDataFromDeltaSnapshot(fetcherInfo.Snapshot)
			if err != nil {
				errCh <- fmt.Errorf("failed to read events data from delta snapshot %s : %v", fetcherInfo.Snapshot.SnapName, err)
				applierInfoCh <- brtypes.ApplierInfo{SnapIndex: -1} // cannot use close(ch) as concurrent fetchSnaps routines might try to send on channel, causing a panic
				return
			}

			eventsFilePath, err := persistDeltaSnapshot(eventsData)
			if err != nil {
				errCh <- fmt.Errorf("failed to persist events data for delta snapshot %s : %v", fetcherInfo.Snapshot.SnapName, err)
				applierInfoCh <- brtypes.ApplierInfo{SnapIndex: -1}
				return
			}

			snapLocationsCh <- eventsFilePath // used for cleanup later

			applierInfo := brtypes.ApplierInfo{
				EventsFilePath: eventsFilePath,
				SnapIndex:      fetcherInfo.SnapIndex,
			}
			applierInfoCh <- applierInfo
		}
	}
}

// applySnaps applies delta snapshot events to the embedded etcd sequentially, in the right order of snapshots, regardless of the order in which they were fetched.
func (r *Restorer) applySnaps(client *clientv3.Client, remainingSnaps snapstore.SnapList, applierInfoCh <-chan brtypes.ApplierInfo, errCh chan<- error, stopCh <-chan bool, wg *sync.WaitGroup) {
	defer wg.Done()
	wg.Add(1)

	pathList := make([]string, len(remainingSnaps))
	nextSnapIndexToApply := 0

	for {
		select {
		case _, more := <-stopCh:
			if !more {
				return
			}
		case applierInfo := <-applierInfoCh:
			if applierInfo.SnapIndex == -1 {
				return
			}

			fetchedSnapIndex := applierInfo.SnapIndex
			pathList[fetchedSnapIndex] = applierInfo.EventsFilePath

			if fetchedSnapIndex < nextSnapIndexToApply {
				errCh <- fmt.Errorf("snap index mismatch for delta snapshot %d; expected snap index to be atleast %d", fetchedSnapIndex, nextSnapIndexToApply)
				return
			}
			if fetchedSnapIndex == nextSnapIndexToApply {
				for currSnapIndex := fetchedSnapIndex; currSnapIndex < len(remainingSnaps); currSnapIndex++ {
					if pathList[currSnapIndex] == "" {
						break
					}

					r.logger.Infof("Applying delta snapshot %s", path.Join(remainingSnaps[currSnapIndex].SnapDir, remainingSnaps[currSnapIndex].SnapName))

					filePath := pathList[currSnapIndex]
					snapName := remainingSnaps[currSnapIndex].SnapName

					eventsData, err := ioutil.ReadFile(filePath)
					if err != nil {
						errCh <- fmt.Errorf("failed to read events data from file for delta snapshot %s : %v", snapName, err)
						return
					}
					if err = os.Remove(filePath); err != nil {
						r.logger.Warnf("Unable to remove file: %s; err: %v", filePath, err)
					}
					events := []brtypes.Event{}
					if err = json.Unmarshal(eventsData, &events); err != nil {
						errCh <- fmt.Errorf("failed to read events from events data for delta snapshot %s : %v", snapName, err)
						return
					}

					if err := applyEventsAndVerify(client, events, remainingSnaps[currSnapIndex]); err != nil {
						errCh <- err
						return
					}
					nextSnapIndexToApply++
					if nextSnapIndexToApply == len(remainingSnaps) {
						errCh <- nil // restore finished
						return
					}
				}
			}
		}
	}
}

// applyEventsAndVerify applies events from one snapshot to the embedded etcd and verifies the correctness of the sequence of snapshot applied.
func applyEventsAndVerify(client *clientv3.Client, events []brtypes.Event, snap *snapstore.Snapshot) error {
	if err := applyEventsToEtcd(client, events); err != nil {
		return fmt.Errorf("failed to apply events to etcd for delta snapshot %s : %v", snap.SnapName, err)
	}

	if err := verifySnapshotRevision(client, snap); err != nil {
		return fmt.Errorf("snapshot revision verification failed for delta snapshot %s : %v", snap.SnapName, err)
	}
	return nil
}

// applyFirstDeltaSnapshot applies the events from first delta snapshot to etcd.
func (r *Restorer) applyFirstDeltaSnapshot(client *clientv3.Client, snap snapstore.Snapshot) error {
	r.logger.Infof("Applying first delta snapshot %s", path.Join(snap.SnapDir, snap.SnapName))
	events, err := r.getEventsFromDeltaSnapshot(snap)
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

// getEventsFromDeltaSnapshot returns the events from delta snapshot from snap store.
func (r *Restorer) getEventsFromDeltaSnapshot(snap snapstore.Snapshot) ([]brtypes.Event, error) {
	data, err := r.getEventsDataFromDeltaSnapshot(snap)
	if err != nil {
		return nil, err
	}

	events := []brtypes.Event{}
	if err := json.Unmarshal(data, &events); err != nil {
		return nil, err
	}

	return events, nil
}

// getEventsDataFromDeltaSnapshot fetches the events data from delta snapshot from snap store.
func (r *Restorer) getEventsDataFromDeltaSnapshot(snap snapstore.Snapshot) ([]byte, error) {
	rc, err := r.store.Fetch(snap)
	if err != nil {
		return nil, err
	}

	startTime := time.Now()
	isCompressed, compressionPolicy, err := compressor.IsSnapshotCompressed(snap.CompressionSuffix)
	if err != nil {
		return nil, err
	}
	if isCompressed {
		// decompress the snapshot
		rc, err = compressor.DecompressSnapshot(rc, compressionPolicy)
		if err != nil {
			return nil, fmt.Errorf("unable to decompress the snapshot: %v", err)
		}
	}
	defer rc.Close()

	buf := new(bytes.Buffer)
	bufSize, err := buf.ReadFrom(rc)
	if err != nil {
		return nil, err
	}
	totalTime := time.Now().Sub(startTime).Seconds()

	if isCompressed {
		r.logger.Infof("successfully fetched data of delta snapshot in %v seconds [CompressionPolicy:%v]", totalTime, compressionPolicy)
	} else {
		r.logger.Infof("successfully fetched data of delta snapshot in %v seconds", totalTime)
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

	computedSha := h.Sum(nil)
	if !reflect.DeepEqual(snapHash, computedSha) {
		return nil, fmt.Errorf("expected sha256 %v, got %v", snapHash, computedSha)
	}

	return data, nil
}

// persistDeltaSnapshot writes delta snapshot events to disk and returns the file path for the same.
func persistDeltaSnapshot(data []byte) (string, error) {
	tmpFile, err := ioutil.TempFile(tmpDir, tmpEventsDataFilePrefix)
	if err != nil {
		err = fmt.Errorf("failed to create temp file")
		return "", err
	}
	defer tmpFile.Close()

	if _, err = tmpFile.Write(data); err != nil {
		err = fmt.Errorf("failed to write events data into temp file")
		return "", err
	}

	return tmpFile.Name(), nil
}

// applyEventsToEtcd performs operations in events sequentially.
func applyEventsToEtcd(client *clientv3.Client, events []brtypes.Event) error {
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
