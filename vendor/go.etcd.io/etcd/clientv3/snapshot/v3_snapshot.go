// Copyright 2018 The etcd Authors
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

package snapshot

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/etcdserver"
	"go.etcd.io/etcd/etcdserver/api/membership"
	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/etcdserver/api/v2store"
	"go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/lease"
	"go.etcd.io/etcd/mvcc"
	"go.etcd.io/etcd/mvcc/backend"
	"go.etcd.io/etcd/pkg/fileutil"
	"go.etcd.io/etcd/pkg/traceutil"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal"
	"go.etcd.io/etcd/wal/walpb"
	"go.uber.org/zap"
)

// Manager defines snapshot methods.
type Manager interface {
	// Save fetches snapshot from remote etcd server and saves data
	// to target path. If the context "ctx" is canceled or timed out,
	// snapshot save stream will error out (e.g. context.Canceled,
	// context.DeadlineExceeded). Make sure to specify only one endpoint
	// in client configuration. Snapshot API must be requested to a
	// selected node, and saved snapshot is the point-in-time state of
	// the selected node.
	Save(ctx context.Context, cfg clientv3.Config, dbPath string) error

	// Status returns the snapshot file information.
	Status(dbPath string) (Status, error)

	// Restore restores a new etcd data directory from given snapshot
	// file. It returns an error if specified data directory already
	// exists, to prevent unintended data directory overwrites.
	Restore(cfg RestoreConfig) error
}

// NewV3 returns a new snapshot Manager for v3.x snapshot.
func NewV3(lg *zap.Logger) Manager {
	if lg == nil {
		lg = zap.NewExample()
	}
	return &v3Manager{lg: lg}
}

type v3Manager struct {
	lg *zap.Logger

	name    string
	dbPath  string
	walDir  string
	snapDir string
	cl      *membership.RaftCluster

	skipHashCheck bool
}

// hasChecksum returns "true" if the file size "n"
// has appended sha256 hash digest.
func hasChecksum(n int64) bool {
	// 512 is chosen because it's a minimum disk sector size
	// smaller than (and multiplies to) OS page size in most systems
	return (n % 512) == sha256.Size
}

// Save fetches snapshot from remote etcd server and saves data to target path.
func (s *v3Manager) Save(ctx context.Context, cfg clientv3.Config, dbPath string) error {
	if len(cfg.Endpoints) != 1 {
		return fmt.Errorf("snapshot must be requested to one selected node, not multiple %v", cfg.Endpoints)
	}
	cli, err := clientv3.New(cfg)
	if err != nil {
		return err
	}
	defer cli.Close()

	partpath := dbPath + ".part"
	defer os.RemoveAll(partpath)

	var f *os.File
	f, err = os.OpenFile(partpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, fileutil.PrivateFileMode)
	if err != nil {
		return fmt.Errorf("could not open %s (%v)", partpath, err)
	}
	s.lg.Info("created temporary db file", zap.String("path", partpath))

	now := time.Now()
	var rd io.ReadCloser
	rd, err = cli.Snapshot(ctx)
	if err != nil {
		return err
	}
	s.lg.Info("fetching snapshot", zap.String("endpoint", cfg.Endpoints[0]))
	var size int64
	size, err = io.Copy(f, rd)
	if err != nil {
		return err
	}
	if !hasChecksum(size) {
		return fmt.Errorf("sha256 checksum not found [bytes: %d]", size)
	}
	if err = fileutil.Fsync(f); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	s.lg.Info(
		"fetched snapshot",
		zap.String("endpoint", cfg.Endpoints[0]),
		zap.String("size", humanize.Bytes(uint64(size))),
		zap.Duration("took", time.Since(now)),
	)

	if err = os.Rename(partpath, dbPath); err != nil {
		return fmt.Errorf("could not rename %s to %s (%v)", partpath, dbPath, err)
	}
	s.lg.Info("saved", zap.String("path", dbPath))
	return nil
}

// Status is the snapshot file status.
type Status struct {
	Hash      uint32 `json:"hash"`
	Revision  int64  `json:"revision"`
	TotalKey  int    `json:"totalKey"`
	TotalSize int64  `json:"totalSize"`
}

// Status returns the snapshot file information.
func (s *v3Manager) Status(dbPath string) (ds Status, err error) {
	if _, err = os.Stat(dbPath); err != nil {
		return ds, err
	}

	db, err := bolt.Open(dbPath, 0400, &bolt.Options{ReadOnly: true})
	if err != nil {
		return ds, err
	}
	defer db.Close()

	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))

	if err = db.View(func(tx *bolt.Tx) error {
		// check snapshot file integrity first
		var dbErrStrings []string
		for dbErr := range tx.Check() {
			dbErrStrings = append(dbErrStrings, dbErr.Error())
		}
		if len(dbErrStrings) > 0 {
			return fmt.Errorf("snapshot file integrity check failed. %d errors found.\n"+strings.Join(dbErrStrings, "\n"), len(dbErrStrings))
		}
		ds.TotalSize = tx.Size()
		c := tx.Cursor()
		for next, _ := c.First(); next != nil; next, _ = c.Next() {
			b := tx.Bucket(next)
			if b == nil {
				return fmt.Errorf("cannot get hash of bucket %s", string(next))
			}
			h.Write(next)
			iskeyb := (string(next) == "key")
			b.ForEach(func(k, v []byte) error {
				h.Write(k)
				h.Write(v)
				if iskeyb {
					rev := bytesToRev(k)
					ds.Revision = rev.main
				}
				ds.TotalKey++
				return nil
			})
		}
		return nil
	}); err != nil {
		return ds, err
	}

	ds.Hash = h.Sum32()
	return ds, nil
}

// RestoreConfig configures snapshot restore operation.
type RestoreConfig struct {
	// SnapshotPath is the path of snapshot file to restore from.
	SnapshotPath string

	// Name is the human-readable name of this member.
	Name string

	// OutputDataDir is the target data directory to save restored data.
	// OutputDataDir should not conflict with existing etcd data directory.
	// If OutputDataDir already exists, it will return an error to prevent
	// unintended data directory overwrites.
	// If empty, defaults to "[Name].etcd" if not given.
	OutputDataDir string
	// OutputWALDir is the target WAL data directory.
	// If empty, defaults to "[OutputDataDir]/member/wal" if not given.
	OutputWALDir string

	// PeerURLs is a list of member's peer URLs to advertise to the rest of the cluster.
	PeerURLs []string

	// InitialCluster is the initial cluster configuration for restore bootstrap.
	InitialCluster string
	// InitialClusterToken is the initial cluster token for etcd cluster during restore bootstrap.
	InitialClusterToken string

	// SkipHashCheck is "true" to ignore snapshot integrity hash value
	// (required if copied from data directory).
	SkipHashCheck bool

	// RevisionBump is the amount to increase the latest revision after restore,
	// to allow administrators to trick clients into thinking that revision never decreased.
	// If 0, revision bumping is skipped.
	// (required if MarkCompacted == true)
	RevisionBump uint64

	// MarkCompacted is "true" to mark the latest revision as compacted.
	// (required if RevisionBump > 0)
	MarkCompacted bool
}

// Restore restores a new etcd data directory from given snapshot file.
func (s *v3Manager) Restore(cfg RestoreConfig) error {
	pURLs, err := types.NewURLs(cfg.PeerURLs)
	if err != nil {
		return err
	}
	var ics types.URLsMap
	ics, err = types.NewURLsMap(cfg.InitialCluster)
	if err != nil {
		return err
	}

	srv := etcdserver.ServerConfig{
		Logger:              s.lg,
		Name:                cfg.Name,
		PeerURLs:            pURLs,
		InitialPeerURLsMap:  ics,
		InitialClusterToken: cfg.InitialClusterToken,
	}
	if err = srv.VerifyBootstrap(); err != nil {
		return err
	}

	s.cl, err = membership.NewClusterFromURLsMap(s.lg, cfg.InitialClusterToken, ics, true)
	if err != nil {
		return err
	}

	dataDir := cfg.OutputDataDir
	if dataDir == "" {
		dataDir = cfg.Name + ".etcd"
	}
	if fileutil.Exist(dataDir) {
		return fmt.Errorf("data-dir %q exists", dataDir)
	}

	walDir := cfg.OutputWALDir
	if walDir == "" {
		walDir = filepath.Join(dataDir, "member", "wal")
	} else if fileutil.Exist(walDir) {
		return fmt.Errorf("wal-dir %q exists", walDir)
	}

	s.name = cfg.Name
	s.dbPath = cfg.SnapshotPath
	s.walDir = walDir
	s.snapDir = filepath.Join(dataDir, "member", "snap")
	s.skipHashCheck = cfg.SkipHashCheck

	s.lg.Info(
		"restoring snapshot",
		zap.String("path", s.dbPath),
		zap.String("wal-dir", s.walDir),
		zap.String("data-dir", dataDir),
		zap.String("snap-dir", s.snapDir),
	)
	if err = s.saveDB(); err != nil {
		return err
	}

	if cfg.MarkCompacted && cfg.RevisionBump > 0 {
		if err = s.modifyLatestRevision(cfg.RevisionBump); err != nil {
			return err
		}
	}

	if err = s.saveWALAndSnap(); err != nil {
		return err
	}
	s.lg.Info(
		"restored snapshot",
		zap.String("path", s.dbPath),
		zap.String("wal-dir", s.walDir),
		zap.String("data-dir", dataDir),
		zap.String("snap-dir", s.snapDir),
	)

	return nil
}

// saveDB copies the database snapshot to the snapshot directory
func (s *v3Manager) saveDB() error {
	f, ferr := os.OpenFile(s.dbPath, os.O_RDONLY, 0600)
	if ferr != nil {
		return ferr
	}
	defer f.Close()

	// get snapshot integrity hash
	if _, err := f.Seek(-sha256.Size, io.SeekEnd); err != nil {
		return err
	}
	sha := make([]byte, sha256.Size)
	if _, err := f.Read(sha); err != nil {
		return err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}

	if err := fileutil.CreateDirAll(s.snapDir); err != nil {
		return err
	}

	dbpath := filepath.Join(s.snapDir, "db")
	db, dberr := os.OpenFile(dbpath, os.O_RDWR|os.O_CREATE, 0600)
	if dberr != nil {
		return dberr
	}
	if _, err := io.Copy(db, f); err != nil {
		return err
	}

	// truncate away integrity hash, if any.
	off, serr := db.Seek(0, io.SeekEnd)
	if serr != nil {
		return serr
	}
	hasHash := hasChecksum(off)
	if hasHash {
		if err := db.Truncate(off - sha256.Size); err != nil {
			return err
		}
	}

	if !hasHash && !s.skipHashCheck {
		return fmt.Errorf("snapshot missing hash but --skip-hash-check=false")
	}

	if hasHash && !s.skipHashCheck {
		// check for match
		if _, err := db.Seek(0, io.SeekStart); err != nil {
			return err
		}
		h := sha256.New()
		if _, err := io.Copy(h, db); err != nil {
			return err
		}
		dbsha := h.Sum(nil)
		if !reflect.DeepEqual(sha, dbsha) {
			return fmt.Errorf("expected sha256 %v, got %v", sha, dbsha)
		}
	}

	// db hash is OK, can now modify DB so it can be part of a new cluster
	db.Close()

	commit := len(s.cl.Members())

	// update consistentIndex so applies go through on etcdserver despite
	// having a new raft instance
	be := backend.NewDefaultBackend(dbpath)

	// a lessor never timeouts leases
	lessor := lease.NewLessor(s.lg, be, nil, lease.LessorConfig{MinLeaseTTL: math.MaxInt64})

	mvs := mvcc.NewStore(s.lg, be, lessor, (*initIndex)(&commit), mvcc.StoreConfig{CompactionBatchLimit: math.MaxInt32})
	txn := mvs.Write(traceutil.TODO())
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

	mvs.Commit()
	mvs.Close()
	be.Close()

	return nil
}

// modifyLatestRevision can increase the latest revision by the given amount and sets the scheduled compaction
// to that revision so that the server will consider this revision compacted.
func (s *v3Manager) modifyLatestRevision(bumpAmount uint64) error {
	dbpath := filepath.Join(s.snapDir, "db")
	be := backend.NewDefaultBackend(dbpath)
	defer func() {
		be.ForceCommit()
		be.Close()
	}()

	tx := be.BatchTx()
	tx.Lock()
	defer tx.Unlock()

	latest, err := s.unsafeGetLatestRevision(tx)
	if err != nil {
		return err
	}

	latest = s.unsafeBumpRevision(tx, latest, int64(bumpAmount))
	s.unsafeMarkRevisionCompacted(tx, latest)

	return nil
}

func (s *v3Manager) unsafeBumpRevision(tx backend.BatchTx, latest revision, amount int64) revision {
	s.lg.Info(
		"bumping latest revision",
		zap.Int64("latest-revision", latest.main),
		zap.Int64("bump-amount", amount),
		zap.Int64("new-latest-revision", latest.main+amount),
	)

	latest.main += amount
	latest.sub = 0
	k := make([]byte, 17)
	revToBytes(k, latest)
	tx.UnsafePut([]byte("key"), k, []byte{})

	return latest
}

func (s *v3Manager) unsafeMarkRevisionCompacted(tx backend.BatchTx, latest revision) {
	s.lg.Info(
		"marking revision compacted",
		zap.Int64("revision", latest.main),
	)

	mvcc.UnsafeSetScheduledCompact(tx, latest.main)
}

func (s *v3Manager) unsafeGetLatestRevision(tx backend.BatchTx) (revision, error) {
	var latest revision
	err := tx.UnsafeForEach([]byte("key"), func(k, _ []byte) (err error) {
		rev := bytesToRev(k)

		if rev.GreaterThan(latest) {
			latest = rev
		}

		return nil
	})
	return latest, err
}

// saveWALAndSnap creates a WAL for the initial cluster
func (s *v3Manager) saveWALAndSnap() error {
	if err := fileutil.CreateDirAll(s.walDir); err != nil {
		return err
	}

	// add members again to persist them to the store we create.
	st := v2store.New(etcdserver.StoreClusterPrefix, etcdserver.StoreKeysPrefix)
	s.cl.SetStore(st)
	for _, m := range s.cl.Members() {
		s.cl.AddMember(m)
	}

	m := s.cl.MemberByName(s.name)
	md := &etcdserverpb.Metadata{NodeID: uint64(m.ID), ClusterID: uint64(s.cl.ID())}
	metadata, merr := md.Marshal()
	if merr != nil {
		return merr
	}
	w, walerr := wal.Create(s.lg, s.walDir, metadata)
	if walerr != nil {
		return walerr
	}
	defer w.Close()

	peers := make([]raft.Peer, len(s.cl.MemberIDs()))
	for i, id := range s.cl.MemberIDs() {
		ctx, err := json.Marshal((*s.cl).Member(id))
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
			Context: p.Context,
		}
		d, err := cc.Marshal()
		if err != nil {
			return err
		}
		ents[i] = raftpb.Entry{
			Type:  raftpb.EntryConfChange,
			Term:  1,
			Index: uint64(i + 1),
			Data:  d,
		}
	}

	commit, term := uint64(len(ents)), uint64(1)
	if err := w.Save(raftpb.HardState{
		Term:   term,
		Vote:   peers[0].ID,
		Commit: commit,
	}, ents); err != nil {
		return err
	}

	b, berr := st.Save()
	if berr != nil {
		return berr
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
	sn := snap.New(s.lg, s.snapDir)
	if err := sn.SaveSnap(raftSnap); err != nil {
		return err
	}
	return w.SaveSnapshot(walpb.Snapshot{Index: commit, Term: term})
}
