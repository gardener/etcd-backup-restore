package validator

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/coreos/etcd/embed"
)

const (
	DATA_DIRECTORY_EMPTY = iota
	DATA_DIRECTORY_VALID
	DATA_DIRECTORY_CORRUPT
	DATA_DIRECTORY_ERROR
)

type DataDirStatus int

var logger *logrus.Logger

func init() {
	logger = logrus.New()
}

//Validate performs the steps required to validate data for Etcd instance.
// The steps involed are:
//   * Check if data directory exists.
//     - If data directory exists
//       * Check for data corruption.
//			- If data directory is in corrupted state, clear the data directory. Error out.
//     - If data directory does not exist.
//		 * Return nil
func (d *DataValidator) Validate() (DataDirStatus, error) {
	dataDir := d.Config.DataDir
	isEmpty, err := isDirEmpty(dataDir)
	if err != nil {
		return DATA_DIRECTORY_ERROR, err
	}
	if !isEmpty {
		err := d.CheckForDataCorruption()
		if err != nil {
			d.Logger.Error("Data directory corrupt.")
			return DATA_DIRECTORY_CORRUPT, err
		}
		d.Logger.Info("Data directory valid.")
		return DATA_DIRECTORY_VALID, nil
	}
	d.Logger.Info("Data directory empty.")
	return DATA_DIRECTORY_EMPTY, nil
}

func (d *DataValidator) CheckForDataCorruption() error {
	etcd, err := startEmbeddedEtcd(d.Config.DataDir)
	if err != nil {
		logger.Error("Corruption check error:", err)
		return err
	}
	logger.Info("Embedded server is ready!")
	defer etcd.Close()
	return nil
}

func startEmbeddedEtcd(dataDir string) (e *embed.Etcd, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("etcd server crashed while starting")
		}
	}()
	cfg := embed.NewConfig()
	cfg.Dir = dataDir
	e, err = embed.StartEtcd(cfg)

	if err != nil {
		logger.Error("Corruption check error: %v", err)
		return e, err
	}
	select {
	case <-e.Server.ReadyNotify():
		logger.Info("Embedded server is ready!")
	case <-time.After(60 * time.Second):
		e.Server.Stop() // trigger a shutdown
		e.Close()
		return nil, fmt.Errorf("server took too long to start")
	}
	return e, err
}

func isDirEmpty(dataDir string) (bool, error) {
	f, err := os.Open(dataDir)
	if err != nil {
		return false, err
	}
	defer f.Close()

	_, err = f.Readdirnames(1) // Or f.Readdir(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err // Either not empty or error, suits both cases
}

/*

var (
	// A map of valid files that can be present in the snap folder.
	validFiles = map[string]bool{
		"db": true,
	}
	crcTable = crc32.MakeTable(crc32.Castagnoli)
)

const (
	snapSuffix = ".snap"
)

type consistentIndex uint64

func (d *DataValidator) MemberDir() string { return filepath.Join(d.Config.DataDir, "member") }

func (d *DataValidator) WALDir() string { return filepath.Join(d.MemberDir(), "wal") }

func (d *DataValidator) SnapDir() string { return filepath.Join(d.MemberDir(), "snap") }

func (d *DataValidator) BackendPath() string { return filepath.Join(d.SnapDir(), "db") }

func (d *DataValidator) checkForDataCorruption() error {
	var walsnap walpb.Snapshot
	fmt.Println("Verifying snap directory...")
	snapshot, err := d.verifySnapDir()
	if err != nil && err != snap.ErrNoSnapshot {
		return err
	}

	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	fmt.Println("Verifying WAL directory...")
	if err := verifyWALDir(d.WALDir(), walsnap); err != nil {
		return err
	}

	return nil
}

func (d *DataValidator) NewBackend() backend.Backend {
	bcfg := backend.DefaultBackendConfig()
	bcfg.Path = d.BackendPath()
	return backend.New(bcfg)
}

func (d *DataValidator) verifySnapDir() (*raftpb.Snapshot, error) {
	names, err := d.snapNames()
	if err != nil {
		return nil, err
	}

	var snapshot *raftpb.Snapshot
	for _, name := range names {
		if snapshot, err = verifySnap(d.SnapDir(), name); err == nil {
			break
		}
	}
	if err != nil {
		return nil, snap.ErrNoSnapshot
	}
	return snapshot, nil
}

func verifySnap(dir, name string) (*raftpb.Snapshot, error) {
	fpath := filepath.Join(dir, name)
	snap, err := read(fpath)
	return snap, err
}

// Read reads the snapshot named by snapname and returns the snapshot.
func read(snapname string) (*raftpb.Snapshot, error) {
	fmt.Printf("Verifying Snapfile %s.\n", snapname)
	b, err := ioutil.ReadFile(snapname)
	if err != nil {
		fmt.Printf("Cannot read file %v: %v.\n", snapname, err)
		return nil, err
	}

	if len(b) == 0 {
		fmt.Printf("Unexpected empty snapshot.\n")
		return nil, snap.ErrEmptySnapshot
	}

	var serializedSnap snappb.Snapshot
	if err = serializedSnap.Unmarshal(b); err != nil {
		fmt.Printf("Corrupted snapshot file %v: %v.\n", snapname, err)
		return nil, err
	}

	if len(serializedSnap.Data) == 0 || serializedSnap.Crc == 0 {
		fmt.Printf("Unexpected empty snapshot.\n")
		return nil, snap.ErrEmptySnapshot
	}

	crc := crc32.Update(0, crcTable, serializedSnap.Data)
	if crc != serializedSnap.Crc {
		fmt.Printf("Corrupted snapshot file %v: crc mismatch.\n", snapname)
		return nil, snap.ErrCRCMismatch
	}

	var snap raftpb.Snapshot
	if err = snap.Unmarshal(serializedSnap.Data); err != nil {
		fmt.Printf("corrupted snapshot file %v: %v", snapname, err)
		return nil, err
	}
	return &snap, nil
}

func (d *DataValidator) snapNames() ([]string, error) {
	dir, err := os.Open(d.SnapDir())
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	snaps := checkSuffix(names)
	if len(snaps) == 0 {
		return nil, snap.ErrNoSnapshot
	}
	sort.Sort(sort.Reverse(sort.StringSlice(snaps)))
	return snaps, nil
}

func checkSuffix(names []string) []string {
	snaps := []string{}
	for i := range names {
		if strings.HasSuffix(names[i], snapSuffix) {
			snaps = append(snaps, names[i])
		} else {
			// If we find a file which is not a snapshot then check if it's
			// a vaild file. If not throw out a warning.
			if _, ok := validFiles[names[i]]; !ok {
				fmt.Printf("skipped unexpected non snapshot file %v", names[i])
			}
		}
	}
	return snaps
}



func verifyWALDir(waldir string, snap walpb.Snapshot) error {
	var (
		err error
		w   *wal.WAL
	)

	repaired := false
	for {
		if w, err = wal.Open(waldir, snap); err != nil {
			fmt.Printf("open wal error: %v", err)
		}
		if _, _, _, err = w.ReadAll(); err != nil {
			w.Close()
			// we can only repair ErrUnexpectedEOF and we never repair twice.
			if repaired || err != io.ErrUnexpectedEOF {
				fmt.Printf("read wal error (%v) and cannot be repaired.\n", err)
				return err
			}
			if !wal.Repair(waldir) {
				fmt.Printf("WAL error (%v) cannot be repaired.\n", err)
				return err
			} else {
				fmt.Printf("repaired WAL error (%v).\n", err)
				repaired = true
			}
			continue
		}
		break
	}
	return err
}

*/
