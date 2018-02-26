package validator

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/coreos/etcd/embed"
)

const (
	DataDirectoryValid = iota
	DataDirectoryEmpty
	DataDirectoryNotExist
	DataDirectoryCorrupt
	DataDirectoryError
)

type DataDirStatus int

var logger *logrus.Logger

func init() {
	logger = logrus.New()
}

func (d *DataValidator) MemberDir() string { return filepath.Join(d.Config.DataDir, "member") }

func (d *DataValidator) WALDir() string { return filepath.Join(d.MemberDir(), "wal") }

func (d *DataValidator) SnapDir() string { return filepath.Join(d.MemberDir(), "snap") }

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
	dirExists, err := directoryExist(dataDir)
	if err != nil {
		return DataDirectoryError, err
	} else if !dirExists {
		d.Logger.Error("Data directory corrupt.")
		return DataDirectoryCorrupt, err
	}
	isEmpty, err := isDirEmpty(dataDir)
	if err != nil {
		return DataDirectoryError, err
	}
	etcdDirStructValid, err := d.hasEtcdDirectoryStructure()
	if err != nil {
		return DataDirectoryError, err
	}
	if !etcdDirStructValid {
		d.Logger.Error("Data directory corrupt.")
		return DataDirectoryCorrupt, err
	}
	if !isEmpty {
		err := d.checkForDataCorruption()
		if err != nil {
			d.Logger.Error("Data directory corrupt.")
			return DataDirectoryCorrupt, err
		}
		d.Logger.Info("Data directory valid.")
		return DataDirectoryValid, nil
	}
	d.Logger.Info("Data directory empty.")
	return DataDirectoryEmpty, nil
}

func (d *DataValidator) hasEtcdDirectoryStructure() (bool, error) {
	var memberExist, snapExist, walExist bool
	var err error
	if memberExist, err = directoryExist(d.MemberDir()); err != nil {
		return false, err
	}
	if snapExist, err = directoryExist(d.SnapDir()); err != nil {
		return false, err
	}
	if walExist, err = directoryExist(d.WALDir()); err != nil {
		return false, err
	}
	return memberExist && snapExist && walExist, nil
}

func (d *DataValidator) checkForDataCorruption() error {
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
		logger.Errorf("Corruption check error: %v", err)
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

func directoryExist(dir string) (bool, error) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	} else {
		return true, nil
	}
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
