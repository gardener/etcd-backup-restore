// Copyright © 2018 The Gardener Authors.
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
	// DataDirectoryValid indicates data directory is valid.
	DataDirectoryValid = iota
	// DataDirectoryEmpty indicates data directory is empty.
	DataDirectoryEmpty
	// DataDirectoryNotExist indicates data directory is non-existant.
	DataDirectoryNotExist
	// DataDirectoryInvStruct indicates data directory has invalid structure.
	DataDirectoryInvStruct
	// DataDirectoryCorrupt indicates data directory is corrupt.
	DataDirectoryCorrupt
	// DataDirectoryError indicates unknown error while validation.
	DataDirectoryError
)

// DataDirStatus represents the status of the etcd data directory.
type DataDirStatus int

var logger *logrus.Logger

func init() {
	logger = logrus.New()
}

func (d *DataValidator) memberDir() string { return filepath.Join(d.Config.DataDir, "member") }

func (d *DataValidator) walDir() string { return filepath.Join(d.memberDir(), "wal") }

func (d *DataValidator) snapDir() string { return filepath.Join(d.memberDir(), "snap") }

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
		err = fmt.Errorf("Directory does not exist. %v", err)
		return DataDirectoryCorrupt, err
	}
	isEmpty, err := isDirEmpty(dataDir)
	if err != nil {
		return DataDirectoryError, err
	}
	etcdDirStructValid, err := d.hasEtcdDirectoryStructure()
	if err != nil {
		return DataDirectoryInvStruct, err
	}
	if !etcdDirStructValid {
		err = fmt.Errorf("Data directory structure invalid. %v", err)
		return DataDirectoryCorrupt, err
	}
	if !isEmpty {
		err := d.checkForDataCorruption()
		if err != nil {
			err = fmt.Errorf("Data directory corrupt. %v", err)
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
	if memberExist, err = directoryExist(d.memberDir()); err != nil {
		return false, err
	}
	if snapExist, err = directoryExist(d.snapDir()); err != nil {
		return false, err
	}
	if walExist, err = directoryExist(d.walDir()); err != nil {
		return false, err
	}
	return memberExist && snapExist && walExist, nil
}

func (d *DataValidator) checkForDataCorruption() error {
	etcd, err := startEmbeddedEtcd(d.Config.DataDir)
	if err != nil {
		err = fmt.Errorf("Corruption check error: %v", err)
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
		err = fmt.Errorf("Corruption check error: %v", err)
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
