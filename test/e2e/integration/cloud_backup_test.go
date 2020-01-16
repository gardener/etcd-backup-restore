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

package integration_test

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/initializer/validator"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	. "github.com/gardener/etcd-backup-restore/test/e2e/integration"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
)

func startEtcd() (*Cmd, *chan error) {
	errChan := make(chan error)
	logger := logrus.New()
	etcdArgs := []string{"--name=etcd",
		"--advertise-client-urls=http://0.0.0.0:2379",
		"--listen-client-urls=http://0.0.0.0:2379",
		"--initial-cluster-state=new",
		"--initial-cluster-token=new",
		"--log-output=stdout",
		"--data-dir=" + os.Getenv("ETCD_DATA_DIR")}
	cmdEtcd := &Cmd{
		Task:    "etcd",
		Flags:   etcdArgs,
		Logger:  logger,
		Logfile: filepath.Join(os.Getenv("TEST_DIR"), "etcd.log"),
	}
	logger.Info("Starting etcd..")
	go func(errCh chan error) {
		err := cmdEtcd.RunCmdWithFlags()
		errCh <- err
	}(errChan)
	return cmdEtcd, &errChan
}

func startSnapshotter() (*Cmd, *chan error) {
	errChan := make(chan error)
	logger := logrus.New()
	etcdbrctlArgs := []string{
		"snapshot",
		"--max-backups=1",
		"--garbage-collection-policy=LimitBased",
		"--garbage-collection-period=30s",
		"--schedule=*/1 * * * *",
		"--storage-provider=S3",
		"--store-container=" + os.Getenv("TEST_ID"),
	}
	logger.Info(etcdbrctlArgs)
	cmdEtcdbrctl := &Cmd{
		Task:    "etcdbrctl",
		Flags:   etcdbrctlArgs,
		Logger:  logger,
		Logfile: filepath.Join(os.Getenv("TEST_DIR"), "etcdbrctl.log"),
	}
	go func(errCh chan error) {
		err := cmdEtcdbrctl.RunCmdWithFlags()
		errCh <- err
	}(errChan)

	return cmdEtcdbrctl, &errChan
}

func startBackupRestoreServer() (*Cmd, *chan error) {
	errChan := make(chan error)
	logger := logrus.New()
	etcdbrctlArgs := []string{
		"server",
		"--max-backups=1",
		"--data-dir=" + os.Getenv("ETCD_DATA_DIR"),
		"--insecure-transport=true",
		"--garbage-collection-policy=LimitBased",
		"--garbage-collection-period=30s",
		"--delta-snapshot-period=10s",
		"--schedule=*/1 * * * *",
		"--storage-provider=S3",
		"--store-container=" + os.Getenv("TEST_ID"),
	}
	logger.Info(etcdbrctlArgs)
	cmdEtcdbrctl := &Cmd{
		Task:    "etcdbrctl",
		Flags:   etcdbrctlArgs,
		Logger:  logger,
		Logfile: filepath.Join(os.Getenv("TEST_DIR"), "etcdbrctl.log"),
	}
	go func(errCh chan error) {
		err := cmdEtcdbrctl.RunCmdWithFlags()
		errCh <- err
	}(errChan)

	return cmdEtcdbrctl, &errChan
}

var _ = Describe("CloudBackup", func() {

	var snapstoreConfig *snapstore.Config
	var store snapstore.SnapStore
	var ctx = context.TODO()

	BeforeEach(func() {
		snapstoreConfig = &snapstore.Config{
			Provider:  "S3",
			Container: os.Getenv("TEST_ID"),
			Prefix:    path.Join("v1"),
		}
	})

	Describe("Regular backups", func() {
		var (
			cmdEtcd, cmdEtcdbrctl      *Cmd
			etcdErrChan, etcdbrErrChan *chan error
			err                        error
		)

		BeforeEach(func() {
			store, err = snapstore.GetSnapstore(snapstoreConfig)
			Expect(err).ShouldNot(HaveOccurred())

			snapList, err := store.List(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			for _, snap := range snapList {
				store.Delete(ctx, *snap)
			}

			cmdEtcd, etcdErrChan = startEtcd()
			cmdEtcdbrctl, etcdbrErrChan = startSnapshotter()
			go func() {
				select {
				case err := <-*etcdErrChan:
					Expect(err).ShouldNot(HaveOccurred())
				case err := <-*etcdbrErrChan:
					Expect(err).ShouldNot(HaveOccurred())
				}
			}()

		})

		Context("taken at 1 minute interval", func() {
			It("should take periodic backups.", func() {
				snaplist, err := store.List(ctx)
				Expect(snaplist).Should(BeEmpty())
				Expect(err).ShouldNot(HaveOccurred())

				time.Sleep(70 * time.Second)

				snaplist, err = store.List(ctx)
				Expect(snaplist).ShouldNot(BeEmpty())
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

		Context("taken at 1 minute interval", func() {
			It("should take periodic backups and limit based garbage collect backups over maxBackups configured", func() {
				snaplist, err := store.List(ctx)
				Expect(snaplist).Should(BeEmpty())
				Expect(err).ShouldNot(HaveOccurred())

				time.Sleep(190 * time.Second)

				snaplist, err = store.List(ctx)
				Expect(err).ShouldNot(HaveOccurred())
				count := 0
				for _, snap := range snaplist {
					if snap.Kind == snapstore.SnapshotKindFull {
						count++
					}
				}
				// We don't have control over whether for the lifecycle of test, whether GC ran after
				// last snapshot of not. But we are sure that if GC is working, there will be
				// max 2 snapshots otherwise 3 full snapshot will be there for 190 second period.
				Expect(count).Should(BeNumerically("<=", 2))
			})
		})

		AfterEach(func() {
			cmdEtcd.StopProcess()
			cmdEtcdbrctl.StopProcess()
			// To make sure etcd is down.
			time.Sleep(10 * time.Second)
		})
	})

	Describe("EtcdCorruptionCheck", func() {
		var cmd *Cmd
		logger := logrus.New()

		Context("checks a healthy data dir", func() {
			It("valids non corrupt data directory", func() {
				dataDir := os.Getenv("ETCD_DATA_DIR")
				Expect(dataDir).ShouldNot(Equal(nil))
				dataValidator := validator.DataValidator{
					Logger: logger,
					Config: &validator.Config{
						DataDir:         dataDir,
						SnapstoreConfig: snapstoreConfig,
					},
				}
				dataDirStatus, err := dataValidator.Validate(ctx, validator.Full, 0)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(dataDirStatus).Should(Equal(validator.DataDirStatus(validator.DataDirectoryValid)))
			})
		})

		Context("checks a corrupt snap dir", func() {

			BeforeEach(func() {
				dataDir := os.Getenv("ETCD_DATA_DIR")
				dbFilePath := filepath.Join(dataDir, "member", "snap", "db")
				dbFileBakPath := filepath.Join(dataDir, "member", "snap", "db.bak")
				err := os.Rename(dbFilePath, dbFileBakPath)
				Expect(err).ShouldNot(HaveOccurred())
			})
			It("raises data corruption on etcd data.", func() {
				dataDir := os.Getenv("ETCD_DATA_DIR")
				dbFilePath := filepath.Join(dataDir, "member", "snap", "db")
				logger.Infof("db file: %v", dbFilePath)
				file, err := os.Create(dbFilePath)
				defer file.Close()
				fileWriter := bufio.NewWriter(file)
				Expect(err).ShouldNot(HaveOccurred())
				fileWriter.Write([]byte("corrupt file.."))
				fileWriter.Flush()
				dataValidator := validator.DataValidator{
					Logger: logger,
					Config: &validator.Config{
						DataDir:         dataDir,
						SnapstoreConfig: snapstoreConfig,
					},
				}
				dataDirStatus, err := dataValidator.Validate(ctx, validator.Full, 0)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(dataDirStatus).Should(SatisfyAny(Equal(validator.DataDirStatus(validator.DataDirectoryCorrupt)), Equal(validator.DataDirStatus(validator.RevisionConsistencyError))))
			})

			AfterEach(func() {
				dataDir := os.Getenv("ETCD_DATA_DIR")
				dbFilePath := filepath.Join(dataDir, "member", "snap", "db")
				dbFileBakPath := filepath.Join(dataDir, "member", "snap", "db.bak")
				err := os.Rename(dbFileBakPath, dbFilePath)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

		Context("checks data dir", func() {
			BeforeEach(func() {
				dataDir := os.Getenv("ETCD_DATA_DIR")
				dbFilePath := filepath.Join(dataDir, "member", "snap", "db")
				dbFileBakPath := filepath.Join(dataDir, "member", "snap", "db.bak")
				err := os.Rename(dbFilePath, dbFileBakPath)
				Expect(err).ShouldNot(HaveOccurred())
			})

			It("restores corrupt data directory", func() {
				logger = logrus.New()
				dataDir := os.Getenv("ETCD_DATA_DIR")
				dbFilePath := filepath.Join(dataDir, "member", "snap", "db")
				file, err := os.Create(dbFilePath)
				defer file.Close()
				fileWriter := bufio.NewWriter(file)
				Expect(err).ShouldNot(HaveOccurred())
				fileWriter.WriteString("corrupt file..")
				fileWriter.Flush()
				logger.Info("Successfully corrupted db file.")
				etcdbrctlArgs := []string{
					"initialize",
					"--storage-provider=S3",
					"--store-container=" + os.Getenv("TEST_ID"),
					"--data-dir=" + dataDir,
				}
				cmd = &Cmd{
					Task:    "etcdbrctl",
					Flags:   etcdbrctlArgs,
					Logger:  logger,
					Logfile: filepath.Join(os.Getenv("TEST_DIR"), "etcdbrctl.log"),
				}
				err = cmd.RunCmdWithFlags()
				Expect(err).ShouldNot(HaveOccurred())
			})
		})
	})

	Describe("DataDirInitializeCheck", func() {
		var (
			cmdEtcd, cmdEtcdbrctl      *Cmd
			etcdErrChan, etcdbrErrChan *chan error
		)
		Context("checks if etcdbrctl re-initializes", func() {
			It("checks that there is no deadlock on", func() {
				logger := logrus.New()
				// Start etcdbrctl server.
				logger.Info("Starting etcd br server...")
				cmdEtcdbrctl, etcdbrErrChan = startBackupRestoreServer()
				go func() {
					err := <-*etcdbrErrChan
					Expect(err).ShouldNot(HaveOccurred())
				}()
				time.Sleep(5 * time.Second)
				// Run data directory initialize again.
				// Verify that deadlock is not occurring.
				logger.Info("Taking status of etcd br server...")
				status, err := getEtcdBrServerStatus()
				logger.Infof("Etcd br server in %v. %v", status, err)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(status).Should(Equal("New"))
				// Curl request to etcdbrctl server to initialize data directory.
				logger.Info("Initializing data directory...")
				_, err = initializeDataDir()
				logger.Info("Done initializing data directory.")
				Expect(err).ShouldNot(HaveOccurred())
				logger.Info("Taking status of etcd br server...")
				for status, err = getEtcdBrServerStatus(); status == "Progress"; status, err = getEtcdBrServerStatus() {
					logger.Infof("Etcdbr server status: %v", status)
					time.Sleep(1 * time.Second)
				}
				logger.Infof("Etcd br server in %v. %v", status, err)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(status).Should(Equal("Successful"))
				// Start etcd.
				cmdEtcd, etcdErrChan = startEtcd()
				go func() {
					err := <-*etcdErrChan
					Expect(err).ShouldNot(HaveOccurred())
				}()
				// Get status of etcdbrctl via cURL to make status New
				status, err = getEtcdBrServerStatus()
				Expect(status).Should(Equal("New"))
				Expect(err).ShouldNot(HaveOccurred())
				time.Sleep(10 * time.Second)
				// Stop etcd.
				cmdEtcd.StopProcess()
				time.Sleep(10 * time.Second)
				// Corrupt directory
				dataDir := os.Getenv("ETCD_DATA_DIR")
				dbFilePath := filepath.Join(dataDir, "member", "snap", "db")
				logger.Infof("db file: %v", dbFilePath)
				file, err := os.Create(dbFilePath)
				defer file.Close()
				fileWriter := bufio.NewWriter(file)
				Expect(err).ShouldNot(HaveOccurred())
				fileWriter.Write([]byte("corrupt file.."))
				fileWriter.Flush()
				status, err = getEtcdBrServerStatus()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(status).Should(Equal("New"))
				// Curl request to etcdbrctl server to initialize data directory.
				_, err = initializeDataDir()
				Expect(err).ShouldNot(HaveOccurred())
				for status, err = getEtcdBrServerStatus(); status == "Progress"; status, err = getEtcdBrServerStatus() {
					logger.Infof("Etcdbr server status: %v", status)
					time.Sleep(1 * time.Second)
				}
				Expect(err).ShouldNot(HaveOccurred())
				// Start etcd.
				cmdEtcd, etcdErrChan = startEtcd()
				go func() {
					err := <-*etcdErrChan
					Expect(err).ShouldNot(HaveOccurred())
				}()
				time.Sleep(10 * time.Second)
			})
			AfterEach(func() {
				cmdEtcd.StopProcess()
				cmdEtcdbrctl.StopProcess()
				// To make sure etcd is down.
				time.Sleep(10 * time.Second)
			})
		})
	})
})

func getEtcdBrServerStatus() (string, error) {
	url := "http://localhost:8080/initialization/status"
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	statusBuilder := &strings.Builder{}
	if _, err := io.Copy(statusBuilder, res.Body); err != nil {
		return "", err
	}
	return statusBuilder.String(), nil
}

func initializeDataDir() (int, error) {
	url := "http://localhost:8080/initialization/start"
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return -1, err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return -1, err
	}

	return res.StatusCode, err
}
