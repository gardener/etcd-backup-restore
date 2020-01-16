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
package validator_test

import (
	"fmt"
	"math"
	"os"
	"path"

	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	"github.com/gardener/etcd-backup-restore/test/utils"

	. "github.com/gardener/etcd-backup-restore/pkg/initializer/validator"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Running Datavalidator", func() {
	var (
		restoreDataDir     string
		snapstoreBackupDir string
		snapstoreConfig    *snapstore.Config
		validator          *DataValidator
	)

	BeforeEach(func() {
		restoreDataDir = path.Clean(etcdDir)
		snapstoreBackupDir = path.Clean(snapstoreDir)

		snapstoreConfig = &snapstore.Config{
			Container: snapstoreBackupDir,
			Provider:  "Local",
		}

		validator = &DataValidator{
			Config: &Config{
				DataDir:         restoreDataDir,
				SnapstoreConfig: snapstoreConfig,
			},
			Logger: logger.Logger,
		}
	})

	Context("with missing data directory", func() {
		It("should return DataDirStatus as DataDirectoryNotExist, and non-nil error", func() {
			tempDir := fmt.Sprintf("%s.%s", restoreDataDir, "temp")
			err = os.Rename(restoreDataDir, tempDir)
			Expect(err).ShouldNot(HaveOccurred())
			dataDirStatus, err := validator.Validate(testCtx, Full, 0)
			Expect(err).Should(HaveOccurred())
			Expect(int(dataDirStatus)).Should(Equal(DataDirectoryNotExist))
			err = os.Rename(tempDir, restoreDataDir)
			Expect(err).ShouldNot(HaveOccurred())
		})
	})

	Context("with missing member directory", func() {
		It("should return DataDirStatus as DataDirectoryInvStruct, and nil error", func() {
			memberDir := path.Join(restoreDataDir, "member")
			tempDir := fmt.Sprintf("%s.%s", memberDir, "temp")
			err = os.Rename(memberDir, tempDir)
			Expect(err).ShouldNot(HaveOccurred())
			dataDirStatus, err := validator.Validate(testCtx, Full, 0)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(int(dataDirStatus)).Should(Equal(DataDirectoryInvStruct))
			err = os.Rename(tempDir, memberDir)
			Expect(err).ShouldNot(HaveOccurred())
		})
	})

	Context("with missing snap directory", func() {
		It("should return DataDirStatus as DataDirectoryInvStruct , and nil error", func() {
			snapDir := path.Join(restoreDataDir, "member", "snap")
			tempDir := fmt.Sprintf("%s.%s", snapDir, "temp")
			err = os.Rename(snapDir, tempDir)
			Expect(err).ShouldNot(HaveOccurred())
			dataDirStatus, err := validator.Validate(testCtx, Full, 0)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(int(dataDirStatus)).Should(Equal(DataDirectoryInvStruct))
			err = os.Rename(tempDir, snapDir)
			Expect(err).ShouldNot(HaveOccurred())
		})
	})

	Context("with missing wal directory", func() {
		It("should return DataDirStatus as DataDirectoryInvStruct or DataDirectoryStatusUnknown, and nil error", func() {
			walDir := path.Join(restoreDataDir, "member", "wal")
			tempDir := fmt.Sprintf("%s.%s", walDir, "temp")
			err = os.Rename(walDir, tempDir)
			Expect(err).ShouldNot(HaveOccurred())
			dataDirStatus, err := validator.Validate(testCtx, Full, 0)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(int(dataDirStatus)).Should(SatisfyAny(Equal(DataDirectoryInvStruct), Equal(DataDirectoryStatusUnknown)))
			err = os.Rename(tempDir, walDir)
			Expect(err).ShouldNot(HaveOccurred())
		})
	})

	Context("with empty wal directory and data validation in sanity mode", func() {
		It("should return DataDirStatus as DataDirectoryValid, and nil error", func() {
			walDir := path.Join(restoreDataDir, "member", "wal")
			tempWalDir := fmt.Sprintf("%s.%s", walDir, "temp")
			err = os.Rename(walDir, tempWalDir)
			Expect(err).ShouldNot(HaveOccurred())
			err = os.Mkdir(walDir, 0700)
			Expect(err).ShouldNot(HaveOccurred())
			dataDirStatus, err := validator.Validate(testCtx, Sanity, 0)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(int(dataDirStatus)).Should(Equal(DataDirectoryValid))
			err = os.RemoveAll(walDir)
			Expect(err).ShouldNot(HaveOccurred())
			err = os.Rename(tempWalDir, walDir)
			Expect(err).ShouldNot(HaveOccurred())
		})
	})

	Context("with corrupt db file", func() {
		It("should return DataDirStatus as DataDirectoryCorrupt, and nil error", func() {
			dbFile := path.Join(restoreDataDir, "member", "snap", "db")
			_, err = os.Stat(dbFile)
			Expect(err).ShouldNot(HaveOccurred())

			tempFile := path.Join(outputDir, "temp", "db")
			err = copyFile(dbFile, tempFile)
			Expect(err).ShouldNot(HaveOccurred())

			file, err := os.OpenFile(
				dbFile,
				os.O_WRONLY|os.O_TRUNC|os.O_CREATE,
				0666,
			)
			Expect(err).ShouldNot(HaveOccurred())
			defer file.Close()

			// corrupt the db file by writing random data to it
			byteSlice := []byte("Random data!\n")
			_, err = file.Write(byteSlice)
			Expect(err).ShouldNot(HaveOccurred())

			dataDirStatus, err := validator.Validate(testCtx, Full, 0)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(int(dataDirStatus)).Should(Equal(DataDirectoryCorrupt))

			err = os.Remove(dbFile)
			Expect(err).ShouldNot(HaveOccurred())

			err = os.Rename(tempFile, dbFile)
			Expect(err).ShouldNot(HaveOccurred())
		})
	})

	Context("with combination of valid,corrupt,invalid name and empty snap file", func() {
		It("should return DataDirStatus as DataDirectoryValid , and nil error", func() {
			snapDir := path.Join(restoreDataDir, "member", "snap")
			emptySnap := path.Join(snapDir, "empty.snap")
			corruptSnap := path.Join(snapDir, "corrupt.snap")
			withoutSnapSuffix := path.Join(snapDir, "corrupt")
			Expect(err).ShouldNot(HaveOccurred())
			file, err := os.Create(emptySnap)
			Expect(err).ShouldNot(HaveOccurred())
			defer file.Close()
			createCorruptSnap(corruptSnap)
			createCorruptSnap(withoutSnapSuffix)
			defer func() {
				err = os.Remove(emptySnap + ".broken")
				Expect(err).ShouldNot(HaveOccurred())
				err = os.Remove(corruptSnap + ".broken")
				Expect(err).ShouldNot(HaveOccurred())
				err = os.Remove(withoutSnapSuffix)
				Expect(err).ShouldNot(HaveOccurred())
			}()

			dataDirStatus, err := validator.Validate(testCtx, Full, 0)

			Expect(err).ShouldNot(HaveOccurred())
			Expect(int(dataDirStatus)).Should(Equal(DataDirectoryValid))

		})
	})

	Context("with inconsistent revision numbers between etcd and latest snapshot", func() {
		It("should return DataDirStatus as RevisionConsistencyError, and nil error", func() {
			tempDir := fmt.Sprintf("%s.%s", restoreDataDir, "temp")
			err = os.Rename(restoreDataDir, tempDir)
			Expect(err).ShouldNot(HaveOccurred())
			defer func() {
				err = os.RemoveAll(restoreDataDir)
				Expect(err).ShouldNot(HaveOccurred())
				err = os.Rename(tempDir, restoreDataDir)
				Expect(err).ShouldNot(HaveOccurred())
			}()

			// start etcd
			etcd, err := utils.StartEmbeddedEtcd(testCtx, restoreDataDir, logger)
			Expect(err).ShouldNot(HaveOccurred())
			endpoints := []string{etcd.Clients[0].Addr().String()}
			// populate etcd but with lesser data than previous populate call, so that the new db has a lower revision
			resp := &utils.EtcdDataPopulationResponse{}
			utils.PopulateEtcd(testCtx, logger, endpoints, 0, int(keyTo/2), resp)
			Expect(resp.Err).ShouldNot(HaveOccurred())
			etcd.Close()

			// etcdRevision: latest revision number on the snapstore (etcd backup)
			// resp.EndRevision: current revision number on etcd db
			Expect(etcdRevision).To(BeNumerically(">=", resp.EndRevision))

			dataDirStatus, err := validator.Validate(testCtx, Full, 0)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(int(dataDirStatus)).Should(Equal(RevisionConsistencyError))
		})
	})

	Context("with fail below revision configured to low value and no snapshots taken", func() {
		It("should return DataDirStatus as DataDirectoryValid, and nil error", func() {
			validator.Config.SnapstoreConfig.Container = path.Join(snapstoreBackupDir, "tmp")
			dataDirStatus, err := validator.Validate(testCtx, Full, 0)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(int(dataDirStatus)).Should(Equal(DataDirectoryValid))
		})
	})

	Context("with fail below revision configured to high value", func() {
		const failBelowRevision = math.MaxInt64
		BeforeEach(func() {
			validator.Config.SnapstoreConfig = snapstoreConfig
		})

		Context("with snapstore config provided and snapshot present", func() {
			It("should return DataDirStatus as DataDirectoryValid and nil error", func() {
				dataDirStatus, err := validator.Validate(testCtx, Sanity, failBelowRevision)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(int(dataDirStatus)).Should(Equal(DataDirectoryValid))
			})
		})

		Context("with snapstore config provided but no snapshots present", func() {
			It("should return DataDirStatus as FailBelowRevisionConsistencyError and nil error", func() {
				validator.Config.SnapstoreConfig.Container = path.Join(snapstoreBackupDir, "tmp")
				dataDirStatus, err := validator.Validate(testCtx, Sanity, failBelowRevision)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(int(dataDirStatus)).Should(Equal(FailBelowRevisionConsistencyError))
			})
		})
	})

	Context("without providing snapstore config", func() {
		It("should return DataDirStatus as DataDirectoryValid and nil error for low failBelowRevision", func() {
			validator.Config.SnapstoreConfig = nil
			dataDirStatus, err := validator.Validate(testCtx, Sanity, 0)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(int(dataDirStatus)).Should(Equal(DataDirectoryValid))
		})

		It("should return DataDirStatus as DataDirectoryValid and nil error for high failBelowRevision", func() {
			validator.Config.SnapstoreConfig = nil
			dataDirStatus, err := validator.Validate(testCtx, Sanity, math.MaxInt64)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(int(dataDirStatus)).Should(Equal(DataDirectoryValid))
		})
	})

	Context("with failure on snapstore call due to unknown snapstore provider", func() {
		It("should return DataDirStatus as DataDirectoryStatusUnknown and error", func() {
			//this is to fake the failure the snapstore call.
			validator.Config.SnapstoreConfig.Provider = "unknown"
			dataDirStatus, err := validator.Validate(testCtx, Full, 0)
			Expect(err).Should(HaveOccurred())
			Expect(int(dataDirStatus)).Should(Equal(DataDirectoryStatusUnknown))
		})
	})

	Context("with failure on snapstore call due to fake failing snapstore provider", func() {
		It("should return DataDirStatus as DataDirectoryStatusUnknown and error", func() {
			//this is to fake the failure the snapstore call.
			validator.Config.SnapstoreConfig.Provider = snapstore.SnapstoreProviderFakeFailed
			dataDirStatus, err := validator.Validate(testCtx, Full, 0)
			Expect(err).Should(HaveOccurred())
			Expect(int(dataDirStatus)).Should(Equal(DataDirectoryStatusUnknown))
		})
	})
})

func createCorruptSnap(filePath string) {
	file, err := os.Create(filePath)
	Expect(err).ShouldNot(HaveOccurred())
	defer file.Close()

	// corrupt the snap file by writing random data to it
	byteSlice := []byte("Random data!\n")
	_, err = file.Write(byteSlice)
	Expect(err).ShouldNot(HaveOccurred())
}
