package integration_test

import (
	"bufio"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/initializer/validator"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"

	. "github.com/gardener/etcd-backup-restore/tests/e2e/integration"
)

var _ = Describe("CloudBackup", func() {
	var cmdEtcd, cmdEtcdbrctl *Cmd
	var logger *logrus.Logger
	var s3Snapstore snapstore.SnapStore
	var err error

	Describe("Regular backups", func() {
		BeforeEach(func() {
			logger := logrus.New()
			etcdArgs := []string{"--name=etcd",
				"--advertise-client-urls=http://0.0.0.0:2379",
				"--listen-client-urls=http://0.0.0.0:2379",
				"--initial-cluster-state=new",
				"--initial-cluster-token=new",
				"--log-output=stdout",
				"--data-dir=" + os.Getenv("ETCD_DATA_DIR")}
			cmdEtcd = &Cmd{
				Task:    "etcd",
				Flags:   etcdArgs,
				Logger:  logger,
				Logfile: filepath.Join(os.Getenv("ETCD_DATA_DIR"), "etcd.log"),
			}
			logger.Info("Starting etcd..")
			go cmdEtcd.RunCmdWithFlags()
		})
		BeforeEach(func() {
			logger = logrus.New()
			etcdbrctlArgs := []string{
				"snapshot",
				"--max-backups=1",
				"--schedule=*/1 * * * *",
				"--storage-provider=S3",
				"--store-container=" + os.Getenv("NAMESPACE"),
			}
			logger.Info(etcdbrctlArgs)
			cmdEtcdbrctl = &Cmd{
				Task:    "etcdbrctl",
				Flags:   etcdbrctlArgs,
				Logger:  logger,
				Logfile: filepath.Join(os.Getenv("ETCD_DATA_DIR"), "etcdbrctl.log"),
			}
			go cmdEtcdbrctl.RunCmdWithFlags()
			s3SnapstoreConfig := &snapstore.Config{
				Provider:  "S3",
				Container: os.Getenv("NAMESPACE"),
				Prefix:    path.Join("v1"),
			}
			s3Snapstore, err = snapstore.GetSnapstore(s3SnapstoreConfig)
			Expect(err).ShouldNot(HaveOccurred())
		})

		BeforeEach(func() {
			snapList, err := s3Snapstore.List()
			Expect(err).ShouldNot(HaveOccurred())
			for _, snap := range snapList {
				s3Snapstore.Delete(*snap)
			}

		})
		Context("taken at 1 minute interval", func() {
			It("should take periodic backups.", func() {
				snap, err := s3Snapstore.GetLatest()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(snap).Should(BeNil())

				time.Sleep(1 * time.Minute)

				latestSnap, err := s3Snapstore.GetLatest()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(latestSnap).ShouldNot(BeNil())
			})
		})
		Context("taken at 1 minute interval", func() {
			It("should take periodic backups and garbage collect backups over maxBackups configured", func() {
				snap, err := s3Snapstore.GetLatest()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(snap).Should(BeNil())

				time.Sleep(1 * time.Minute)

				firstSnap, err := s3Snapstore.GetLatest()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(firstSnap).ShouldNot(BeNil())

				time.Sleep(1 * time.Minute)

				snaps, err := s3Snapstore.List()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(len(snaps)).To(Equal(1))
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
						DataDir: dataDir,
					},
				}
				dataDirStatus, err := dataValidator.Validate()
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
						DataDir: dataDir,
					},
				}
				dataDirStatus, err := dataValidator.Validate()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(dataDirStatus).Should(Equal(validator.DataDirStatus(validator.DataDirectoryCorrupt)))
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
					"--store-container=" + os.Getenv("NAMESPACE"),
					"--data-dir=" + dataDir,
				}
				cmd = &Cmd{
					Task:    "etcdbrctl",
					Flags:   etcdbrctlArgs,
					Logger:  logger,
					Logfile: filepath.Join(os.Getenv("ETCD_DATA_DIR"), "etcdbrctl.log"),
				}
				cmd.RunCmdWithFlags()

			})

		})
	})

})
