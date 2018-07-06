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

	. "github.com/gardener/etcd-backup-restore/test/e2e/integration"
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
		"--garbage-collection-period-seconds=30",
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

	var store snapstore.SnapStore

	Describe("Regular backups", func() {
		var (
			cmdEtcd, cmdEtcdbrctl      *Cmd
			etcdErrChan, etcdbrErrChan *chan error
			err                        error
		)

		BeforeEach(func() {
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

			snapstoreConfig := &snapstore.Config{
				Provider:  "S3",
				Container: os.Getenv("TEST_ID"),
				Prefix:    path.Join("v1"),
			}
			store, err = snapstore.GetSnapstore(snapstoreConfig)
			Expect(err).ShouldNot(HaveOccurred())

			snapList, err := store.List()
			Expect(err).ShouldNot(HaveOccurred())
			for _, snap := range snapList {
				store.Delete(*snap)
			}

		})

		Context("taken at 1 minute interval", func() {
			It("should take periodic backups.", func() {
				snaplist, err := store.List()
				Expect(snaplist).Should(BeEmpty())
				Expect(err).ShouldNot(HaveOccurred())

				time.Sleep(1 * time.Minute)

				snaplist, err = store.List()
				Expect(snaplist).ShouldNot(BeEmpty())
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

		Context("taken at 1 minute interval", func() {
			It("should take periodic backups and limit based garbage collect backups over maxBackups configured", func() {
				snaplist, err := store.List()
				Expect(snaplist).Should(BeEmpty())
				Expect(err).ShouldNot(HaveOccurred())

				time.Sleep(160 * time.Second)

				snaplist, err = store.List()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(len(snaplist)).To(Equal(1))
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
})
