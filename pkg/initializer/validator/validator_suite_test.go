package validator_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/compressor"
	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/snapshotter"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/gardener/etcd-backup-restore/test/utils"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/embed"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	outputDir    = "../../../test/output"
	etcdDir      = outputDir + "/default.etcd"
	snapstoreDir = outputDir + "/snapshotter.bkp"
)

var (
	testCtx      = context.Background()
	logger       = logrus.New().WithField("suite", "validator")
	etcd         *embed.Etcd
	err          error
	keyTo        int
	endpoints    []string
	etcdRevision int64
)

// fileInfo holds file information such as file name and file path
type fileInfo struct {
	name string
	path string
}

func TestValidator(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Validator Suite")
}

var _ = SynchronizedBeforeSuite(func() []byte {
	var (
		data []byte
	)

	err = os.RemoveAll(outputDir)
	Expect(err).ShouldNot(HaveOccurred())

	etcd, err = utils.StartEmbeddedEtcd(testCtx, etcdDir, logger)
	Expect(err).ShouldNot(HaveOccurred())
	defer func() {
		etcd.Server.Stop()
		etcd.Close()
	}()
	endpoints = []string{etcd.Clients[0].Addr().String()}

	populatorCtx, cancelPopulator := context.WithTimeout(testCtx, time.Duration(15*time.Second))
	defer cancelPopulator()
	resp := &utils.EtcdDataPopulationResponse{}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go utils.PopulateEtcdWithWaitGroup(populatorCtx, wg, logger, endpoints, resp)

	deltaSnapshotPeriod := 5 * time.Second
	ctx := utils.ContextWithWaitGroupFollwedByGracePeriod(populatorCtx, wg, deltaSnapshotPeriod+2*time.Second)
	err = runSnapshotter(logger, deltaSnapshotPeriod, endpoints, ctx.Done())
	Expect(err).ShouldNot(HaveOccurred())

	keyTo = resp.KeyTo
	etcdRevision = resp.EndRevision

	err = os.Mkdir(path.Join(outputDir, "temp"), 0700)
	Expect(err).ShouldNot(HaveOccurred())

	return data
}, func(data []byte) {})

// runSnapshotter creates a snapshotter object and runs it for a duration specified by 'snapshotterDurationSeconds'
func runSnapshotter(logger *logrus.Entry, deltaSnapshotPeriod time.Duration, endpoints []string, stopCh <-chan struct{}) error {
	store, err := snapstore.GetSnapstore(&brtypes.SnapstoreConfig{Container: snapstoreDir, Provider: "Local"})
	if err != nil {
		return err
	}

	compressionConfig := compressor.NewCompressorConfig()
	etcdConnectionConfig := etcdutil.NewEtcdConnectionConfig()
	etcdConnectionConfig.Endpoints = endpoints
	etcdConnectionConfig.ConnectionTimeout.Duration = 10 * time.Second
	logger.Infof("etcdConnectionConfig %v", etcdConnectionConfig)

	snapshotterConfig := snapshotter.NewSnapshotterConfig()
	snapshotterConfig.GarbageCollectionPolicy = brtypes.GarbageCollectionPolicyLimitBased
	snapshotterConfig.FullSnapshotSchedule = "0 0 1 1 *"
	snapshotterConfig.MaxBackups = 1

	ssr, err := snapshotter.NewSnapshotter(logger, snapshotterConfig, store, etcdConnectionConfig, compressionConfig)
	if err != nil {
		return err
	}

	return ssr.Run(stopCh, true)
}

// copyFile copies the contents of the file at sourceFilePath into the file at destinationFilePath. If no file exists at destinationFilePath, a new file is created before copying
func copyFile(sourceFilePath, destinationFilePath string, filePermission os.FileMode) error {
	data, err := ioutil.ReadFile(sourceFilePath)
	if err != nil {
		return fmt.Errorf("unable to read source file %s: %v", sourceFilePath, err)
	}

	destinationDirectoryPath := path.Dir(destinationFilePath)
	err = os.MkdirAll(destinationDirectoryPath, filePermission)
	if err != nil {
		return fmt.Errorf("unable to create destination directory %s: %v", destinationDirectoryPath, err)
	}

	err = ioutil.WriteFile(destinationFilePath, data, filePermission)
	if err != nil {
		return fmt.Errorf("unable to create destination file %s: %v", destinationFilePath, err)
	}
	return nil
}

// copyDir copies the contents of the Source dir to the destination dir.
func copyDir(sourceDirPath, destinationDirPath string) error {
	if len(sourceDirPath) == 0 || len(destinationDirPath) == 0 {
		return nil
	}

	files, err := ioutil.ReadDir(sourceDirPath)
	if err != nil {
		return err
	}

	for _, file := range files {
		sourcePath := path.Join(sourceDirPath, file.Name())
		destPath := path.Join(destinationDirPath, file.Name())

		fileInfo, err := os.Stat(sourcePath)
		if err != nil {
			return err
		}

		if fileInfo.Mode().IsDir() {
			os.Mkdir(destPath, fileInfo.Mode())
			copyDir(sourcePath, destPath)
		} else if fileInfo.Mode().IsRegular() {
			copyFile(sourcePath, destPath, fileInfo.Mode())
		} else {
			return fmt.Errorf("File format not known")
		}
	}
	return nil
}
