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

	"github.com/coreos/etcd/embed"
	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/snapshotter"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	"github.com/gardener/etcd-backup-restore/test/utils"
	"github.com/sirupsen/logrus"

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
	var (
		store                    snapstore.SnapStore
		certFile                 string
		keyFile                  string
		caFile                   string
		insecureTransport        = true
		insecureSkipVerify       = true
		maxBackups               = 1
		deltaSnapshotMemoryLimit = 10 * 1024 * 1024 //10Mib
		etcdConnectionTimeout    = 10 * time.Second
		garbageCollectionPeriod  = 60 * time.Second
		schedule                 = "0 0 1 1 *"
		garbageCollectionPolicy  = snapshotter.GarbageCollectionPolicyLimitBased
		etcdUsername             string
		etcdPassword             string
	)

	store, err = snapstore.GetSnapstore(&snapstore.Config{Container: snapstoreDir, Provider: "Local"})
	if err != nil {
		return err
	}

	tlsConfig := etcdutil.NewTLSConfig(
		certFile,
		keyFile,
		caFile,
		insecureTransport,
		insecureSkipVerify,
		endpoints,
		etcdUsername,
		etcdPassword,
	)

	logger.Infof("tlsconfig %v", tlsConfig)
	snapshotterConfig, err := snapshotter.NewSnapshotterConfig(
		schedule,
		store,
		maxBackups,
		deltaSnapshotMemoryLimit,
		deltaSnapshotPeriod,
		etcdConnectionTimeout,
		garbageCollectionPeriod,
		garbageCollectionPolicy,
		tlsConfig,
	)
	if err != nil {
		return err
	}

	ssr := snapshotter.NewSnapshotter(
		logger,
		snapshotterConfig,
	)

	return ssr.Run(stopCh, true)
}

// copyFile copies the contents of the file at sourceFilePath into the file at destinationFilePath. If no file exists at destinationFilePath, a new file is created before copying
func copyFile(sourceFilePath, destinationFilePath string) error {
	data, err := ioutil.ReadFile(sourceFilePath)
	if err != nil {
		return fmt.Errorf("unable to read source file %s: %v", sourceFilePath, err)
	}

	err = ioutil.WriteFile(destinationFilePath, data, 0700)
	if err != nil {
		return fmt.Errorf("unable to create destination file %s: %v", destinationFilePath, err)
	}

	return nil
}
