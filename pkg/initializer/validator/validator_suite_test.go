package validator_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/snapshotter"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
)

const (
	outputDir                  = "../../../test/output"
	etcdDir                    = outputDir + "/default.etcd"
	snapstoreDir               = outputDir + "/snapshotter.bkp"
	etcdEndpoint               = "http://localhost:2379"
	snapshotterDurationSeconds = 15
	snapCount                  = 10
	keyPrefix                  = "key-"
	valuePrefix                = "val-"
	keyFrom                    = 1
)

var (
	etcd         *embed.Etcd
	err          error
	keyTo        int
	endpoints    = []string{etcdEndpoint}
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
		data            []byte
		errCh           = make(chan error)
		populatorStopCh = make(chan bool)
		ssrStopCh       = make(chan struct{})
	)

	err = os.RemoveAll(outputDir)
	Expect(err).ShouldNot(HaveOccurred())

	logger := logrus.New()

	err = os.RemoveAll(etcdDir)
	Expect(err).ShouldNot(HaveOccurred())

	etcd, err = startEmbeddedEtcd(etcdDir, logger)
	Expect(err).ShouldNot(HaveOccurred())
	wg := &sync.WaitGroup{}
	deltaSnapshotPeriod := 5
	wg.Add(1)
	go populateEtcd(wg, logger, endpoints, errCh, populatorStopCh)

	go func() {
		<-time.After(time.Duration(snapshotterDurationSeconds * time.Second))
		close(populatorStopCh)
		wg.Wait()
		time.Sleep(time.Duration(deltaSnapshotPeriod+2) * time.Second)
		close(ssrStopCh)
	}()

	err = runSnapshotter(logger, deltaSnapshotPeriod, endpoints, ssrStopCh)
	Expect(err).ShouldNot(HaveOccurred())

	etcd.Server.Stop()
	etcd.Close()

	err = os.Mkdir(path.Join(outputDir, "temp"), 0700)
	Expect(err).ShouldNot(HaveOccurred())

	return data
}, func(data []byte) {})

var _ = SynchronizedAfterSuite(func() {}, func() {

})

// startEmbeddedEtcd starts an embedded etcd server
func startEmbeddedEtcd(dir string, logger *logrus.Logger) (*embed.Etcd, error) {
	logger.Infof("Starting embedded etcd")
	cfg := embed.NewConfig()
	cfg.Dir = dir
	cfg.EnableV2 = false
	cfg.Debug = false
	cfg.GRPCKeepAliveTimeout = 0
	cfg.SnapCount = snapCount
	e, err := embed.StartEtcd(cfg)
	if err != nil {
		return nil, err
	}

	select {
	case <-e.Server.ReadyNotify():
		fmt.Printf("Embedded server is ready!\n")
	case <-time.After(60 * time.Second):
		e.Server.Stop() // trigger a shutdown
		e.Close()
		return nil, fmt.Errorf("server took too long to start")
	}
	return e, nil
}

// runSnapshotter creates a snapshotter object and runs it for a duration specified by 'snapshotterDurationSeconds'
func runSnapshotter(logger *logrus.Logger, deltaSnapshotPeriod int, endpoints []string, stopCh chan struct{}) error {
	var (
		store                          snapstore.SnapStore
		certFile                       string
		keyFile                        string
		caFile                         string
		insecureTransport              bool
		insecureSkipVerify             bool
		maxBackups                     = 1
		deltaSnapshotMemoryLimit       = 10 * 1024 * 1024 //10Mib
		etcdConnectionTimeout          = time.Duration(10)
		garbageCollectionPeriodSeconds = time.Duration(60)
		schedule                       = "0 0 1 1 *"
		garbageCollectionPolicy        = snapshotter.GarbageCollectionPolicyLimitBased
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
	)

	snapshotterConfig, err := snapshotter.NewSnapshotterConfig(
		schedule,
		store,
		maxBackups,
		deltaSnapshotPeriod,
		deltaSnapshotMemoryLimit,
		etcdConnectionTimeout,
		garbageCollectionPeriodSeconds,
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

// populateEtcd sequentially puts key-value pairs into the embedded etcd, until stopped
func populateEtcd(wg *sync.WaitGroup, logger *logrus.Logger, endpoints []string, errCh chan<- error, stopCh <-chan bool) {
	defer wg.Done()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 10 * time.Second,
	})
	if err != nil {
		errCh <- fmt.Errorf("unable to start etcd client: %v", err)
		return
	}
	defer cli.Close()

	var (
		key     string
		value   string
		currKey = 0
	)

	for {
		select {
		case _, more := <-stopCh:
			if !more {
				keyTo = currKey
				logger.Infof("Populated data till key %s into embedded etcd", keyPrefix+strconv.Itoa(currKey))
				return
			}
		default:
			currKey++
			key = keyPrefix + strconv.Itoa(currKey)
			value = valuePrefix + strconv.Itoa(currKey)
			resp, err := cli.Put(context.TODO(), key, value)
			if err != nil {
				errCh <- fmt.Errorf("unable to put key-value pair (%s, %s) into embedded etcd: %v", key, value, err)
				return
			}
			etcdRevision = resp.Header.GetRevision()
			time.Sleep(time.Second * 1)
		}
	}
}

// populateEtcd sequentially puts key-value pairs through numbers 'from' and 'to' into the embedded etcd
func populateEtcdFinite(logger *logrus.Logger, endpoints []string, from, to int) (int64, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 10 * time.Second,
	})
	if err != nil {
		return -1, fmt.Errorf("unable to start etcd client: %v", err)
	}
	defer cli.Close()

	var (
		key   string
		value string
		rev   int64
	)

	for currKey := from; currKey <= to; currKey++ {
		key = keyPrefix + strconv.Itoa(currKey)
		value = valuePrefix + strconv.Itoa(currKey)
		resp, err := cli.Put(context.TODO(), key, value)
		if err != nil {
			return -1, fmt.Errorf("unable to put key-value pair (%s, %s) into embedded etcd: %v", key, value, err)
		}
		rev = resp.Header.GetRevision()
		time.Sleep(time.Millisecond * 100)
	}

	return rev, nil
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
