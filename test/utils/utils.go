// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/compressor"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/snapshotter"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/gardener/etcd-backup-restore/pkg/wrappers"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
)

const (
	// KeyPrefix is prefix for keys inserted in etcd as a part of etcd-backup-restore tests.
	KeyPrefix = "/etcdbr/test/key-"
	// ValuePrefix is prefix for value inserted in etcd as a part of etcd-backup-restore tests.
	ValuePrefix = "val-"
	// EmbeddedEtcdPortNo defines PortNo which can be used to start EmbeddedEtcd.
	EmbeddedEtcdPortNo = "12379"
	// DefaultEtcdName defines the default etcd name used to start EmbeddedEtcd.
	DefaultEtcdName = "default"
)

// StartEmbeddedEtcd starts the embedded etcd for test purpose with minimal configuration at a given port.
// To get the exact client endpoints it is listening on, use returns etcd.Clients[0].Addr().String()
func StartEmbeddedEtcd(ctx context.Context, etcdDir string, logger *logrus.Entry, name string, port string) (*embed.Etcd, error) {
	logger.Infoln("Starting embedded etcd...")
	cfg := embed.NewConfig()
	cfg.Name = name
	if len(name) == 0 {
		cfg.Name = DefaultEtcdName
	}
	cfg.Dir = etcdDir
	cfg.EnableV2 = false
	cfg.Debug = false
	cfg.GRPCKeepAliveTimeout = 0
	cfg.SnapshotCount = 10
	DefaultListenPeerURLs := "http://localhost:" + getPeerPortNo(port)
	DefaultListenClientURLs := "http://localhost:" + getClientPortNo(port)
	DefaultInitialAdvertisePeerURLs := DefaultListenPeerURLs
	DefaultAdvertiseClientURLs := DefaultListenClientURLs
	lpurl, err := url.Parse(DefaultListenPeerURLs)
	if err != nil {
		return nil, err
	}
	apurl, err := url.Parse(DefaultInitialAdvertisePeerURLs)
	if err != nil {
		return nil, err
	}
	lcurl, err := url.Parse(DefaultListenClientURLs)
	if err != nil {
		return nil, err
	}
	acurl, err := url.Parse(DefaultAdvertiseClientURLs)
	if err != nil {
		return nil, err
	}
	cfg.ListenPeerUrls = []url.URL{*lpurl}
	cfg.ListenClientUrls = []url.URL{*lcurl}
	cfg.AdvertisePeerUrls = []url.URL{*apurl}
	cfg.AdvertiseClientUrls = []url.URL{*acurl}
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
	cfg.Logger = "zap"
	cfg.AutoCompactionMode = "periodic"
	cfg.AutoCompactionRetention = "0"
	e, err := embed.StartEtcd(cfg)
	if err != nil {
		return nil, err
	}

	etcdWaitCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	select {
	case <-e.Server.ReadyNotify():
		logger.Infof("Embedded server is ready to listen client at: %s", e.Clients[0].Addr())
	case <-etcdWaitCtx.Done():
		e.Server.Stop() // trigger a shutdown
		e.Close()
		return nil, fmt.Errorf("server took too long to start")
	}
	return e, nil
}

// getPeerPortNo returns the Peer PortNo.
func getPeerPortNo(port string) string {
	if len(strings.TrimSpace(port)) == 0 {
		return "0"
	}

	lastDigit, err := strconv.Atoi(port[len(port)-1:])
	if err != nil {
		return "0"
	}

	return strings.TrimSpace(port[:len(port)-1] + strconv.Itoa((lastDigit+1)%10))
}

// getClientPortNo returns the Client PortNo.
func getClientPortNo(port string) string {
	if len(strings.TrimSpace(port)) == 0 {
		return "0"
	}
	return strings.TrimSpace(port)
}

// EtcdDataPopulationResponse is response about etcd data population
type EtcdDataPopulationResponse struct {
	KeyTo       int
	EndRevision int64
	Err         error
}

// PopulateEtcd sequentially puts key-value pairs into the embedded etcd, from key <keyFrom> (including) to <keyTo> (excluding). Every key divisible by 10 will be
// be added and deleted immediately. So, for such key you will observer two events on etcd PUT and DELETE and key not being present in etcd at end.
func PopulateEtcd(ctx context.Context, logger *logrus.Entry, endpoints []string, keyFrom, keyTo int, response *EtcdDataPopulationResponse) {
	if response == nil {
		response = &EtcdDataPopulationResponse{}
	}
	response.KeyTo = keyFrom - 1
	logger.Infof("keyFrom: %v, keyTo: %v", keyFrom, keyTo)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 10 * time.Second,
	})
	if err != nil {
		response.Err = fmt.Errorf("unable to start etcd client: %v", err)
		return
	}
	defer cli.Close()

	for {
		select {
		case <-ctx.Done():
			logger.Infof("Populated data till key %s into embedded etcd with etcd end revision: %v", KeyPrefix+strconv.Itoa(response.KeyTo), response.EndRevision)
			return
		case <-time.After(time.Second):
			response.KeyTo++
			if response.KeyTo > keyTo {
				logger.Infof("Populated data till key %s into embedded etcd with etcd end revision: %v", KeyPrefix+strconv.Itoa(response.KeyTo-1), response.EndRevision)
				return
			}
			key := KeyPrefix + strconv.Itoa(response.KeyTo)
			value := ValuePrefix + strconv.Itoa(response.KeyTo)
			resp, err := cli.Put(ctx, key, value)
			if err != nil {
				response.Err = fmt.Errorf("unable to put key-value pair (%s, %s) into embedded etcd: %v", key, value, err)
				logger.Infof("Populated data till key %s into embedded etcd with etcd end revision: %v", KeyPrefix+strconv.Itoa(response.KeyTo), response.EndRevision)
				return
			}
			response.EndRevision = resp.Header.GetRevision()
			// call a delete for every 10th Key after putting it in the store to check deletes in consistency check
			// handles deleted keys as every 10th key is deleted during populate etcd call
			// this handling is also done in the checkDataConsistency() in restorer_test.go file
			// also it assumes that the deltaSnapshotDuration is more than 10.
			// if you change the constant please change the factor accordingly to have coverage of delete scenarios.
			if math.Mod(float64(response.KeyTo), 10) == 0 {
				resp, err := cli.Delete(ctx, key)
				if err != nil {
					response.Err = fmt.Errorf("unable to delete key (%s) from embedded etcd: %v", key, err)
					logger.Infof("Populated data till key %s into embedded etcd with etcd end revision: %v", KeyPrefix+strconv.Itoa(response.KeyTo), response.EndRevision)
					return
				}
				response.EndRevision = resp.Header.GetRevision()
			}
		}
	}
}

// PopulateEtcdWithWaitGroup sequentially puts key-value pairs into the embedded etcd, until stopped via context. Use `wg.Wait()` to make sure that etcd population has stopped completely.
func PopulateEtcdWithWaitGroup(ctx context.Context, wg *sync.WaitGroup, logger *logrus.Entry, endpoints []string, resp *EtcdDataPopulationResponse) {
	defer wg.Done()
	PopulateEtcd(ctx, logger, endpoints, 0, math.MaxInt64, resp)
}

// ContextWithWaitGroup returns a copy of parent with a new Done channel. The returned
// context's Done channel is closed when the the passed waitGroup's Wait function is called
// or when the parent context's Done channel is closed, whichever happens first.
func ContextWithWaitGroup(parent context.Context, wg *sync.WaitGroup) context.Context {
	ctx, cancel := context.WithCancel(parent)
	go func() {
		wg.Wait()
		cancel()
	}()
	return ctx
}

// ContextWithGracePeriod returns a new context, whose Done channel is closed when parent
// context is closed with additional <gracePeriod>.
func ContextWithGracePeriod(parent context.Context, gracePeriod time.Duration) context.Context {
	ctx, cancel := context.WithCancel(context.TODO())
	go func() {
		<-parent.Done()
		<-time.After(gracePeriod)
		cancel()
	}()
	return ctx
}

// ContextWithWaitGroupFollwedByGracePeriod returns a new context, whose Done channel is closed
// when parent context is closed or wait of waitGroup is over, with additional <gracePeriod>.
func ContextWithWaitGroupFollwedByGracePeriod(parent context.Context, wg *sync.WaitGroup, gracePeriod time.Duration) context.Context {
	ctx := ContextWithWaitGroup(parent, wg)
	return ContextWithGracePeriod(ctx, gracePeriod)
}

// RunSnapshotter creates a snapshotter object and runs it for a duration specified by 'snapshotterDurationSeconds'
func RunSnapshotter(logger *logrus.Entry, snapstoreConfig brtypes.SnapstoreConfig, deltaSnapshotPeriod time.Duration, endpoints []string, stopCh <-chan struct{}, startWithFullSnapshot bool, compressionConfig *compressor.CompressionConfig) error {
	store, err := snapstore.GetSnapstore(&snapstoreConfig)
	if err != nil {
		return err
	}

	etcdConnectionConfig := brtypes.NewEtcdConnectionConfig()
	etcdConnectionConfig.ConnectionTimeout.Duration = 10 * time.Second
	etcdConnectionConfig.Endpoints = endpoints

	snapshotterConfig := &brtypes.SnapshotterConfig{
		FullSnapshotSchedule:     "0 0 1 1 *",
		DeltaSnapshotPeriod:      wrappers.Duration{Duration: deltaSnapshotPeriod},
		DeltaSnapshotMemoryLimit: brtypes.DefaultDeltaSnapMemoryLimit,
		GarbageCollectionPeriod:  wrappers.Duration{Duration: time.Minute},
		GarbageCollectionPolicy:  brtypes.GarbageCollectionPolicyLimitBased,
		MaxBackups:               1,
	}

	healthConfig := brtypes.NewHealthConfig()

	ssr, err := snapshotter.NewSnapshotter(logger, snapshotterConfig, store, etcdConnectionConfig, compressionConfig, healthConfig, &snapstoreConfig)
	if err != nil {
		return err
	}

	return ssr.Run(stopCh, startWithFullSnapshot)
}

// CheckDataConsistency starts an embedded etcd and checks for correctness of the values stored in etcd against the keys 'keyFrom' through 'keyTo'
func CheckDataConsistency(ctx context.Context, dir string, keyTo int, logger *logrus.Entry) error {
	etcd, err := StartEmbeddedEtcd(ctx, dir, logger, DefaultEtcdName, EmbeddedEtcdPortNo)
	if err != nil {
		return fmt.Errorf("unable to start embedded etcd server: %v", err)
	}
	defer etcd.Close()
	endpoints := []string{etcd.Clients[0].Addr().String()}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 10 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("unable to start etcd client: %v", err)
	}
	defer cli.Close()

	var (
		key      string
		value    string
		resKey   string
		resValue string
	)

	for currKey := 0; currKey <= keyTo; currKey++ {
		key = KeyPrefix + strconv.Itoa(currKey)
		value = ValuePrefix + strconv.Itoa(currKey)

		resp, err := cli.Get(ctx, key, clientv3.WithLimit(1))
		if err != nil {
			return fmt.Errorf("unable to get value from etcd: %v", err)
		}
		if len(resp.Kvs) == 0 {
			// handles deleted keys as every 10th key is deleted during populate etcd call
			// this handling is also done in the populateEtcd() in restorer_suite_test.go file
			// also it assumes that the deltaSnapshotDuration is more than 10 --
			// if you change the constant please change the factor accordingly to have coverage of delete scenarios.
			if math.Mod(float64(currKey), 10) == 0 {
				continue //it should continue as key was put for action delete
			} else {
				return fmt.Errorf("entry not found for key %s", key)
			}
		}
		res := resp.Kvs[0]
		resKey = string(res.Key)
		resValue = string(res.Value)

		if resKey != key {
			return fmt.Errorf("key mismatch for %s and %s", resKey, key)
		}
		if resValue != value {
			return fmt.Errorf("invalid etcd data - value mismatch for %s and %s", resValue, value)
		}
	}
	fmt.Printf("Data consistency for key-value pairs (%[1]s%[3]d, %[2]s%[3]d) through (%[1]s%[4]d, %[2]s%[4]d) has been verified\n", KeyPrefix, ValuePrefix, 0, keyTo)

	return nil
}
