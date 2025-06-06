// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package miscellaneous

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	errored "errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/errors"
	etcdClient "github.com/gardener/etcd-backup-restore/pkg/etcdutil/client"
	"github.com/gardener/etcd-backup-restore/pkg/metrics"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
	"go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/pkg/types"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	fake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/yaml"
)

const (
	// NoLeaderState defines the state when etcd returns LeaderID as 0.
	NoLeaderState uint64 = 0
	// EtcdConfigFilePath is the file path where the etcd config map is mounted.
	EtcdConfigFilePath string = "/var/etcd/config/etcd.conf.yaml"
	// ClusterStateNew defines the "new" state of etcd cluster.
	ClusterStateNew = "new"
	// ClusterStateExisting defines the "existing" state of etcd cluster.
	ClusterStateExisting = "existing"

	// ScaledToMultiNodeAnnotationKey defines annotation key for scale-up to multi-node cluster.
	ScaledToMultiNodeAnnotationKey = "gardener.cloud/scaled-to-multi-node"

	https = "https"

	// etcdWrapperPort defines the port no. used by etcd-wrapper.
	etcdWrapperPort = "9095"
)

type advertiseURLsConfig struct {
	AdvertiseClientURLs      map[string][]string `json:"advertise-client-urls"`
	InitialAdvertisePeerURLs map[string][]string `json:"initial-advertise-peer-urls"`
}

// GetLatestFullSnapshotAndDeltaSnapList returns the latest snapshot
func GetLatestFullSnapshotAndDeltaSnapList(store brtypes.SnapStore) (*brtypes.Snapshot, brtypes.SnapList, error) {
	var (
		fullSnapshot  *brtypes.Snapshot
		deltaSnapList brtypes.SnapList
	)
	snapList, err := store.List(false)
	if err != nil {
		return nil, nil, err
	}

	for index := len(snapList); index > 0; index-- {
		if snapList[index-1].IsChunk {
			continue
		}
		if snapList[index-1].Kind == brtypes.SnapshotKindFull {
			fullSnapshot = snapList[index-1]
			break
		}
		deltaSnapList = append(deltaSnapList, snapList[index-1])
	}

	sort.Sort(deltaSnapList) // ensures that the delta snapshot list is well formed
	metrics.SnapstoreLatestDeltasTotal.With(prometheus.Labels{}).Set(float64(len(deltaSnapList)))
	if len(deltaSnapList) == 0 {
		metrics.SnapstoreLatestDeltasRevisionsTotal.With(prometheus.Labels{}).Set(0)
	} else {
		revisionDiff := deltaSnapList[len(deltaSnapList)-1].LastRevision - deltaSnapList[0].StartRevision
		metrics.SnapstoreLatestDeltasRevisionsTotal.With(prometheus.Labels{}).Set(float64(revisionDiff))
	}
	return fullSnapshot, deltaSnapList, nil
}

type backup struct {
	FullSnapshot      *brtypes.Snapshot
	DeltaSnapshotList brtypes.SnapList
}

// GetFilteredBackups returns sorted by date (new -> old) SnapList. It will also filter the snapshots that should be included or not using the filter function.
// If the filter is nil it will return all snapshots. Also, maxBackups can be used to target only the last N snapshots (-1 = all).
func GetFilteredBackups(store brtypes.SnapStore, maxBackups int, filter func(snaps brtypes.Snapshot) bool) (brtypes.SnapList, error) {
	snapList, err := store.List(false)
	if err != nil {
		return nil, err
	}
	backups := getStructuredBackupList(snapList)
	sort.Slice(backups, func(i, j int) bool {
		return backups[i].FullSnapshot.CreatedOn.After(backups[j].FullSnapshot.CreatedOn)
	})

	list := brtypes.SnapList{}
	count := 0
	for _, backup := range backups {
		if maxBackups >= 0 && count == maxBackups {
			break
		}
		if filter != nil && !filter(*backup.FullSnapshot) {
			continue
		}
		list = append(list, backup.FullSnapshot)
		list = append(list, backup.DeltaSnapshotList...)
		count++
	}

	return list, nil
}

func getStructuredBackupList(snapList brtypes.SnapList) []backup {
	var (
		backups    []backup
		tempBackup = backup{}
	)

	for i := len(snapList) - 1; i >= 0; i-- {
		if snapList[i].IsChunk {
			continue
		}
		if snapList[i].Kind == brtypes.SnapshotKindFull {
			tempBackup.FullSnapshot = snapList[i]
			backups = append(backups, tempBackup)
			tempBackup = backup{}
			continue
		}
		tempBackup.DeltaSnapshotList = append(tempBackup.DeltaSnapshotList, snapList[i])
	}
	return backups
}

// StartEmbeddedEtcd starts the embedded etcd server.
func StartEmbeddedEtcd(logger *logrus.Entry, ro *brtypes.RestoreOptions) (*embed.Etcd, error) {
	cfg := embed.NewConfig()
	cfg.Dir = filepath.Join(ro.Config.DataDir)
	DefaultListenPeerURLs := "http://localhost:0"
	DefaultListenClientURLs := "http://localhost:0"
	DefaultInitialAdvertisePeerURLs := "http://localhost:0"
	DefaultAdvertiseClientURLs := "http://localhost:0"
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
	cfg.QuotaBackendBytes = ro.Config.EmbeddedEtcdQuotaBytes
	cfg.MaxRequestBytes = ro.Config.MaxRequestBytes
	cfg.MaxTxnOps = ro.Config.MaxTxnOps
	cfg.AutoCompactionMode = ro.Config.AutoCompactionMode
	cfg.AutoCompactionRetention = ro.Config.AutoCompactionRetention
	cfg.Logger = "zap"
	e, err := embed.StartEtcd(cfg)
	if err != nil {
		return nil, err
	}
	select {
	case <-e.Server.ReadyNotify():
		logger.Infof("Embedded server is ready to listen client at: %s", e.Clients[0].Addr())
	case <-time.After(60 * time.Second):
		e.Server.Stop() // trigger a shutdown
		e.Close()
		return nil, fmt.Errorf("server took too long to start")
	}
	return e, nil
}

// GetKubernetesClientSetOrError creates and returns a kubernetes clientset or an error if creation fails
func GetKubernetesClientSetOrError() (client.Client, error) {
	var cl client.Client
	restConfig, err := config.GetConfig()
	if err != nil {
		return nil, err
	}
	cl, err = client.New(restConfig, client.Options{})
	if err != nil {
		return nil, err
	}
	return cl, nil
}

// GetFakeKubernetesClientSet creates a fake client set. To be used for unit tests
func GetFakeKubernetesClientSet() client.Client {
	return fake.NewClientBuilder().Build()
}

// GetAllEtcdEndpoints returns the endPoints of all etcd-member.
func GetAllEtcdEndpoints(ctx context.Context, client etcdClient.ClusterCloser, etcdConnectionConfig *brtypes.EtcdConnectionConfig, logger *logrus.Entry) ([]string, error) {
	var etcdEndpoints []string

	ctx, cancel := context.WithTimeout(ctx, etcdConnectionConfig.ConnectionTimeout.Duration)
	defer cancel()

	membersInfo, err := client.MemberList(ctx)
	if err != nil {
		logger.Errorf("failed to get memberList of etcd with error: %v", err)
		return nil, err
	}

	for _, member := range membersInfo.Members {
		etcdEndpoints = append(etcdEndpoints, member.GetClientURLs()...)
	}

	return etcdEndpoints, nil
}

// IsEtcdClusterHealthy checks whether all members of etcd cluster are in healthy state or not.
func IsEtcdClusterHealthy(ctx context.Context, client etcdClient.MaintenanceCloser, etcdConnectionConfig *brtypes.EtcdConnectionConfig, etcdEndpoints []string, logger *logrus.Entry) (bool, error) {

	// checks the health of all etcd members.
	for _, endPoint := range etcdEndpoints {
		if err := func() error {
			ctx, cancel := context.WithTimeout(ctx, etcdConnectionConfig.ConnectionTimeout.Duration)
			defer cancel()
			if _, err := client.Status(ctx, endPoint); err != nil {
				logger.Errorf("failed to get status of etcd endPoint: %v with error: %v", endPoint, err)
				return err
			}
			return nil
		}(); err != nil {
			return false, err
		}
	}

	return true, nil
}

// GetLeader will return the LeaderID as well as url of etcd leader.
func GetLeader(ctx context.Context, clientMaintenance etcdClient.MaintenanceCloser, client etcdClient.ClusterCloser, endpoint string) (uint64, []string, error) {
	if len(endpoint) == 0 {
		return NoLeaderState, nil, fmt.Errorf("etcd endpoint are not passed correctly")
	}

	response, err := clientMaintenance.Status(ctx, endpoint)
	if err != nil {
		return NoLeaderState, nil, err
	}

	if response.Leader == NoLeaderState {
		return NoLeaderState, nil, &errors.EtcdError{
			Message: "currently there is no Etcd Leader present may be due to etcd quorum loss.",
		}
	}

	membersInfo, err := client.MemberList(ctx)
	if err != nil {
		return response.Leader, nil, err
	}

	for _, member := range membersInfo.Members {
		if response.Leader == member.GetID() {
			return response.Leader, member.GetClientURLs(), nil
		}
	}
	return response.Leader, nil, nil
}

// GetBackupLeaderEndPoint takes etcd leader endpoint and portNo. of backup-restore and returns the backupLeader endpoint.
func GetBackupLeaderEndPoint(endPoints []string, port uint) (string, error) {
	if len(endPoints) == 0 {
		return "", fmt.Errorf("etcd endpoints are not passed correctly")
	}

	url, err := url.Parse(endPoints[0])
	if err != nil {
		return "", err
	}

	host, _, err := net.SplitHostPort(url.Host)
	if err != nil {
		return "", err
	}

	backupLeaderEndPoint := fmt.Sprintf("%s://%s:%s", url.Scheme, host, fmt.Sprintf("%d", port))
	return backupLeaderEndPoint, nil
}

// GetEnvVarOrError returns the value of specified environment variable or error if it's not defined.
func GetEnvVarOrError(varName string) (string, error) {
	value := os.Getenv(varName)
	if value == "" {
		err := fmt.Errorf("missing environment variable %s", varName)
		return value, err
	}

	return value, nil
}

// ProbeEtcd probes the etcd endpoint to check if an etcd is available
func ProbeEtcd(ctx context.Context, clientFactory etcdClient.Factory, logger *logrus.Entry) error {
	clientKV, err := clientFactory.NewKV()
	if err != nil {
		return &errors.EtcdError{
			Message: fmt.Sprintf("Failed to create etcd KV client: %v", err),
		}
	}
	defer clientKV.Close()

	if _, err := clientKV.Get(ctx, "foo"); err != nil {
		logger.Errorf("Failed to connect to etcd KV client: %v", err)
		return err
	}
	return nil
}

// GetConfigFilePath returns the path of the etcd configuration file
func GetConfigFilePath() string {
	// (For testing purpose) If no ETCD_CONF variable set as environment variable, then consider backup-restore server is not used for tests.
	// For tests or to run backup-restore server as standalone, user needs to set ETCD_CONF variable with proper location of ETCD config yaml
	etcdConfigForTest := os.Getenv("ETCD_CONF")
	if etcdConfigForTest != "" {
		return etcdConfigForTest
	}
	return EtcdConfigFilePath
}

// GetClusterSize returns the size of a cluster passed as a string
func GetClusterSize(cluster string) (int, error) {
	clusterMap, err := types.NewURLsMap(cluster)
	if err != nil {
		return 0, err
	}

	return len(clusterMap), nil
}

// IsMultiNode determines whether a pod is part of a multi node setup or not
// This is determined by checking the `initial-cluster` of the etcd configmap to check the number of members expected
func IsMultiNode(logger *logrus.Entry) bool {
	inputFileName := GetConfigFilePath()

	configYML, err := os.ReadFile(inputFileName) // #nosec G304 -- this is a trusted etcd config file.
	if err != nil {
		return false
	}

	config := map[string]interface{}{}
	if err := yaml.Unmarshal([]byte(configYML), &config); err != nil {
		return false
	}

	initialClusterMap, err := GetClusterSize(fmt.Sprint(config["initial-cluster"]))
	if err != nil {
		logger.Fatal("initial cluster value for not present in etcd config file")
	}

	if initialClusterMap > 1 {
		return true
	}

	return false
}

// SleepWithContext sleeps for a determined period while respecting a context
func SleepWithContext(ctx context.Context, sleepFor time.Duration) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleepFor):
			return nil
		}
	}
}

// ContainsBackup checks whether the backup is present in given SnapStore or not.
func ContainsBackup(store brtypes.SnapStore, logger *logrus.Logger) bool {
	baseSnap, deltaSnapList, err := GetLatestFullSnapshotAndDeltaSnapList(store)
	if err != nil {
		logger.Errorf("failed to list the snapshot: %v", err)
		return false
	}

	if baseSnap == nil && len(deltaSnapList) == 0 {
		logger.Infof("No snapshot found. BackupBucket is empty")
		return false
	}
	return true
}

// IsBackupBucketEmpty checks whether the backup bucket is empty or not.
func IsBackupBucketEmpty(snapStoreConfig *brtypes.SnapstoreConfig, logger *logrus.Logger) bool {
	logger.Info("Checking whether the backup bucket is empty or not...")

	if snapStoreConfig == nil || len(snapStoreConfig.Provider) == 0 {
		logger.Info("storage provider name not specified")
		return true
	}
	store, err := snapstore.GetSnapstore(snapStoreConfig)
	if err != nil {
		logger.Fatalf("failed to create snapstore from configured storage provider: %v", err)
	}

	if ContainsBackup(store, logger) {
		return false
	}
	return true
}

// GetInitialClusterStateIfScaleup checks if it is the Scale-up scenario and returns the cluster state either `new` or `existing`.
func GetInitialClusterStateIfScaleup(ctx context.Context, logger logrus.Entry, clientSet client.Client, podName string, podNS string) (*string, error) {
	// Read etcd statefulset to check annotation or updated replicas to toggle `initial-cluster-state`
	etcdSts, err := GetStatefulSet(ctx, clientSet, podNS, podName)
	if err != nil {
		logger.Errorf("unable to fetch statefulset {namespace: %s, name: %s} %v", podNS, podName[:strings.LastIndex(podName, "-")], err)
		return nil, err
	}

	if IsAnnotationPresent(etcdSts, ScaledToMultiNodeAnnotationKey) {
		return ptr.To(ClusterStateExisting), nil
	}

	if *etcdSts.Spec.Replicas > 1 && *etcdSts.Spec.Replicas > etcdSts.Status.UpdatedReplicas {
		return ptr.To(ClusterStateExisting), nil
	}
	return nil, nil
}

// IsAnnotationPresent checks the presence of given annotation in a given statefulset.
func IsAnnotationPresent(sts *appsv1.StatefulSet, annotation string) bool {
	return metav1.HasAnnotation(sts.ObjectMeta, annotation)
}

// GetStatefulSet gets the StatefulSet with the name podName in podNamespace namespace. It will return if there is any error or the StatefulSet is not found.
func GetStatefulSet(ctx context.Context, clientSet client.Client, podNamespace string, podName string) (*appsv1.StatefulSet, error) {
	curSts := &appsv1.StatefulSet{}
	if err := clientSet.Get(ctx, client.ObjectKey{
		Namespace: podNamespace,
		Name:      podName[:strings.LastIndex(podName, "-")],
	}, curSts); err != nil {
		return nil, err
	}
	return curSts, nil
}

// DoPromoteMember promotes a given learner to a voting member.
func DoPromoteMember(ctx context.Context, member *etcdserverpb.Member, cli etcdClient.ClusterCloser, logger *logrus.Entry) error {
	memPromoteCtx, cancel := context.WithTimeout(ctx, brtypes.DefaultEtcdConnectionTimeout)
	defer cancel()

	start := time.Now()
	//Member promote call will succeed only if member is in sync with leader, and will error out otherwise
	_, err := cli.MemberPromote(memPromoteCtx, member.ID)
	if err == nil {
		//Member successfully promoted
		metrics.IsLearner.With(prometheus.Labels{}).Set(0)
		metrics.MemberPromoteDurationSeconds.With(prometheus.Labels{metrics.LabelSucceeded: metrics.ValueSucceededTrue}).Observe(time.Since(start).Seconds())
		logger.Infof("Member %v with [ID: %v] has been promoted", member.GetName(), strconv.FormatUint(member.GetID(), 16))
		return nil
	} else if errored.Is(err, rpctypes.Error(rpctypes.ErrGRPCMemberNotLearner)) {
		//Member is not a learner
		logger.Info("Member ", member.Name, " : ", member.ID, " already a voting member of cluster.")
		return nil
	}

	metrics.MemberPromoteDurationSeconds.With(prometheus.Labels{metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Observe(time.Since(start).Seconds())
	return err
}

// CheckIfLearnerPresent checks whether a learner(non-voting) member present or not.
func CheckIfLearnerPresent(ctx context.Context, cli etcdClient.ClusterCloser) (bool, error) {
	membersInfo, err := cli.MemberList(ctx)
	if err != nil {
		return false, fmt.Errorf("error listing members: %v", err)
	}

	for _, member := range membersInfo.Members {
		if member.IsLearner {
			return true, nil
		}
	}
	return false, nil
}

// RemoveMemberFromCluster removes member of given ID from etcd cluster.
func RemoveMemberFromCluster(ctx context.Context, cli etcdClient.ClusterCloser, memberID uint64, logger *logrus.Entry) error {
	start := time.Now()
	_, err := cli.MemberRemove(ctx, memberID)
	if err != nil {
		metrics.MemberRemoveDurationSeconds.With(prometheus.Labels{metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Observe(time.Since(start).Seconds())
		return fmt.Errorf("unable to remove member [ID:%v] from the cluster: %v", strconv.FormatUint(memberID, 16), err)
	}

	metrics.MemberRemoveDurationSeconds.With(prometheus.Labels{metrics.LabelSucceeded: metrics.ValueSucceededTrue}).Observe(time.Since(start).Seconds())
	logger.Infof("successfully removed member [ID: %v] from the cluster", strconv.FormatUint(memberID, 16))
	return nil
}

// ReadConfigFileAsMap reads the config file given a path and converts it into a map[string]interface{}
func ReadConfigFileAsMap(path string) (map[string]interface{}, error) {
	configYML, err := os.ReadFile(path) // #nosec G304 -- this is a trusted etcd config file.
	if err != nil {
		return nil, fmt.Errorf("unable to read etcd config file at path: %s : %v", path, err)
	}

	c := map[string]interface{}{}
	if err := yaml.Unmarshal(configYML, &c); err != nil {
		return nil, fmt.Errorf("unable to unmarshal etcd config yaml file at path: %s : %v", path, err)
	}
	return c, nil
}

// parseAdvertiseURLsConfig reads and parses the config file and returns the advertise-urls config.
func parseAdvertiseURLsConfig(configFile string) (*advertiseURLsConfig, error) {
	var advURLsConfig advertiseURLsConfig
	config, err := os.ReadFile(configFile) // #nosec G304 -- this is a trusted advertise-urls config file.
	if err != nil {
		return nil, fmt.Errorf("unable to read etcd config file at path: %s : %w", configFile, err)
	}

	if err := yaml.Unmarshal(config, &advURLsConfig); err != nil {
		return nil, fmt.Errorf("unable to unmarshal etcd config yaml file at path: %s : %w", configFile, err)
	}
	return &advURLsConfig, nil
}

// GetMemberPeerURLs retrieves the initial advertise peer URLs for the etcd member using the POD_NAME environment variable.
func GetMemberPeerURLs(configFile string) ([]string, error) {
	memberName, err := GetEnvVarOrError("POD_NAME")
	if err != nil {
		return nil, err
	}

	advURLsConfig, err := parseAdvertiseURLsConfig(configFile)
	if err != nil {
		return nil, fmt.Errorf("failed to parse advertise URLs config: %w", err)
	}

	peerURLs, ok := advURLsConfig.InitialAdvertisePeerURLs[memberName]
	if !ok || len(peerURLs) == 0 {
		return nil, fmt.Errorf("no peer URLs found for pod %s", memberName)
	}

	for _, peerURL := range peerURLs {
		if _, err := url.Parse(peerURL); err != nil {
			return nil, fmt.Errorf("invalid peer URL %s: %w", peerURL, err)
		}
	}
	return peerURLs, nil
}

// GetMemberClientURLs retrieves the advertise client URLs for the etcd member using the POD_NAME environment variable.
func GetMemberClientURLs(configFile string) ([]string, error) {
	memberName, err := GetEnvVarOrError("POD_NAME")
	if err != nil {
		return nil, err
	}

	advURLsConfig, err := parseAdvertiseURLsConfig(configFile)
	if err != nil {
		return nil, fmt.Errorf("failed to parse advertise URLs config: %w", err)
	}

	clientURLs, ok := advURLsConfig.AdvertiseClientURLs[memberName]
	if !ok || len(clientURLs) == 0 {
		return nil, fmt.Errorf("no client URLs found for pod %s", memberName)
	}

	for _, clientURL := range clientURLs {
		if _, err := url.Parse(clientURL); err != nil {
			return nil, fmt.Errorf("invalid client URL %s: %w", clientURL, err)
		}
	}
	return clientURLs, nil
}

// IsPeerURLTLSEnabled checks whether all peer URLs are TLS-enabled (i.e., use the "https" scheme).
func IsPeerURLTLSEnabled() (bool, error) {
	memberPeerURLs, err := GetMemberPeerURLs(GetConfigFilePath())
	if err != nil {
		return false, fmt.Errorf("failed to get initial advertise peer URLs: %w", err)
	}

	for _, peerURL := range memberPeerURLs {
		parsedPeerURL, err := url.Parse(peerURL)
		if err != nil {
			return false, fmt.Errorf("failed to parse peer URL %s: %w", peerURL, err)
		}
		if parsedPeerURL.Scheme != https {
			return false, nil
		}
	}

	return true, nil
}

// GetPrevScheduledSnapTime returns the previous schedule snapshot time.
// TODO: Previous full snapshot time should be calculated on basis of previous cron schedule of full snapshot.
func GetPrevScheduledSnapTime(nextSnapSchedule time.Time, timeWindow float64) time.Time {
	return time.Date(
		nextSnapSchedule.Year(),
		nextSnapSchedule.Month(),
		nextSnapSchedule.Day(),
		nextSnapSchedule.Hour()-int(timeWindow),
		nextSnapSchedule.Minute(),
		nextSnapSchedule.Second(),
		nextSnapSchedule.Nanosecond(),
		nextSnapSchedule.Location(),
	)
}

// CreateBackoff returns the backoff with Factor=2 with upper limit of 120sec.
func CreateBackoff(retryPeriod time.Duration, steps int) wait.Backoff {
	return wait.Backoff{
		Duration: retryPeriod,
		Factor:   2,
		Jitter:   retry.DefaultBackoff.Jitter,
		Steps:    steps,
		Cap:      120 * time.Second,
	}
}

// RemoveDir removes the directory(if exist) and do nothing if directory doesn't exist.
func RemoveDir(dir string) error {
	if _, err := os.Stat(dir); err == nil {
		if err := os.RemoveAll(filepath.Join(dir)); err != nil {
			return fmt.Errorf("failed to remove directory %s with err: %v", dir, err)
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	return nil
}

// RestartEtcdWrapper is to call the "/stop" endpoint of etcd-wrapper to restart the etcd-wrapper container.
func RestartEtcdWrapper(ctx context.Context, tlsEnabled bool, etcdConnectionConfig *brtypes.EtcdConnectionConfig) error {
	client := &http.Client{}

	etcdWrapperURL, err := getEtcdWrapperEndpoint(etcdConnectionConfig.Endpoints)
	if err != nil {
		return err
	}

	if tlsEnabled {
		caCertPool := x509.NewCertPool()
		caCert, err := os.ReadFile(etcdConnectionConfig.CaFile)
		if err != nil {
			return err
		}
		caCertPool.AppendCertsFromPEM(caCert)
		client.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{ // #nosec G402 -- TLSClientConfig.MinVersion=1.2 by default.
				RootCAs: caCertPool,
			},
		}
	}

	httpCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	req, err := http.NewRequestWithContext(httpCtx, http.MethodPost, fmt.Sprintf("%s/%s", etcdWrapperURL, "stop"), nil)
	if err != nil {
		return err
	}
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	return nil
}

func getEtcdWrapperEndpoint(etcdEndpoints []string) (string, error) {
	if len(etcdEndpoints) == 0 {
		return "", fmt.Errorf("etcd endpoints are not passed correctly")
	}

	etcdURL, err := url.Parse(etcdEndpoints[0])
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s://%s:%s", etcdURL.Scheme, etcdURL.Hostname(), etcdWrapperPort), nil
}
