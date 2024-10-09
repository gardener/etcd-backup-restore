// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package integrationcluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"sigs.k8s.io/yaml"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/helm/pkg/chartutil"
	"k8s.io/helm/pkg/helm"
)

// EndpointStatus stores result from output of etcdctl endpoint status command
type EndpointStatus []struct {
	Endpoint string `json:"Endpoint"`
	Status   struct {
		Header struct {
			ClusterID int64 `json:"cluster_id"`
			MemberID  int64 `json:"member_id"`
			Revision  int64 `json:"revision"`
			RaftTerm  int64 `json:"raft_term"`
		} `json:"header"`
		Version   string `json:"version"`
		DbSize    int64  `json:"dbSize"`
		Leader    int64  `json:"leader"`
		RaftIndex int64  `json:"raftIndex"`
		RaftTerm  int64  `json:"raftTerm"`
	} `json:"Status"`
}

// SnapListResult stores the snaplist and any associated error
type SnapListResult struct {
	Snapshots brtypes.SnapList `json:"snapshots"`
	Error     error            `json:"error"`
}

// LatestSnapshots stores the result from output of /snapshot/latest http call
type LatestSnapshots struct {
	FullSnapshot   *brtypes.Snapshot   `json:"fullSnapshot"`
	DeltaSnapshots []*brtypes.Snapshot `json:"deltaSnapshots"`
}

func getEnvOrError(key string) (string, error) {
	if value, ok := os.LookupEnv(key); ok {
		return value, nil
	}

	return "", fmt.Errorf("environment variable not found: %s", key)
}

func getKubeconfig(kubeconfigPath string) (*rest.Config, error) {
	return clientcmd.BuildConfigFromFlags("", kubeconfigPath)
}

func getEnvOrFallback(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}

	return fallback
}

func getKubernetesTypedClient(logger *logrus.Logger, kubeconfigPath string) (*kubernetes.Clientset, error) {
	config, err := getKubeconfig(kubeconfigPath)
	if err != nil {
		return nil, err
	}

	typedClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	return typedClient, nil
}

func helmDeployChart(logger *logrus.Logger, timeout time.Duration, kubeconfigPath, chartPath, releaseName, releaseNamespace string, chartValues map[string]interface{}, waitForResourcesToBeReady bool) error {
	client := helm.NewClient(helm.Host(kubeconfigPath))

	chartRequested, err := chartutil.Load(chartPath)
	if err != nil {
		return err
	}

	valuesToRender, err := yaml.Marshal(chartValues)
	if err != nil {
		return fmt.Errorf("failed to marshal values: %v", err)
	}

	// Install the chart
	_, err = client.InstallReleaseFromChart(
		chartRequested,
		releaseNamespace,
		helm.ValueOverrides(valuesToRender),
		helm.ReleaseName(releaseName),
		helm.InstallWait(waitForResourcesToBeReady),
	)

	if err != nil {
		logger.Infof("helm chart installation to release '%s' failed", releaseName)
		return err
	}

	logger.Infof("successfully installed release '%s'", releaseName)
	return nil
}

func waitForNamespaceToBeCreated(k8sClient *kubernetes.Clientset, name string) error {
	var (
		err error
	)
	namespaceClient := k8sClient.CoreV1().Namespaces()

	for {
		_, err = namespaceClient.Get(context.TODO(), name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			continue
		} else if err != nil {
			return err
		}
		break
	}

	return nil
}

func waitForNamespaceToBeDeleted(k8sClient *kubernetes.Clientset, name string) error {
	var (
		err error
	)
	namespaceClient := k8sClient.CoreV1().Namespaces()

	for {
		_, err = namespaceClient.Get(context.TODO(), name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			break
		} else if err != nil {
			return err
		}
	}

	return nil
}

func waitForPodToBeRunning(k8sClient *kubernetes.Clientset, name, namespace string) error {
	var (
		pod *corev1.Pod
		err error
	)
	podClient := k8sClient.CoreV1().Pods(namespace)

	for {
		pod, err = podClient.Get(context.TODO(), name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			continue
		} else if err != nil {
			return err
		}
		if pod.Status.Phase == corev1.PodRunning {
			break
		}
	}

	return nil
}

func waitForEndpointPortsToBeReady(k8sClient *kubernetes.Clientset, name, namespace string, ports []int32) error {
	var (
		endpoint *corev1.Endpoints
		err      error
	)
	endpointClient := k8sClient.CoreV1().Endpoints(namespace)
	portsMap := make(map[int32]bool)

OuterLoop:
	for {
		endpoint, err = endpointClient.Get(context.TODO(), name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			continue
		} else if err != nil {
			return err
		}
		for _, port := range ports {
			portsMap[port] = false
		}
		for _, subset := range endpoint.Subsets {
			if len(subset.NotReadyAddresses) != 0 {
				continue OuterLoop
			}
			for _, port := range subset.Ports {
				if _, ok := portsMap[port.Port]; ok {
					portsMap[port.Port] = true
				}
			}
		}
		for _, isFound := range portsMap {
			if isFound == false {
				continue OuterLoop
			}
		}
		break
	}

	return nil
}

// getRemoteCommandExecutor builds and returns a remote command Executor from the given command on the specified container
func getRemoteCommandExecutor(kubeconfigPath, namespace, podName, containerName, command string) (remotecommand.Executor, error) {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return nil, err
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	coreClient := clientset.CoreV1()

	req := coreClient.RESTClient().
		Post().
		Namespace(namespace).
		Resource("pods").
		Name(podName).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: containerName,
			Command: []string{
				"/bin/sh",
				"-c",
				command,
			},
			Stdin:  false,
			Stdout: true,
			Stderr: true,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(config, "POST", req.URL())
	if err != nil {
		return nil, err
	}

	return exec, nil
}

// executeRemoteCommand executes a remote shell command on the given pod and container
// and returns the stdout and stderr logs
func executeRemoteCommand(kubeconfigPath, namespace, podName, containerName, command string) (string, string, error) {
	exec, err := getRemoteCommandExecutor(kubeconfigPath, namespace, podName, containerName, command)
	if err != nil {
		return "", "", err
	}

	buf := &bytes.Buffer{}
	errBuf := &bytes.Buffer{}
	err = exec.Stream(remotecommand.StreamOptions{
		Stdout: buf,
		Stderr: errBuf,
	})
	if err != nil {
		return "", "", err
	}

	return strings.TrimSpace(buf.String()), strings.TrimSpace(errBuf.String()), nil
}

func getSnapstore(storageProvider, storageContainer, storePrefix string) (brtypes.SnapStore, error) {
	snapstoreConfig := &brtypes.SnapstoreConfig{
		Provider:  storageProvider,
		Container: storageContainer,
		Prefix:    storePrefix,
	}
	store, err := snapstore.GetSnapstore(snapstoreConfig)
	if err != nil {
		return nil, err
	}

	return store, nil
}

func getTotalFullAndDeltaSnapshotCounts(snapList brtypes.SnapList) (int, int) {
	var numFulls, numDeltas int
	for _, snap := range snapList {
		if snap.Kind == brtypes.SnapshotKindFull {
			numFulls++
		} else if snap.Kind == brtypes.SnapshotKindDelta {
			numDeltas++
		}
	}
	return numFulls, numDeltas
}

func purgeSnapstore(store brtypes.SnapStore) error {
	snapList, err := store.List(false)
	if err != nil {
		return err
	}

	for _, snap := range snapList {
		err = store.Delete(*snap)
		if err != nil {
			return err
		}
	}

	return nil
}

func runEtcdPopulatorWithoutError(logger *logrus.Logger, stopCh <-chan struct{}, doneCh chan<- struct{}, kubeconfigPath, namespace, podName, containerName string) {
	var (
		cmd    string
		stdout string
		stderr string
		err    error
		i      = 1
	)

	for {
		select {
		case <-stopCh:
			logger.Infof("populator received stop channel. Populated etcd till (foo-%d, bar-%d). Exiting", i-1, i-1)
			close(doneCh)
			return
		default:
			cmd = fmt.Sprintf("ETCDCTL_API=3 etcdctl put foo-%d bar-%d", i, i)
			stdout, stderr, err = executeRemoteCommand(kubeconfigPath, namespace, podName, containerName, cmd)
			if err != nil || stderr != "" || stdout != "OK" {
				logger.Infof("failed to put (foo-%d, bar-%d). Retrying", i, i)
				continue
			}
			if i%10 == 0 {
				logger.Infof("put (foo-%d, bar-%d) successful", i, i)
				cmd = fmt.Sprintf("ETCDCTL_API=3 etcdctl del foo-%d", i)
				stdout, stderr, err = executeRemoteCommand(kubeconfigPath, namespace, podName, containerName, cmd)
				if err != nil || stderr != "" || stdout != "1" {
					logger.Infof("failed to put (foo-%d, bar-%d). Retrying", i, i)
					continue
				}
			}
			i++
			time.Sleep(time.Duration(time.Millisecond * 500))
		}
	}
}

func recordCumulativeSnapList(logger *logrus.Logger, stopCh <-chan struct{}, resultCh chan<- SnapListResult, store brtypes.SnapStore) {
	var snapListResult SnapListResult

	for {
		select {
		case <-stopCh:
			logger.Infof("recorder received stop channel. Exiting")
			resultCh <- snapListResult
			return
		default:
			snaps, err := store.List(false)
			if err != nil {
				snapListResult.Error = err
				resultCh <- snapListResult
				return
			}

			for _, snap := range snaps {
				if snapshotInSnapList(snap, snapListResult.Snapshots) {
					continue
				}
				snapListResult.Snapshots = append(snapListResult.Snapshots, snap)
			}

			time.Sleep(time.Duration(time.Second))
		}
	}
}

func snapshotInSnapList(snapshot *brtypes.Snapshot, snapList brtypes.SnapList) bool {
	for _, snap := range snapList {
		if snap.SnapName == snapshot.SnapName {
			return true
		}
	}
	return false
}

func getDbSizeAndRevision(kubeconfigPath, namespace, podName, containerName string) (int64, int64, error) {
	var endpointStatus EndpointStatus
	cmd := "ETCDCTL_API=3 etcdctl endpoint status -w=json"
	stdout, stderr, err := executeRemoteCommand(kubeconfigPath, namespace, podName, "etcd", cmd)
	if err != nil {
		return 0, 0, err
	}
	if stderr != "" {
		return 0, 0, fmt.Errorf("stderr: %s", stderr)
	}
	if err = json.Unmarshal([]byte(stdout), &endpointStatus); err != nil {
		return 0, 0, err
	}
	dbSize := endpointStatus[0].Status.DbSize
	revision := endpointStatus[0].Status.Header.Revision

	return dbSize, revision, nil
}

func triggerOnDemandSnapshot(kubeconfigPath, namespace, podName, containerName string, port int, snapshotKind string) (*brtypes.Snapshot, error) {
	var snapshot *brtypes.Snapshot
	cmd := fmt.Sprintf("curl http://localhost:%d/snapshot/%s -s", port, snapshotKind)
	stdout, _, err := executeRemoteCommand(kubeconfigPath, namespace, podName, containerName, cmd)
	if err != nil {
		return nil, err
	}
	if stdout == "" {
		return nil, fmt.Errorf("empty response")
	}
	if err = json.Unmarshal([]byte(stdout), &snapshot); err != nil {
		return nil, err
	}

	return snapshot, nil
}

func getLatestSnapshots(kubeconfigPath, namespace, podName, containerName string, port int) (*LatestSnapshots, error) {
	var latestSnapshots *LatestSnapshots
	cmd := fmt.Sprintf("curl http://localhost:%d/snapshot/latest -s", port)
	stdout, _, err := executeRemoteCommand(kubeconfigPath, namespace, podName, containerName, cmd)
	if err != nil {
		return nil, err
	}
	if stdout == "" {
		return nil, fmt.Errorf("empty response")
	}
	if err = json.Unmarshal([]byte(stdout), &latestSnapshots); err != nil {
		return nil, err
	}

	latestSnapshots.FullSnapshot.CreatedOn = latestSnapshots.FullSnapshot.CreatedOn.Truncate(time.Second)
	for _, snap := range latestSnapshots.DeltaSnapshots {
		snap.CreatedOn = snap.CreatedOn.Truncate(time.Second)
	}

	return latestSnapshots, nil
}
