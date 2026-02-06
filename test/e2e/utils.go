// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/etcdserver/etcdserverpb"
	helmaction "helm.sh/helm/v3/pkg/action"
	helmchart "helm.sh/helm/v3/pkg/chart"
	helmloader "helm.sh/helm/v3/pkg/chart/loader"
	helmkube "helm.sh/helm/v3/pkg/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
)

// SnapListResult stores the snaplist and any associated error
type SnapListResult struct {
	Error     error            `json:"error"`
	Snapshots brtypes.SnapList `json:"snapshots"`
}

// LatestSnapshots stores the result from output of /snapshot/latest http call
type LatestSnapshots struct {
	FullSnapshot   *brtypes.Snapshot   `json:"fullSnapshot"`
	DeltaSnapshots []*brtypes.Snapshot `json:"deltaSnapshots"`
}

const (
	providerAWS   = "aws"
	providerAzure = "azure"
	providerGCP   = "gcp"

	// AWS related variables
	envAWSCreds            = "AWS_APPLICATION_CREDENTIALS" // #nosec G101 -- This is not a hardcoded password, but only the environment variable to the credentials.
	envAWSEndpointOverride = "AWS_ENDPOINT_OVERRIDE"
	fileS3AccessKeyID      = "accessKeyID"
	fileS3SecretAccessKey  = "secretAccessKey"
	fileS3Region           = "region"
	// GCP related variables
	envGoogleCreds            = "GOOGLE_APPLICATION_CREDENTIALS" // #nosec G101 -- This is not a hardcoded password, but only the environment variable to the credentials.
	envGoogleEndpointOverride = "GOOGLE_ENDPOINT_OVERRIDE"
	// Azure related variables
	envAzureCreds            = "AZURE_APPLICATION_CREDENTIALS" // #nosec G101 -- This is not a hardcoded password, but only the environment variable to the credentials.
	envAzureEndpointOverride = "AZURE_ENDPOINT_OVERRIDE"
	fileAzureStorageAccount  = "storageAccount"
	fileAzureStorageKey      = "storageKey"
	// Generic emulator variables
	envEmulatorURL = "EMULATOR_URL"
)

type storage struct {
	secretData       map[string]interface{}
	provider         string
	emulatorURL      string
	endpointOverride string
}

type testProvider struct {
	storage *storage
	name    string
}

func getProvider(providerName string) (testProvider, error) {
	var provider testProvider
	switch providerName {
	case providerAWS:
		awsCredsDir := getEnvOrFallback(envAWSCreds, "")
		if awsCredsDir == "" {
			return testProvider{}, fmt.Errorf("aws credential directory not found")
		}
		s3AccessKeyID := readCredentialFileFromDir(awsCredsDir, fileS3AccessKeyID)
		s3SecretAccessKey := readCredentialFileFromDir(awsCredsDir, fileS3SecretAccessKey)
		s3Region := readCredentialFileFromDir(awsCredsDir, fileS3Region)
		secretData := map[string]interface{}{
			"accessKeyID":     s3AccessKeyID,
			"secretAccessKey": s3SecretAccessKey,
			"region":          s3Region,
		}
		emulatorURL := getEnvOrFallback(envEmulatorURL, "")
		if emulatorURL != "" {
			secretData["s3ForcePathStyle"] = "true"
		}
		endpointOverride := getEnvOrFallback(envAWSEndpointOverride, "")
		provider = testProvider{
			name: "aws",
			storage: &storage{
				provider:         brtypes.SnapstoreProviderS3,
				secretData:       secretData,
				emulatorURL:      emulatorURL,
				endpointOverride: endpointOverride,
			},
		}
	case providerGCP:
		secretData := map[string]interface{}{}
		emulatorURL := getEnvOrFallback(envEmulatorURL, "")
		endpointOverride := getEnvOrFallback(envGoogleEndpointOverride, "")
		if emulatorURL == "" {
			file, err := os.ReadFile(os.Getenv(envGoogleCreds))
			if err != nil {
				return testProvider{}, err
			}
			jsonStr := string(file)
			secretData["serviceAccountJson"] = jsonStr
		} else {
			secretData["serviceAccountJson"] = `{
				"project_id": "dummy-project-id",
				"type": "service_account"
			}`
		}
		provider = testProvider{
			name: "gcp",
			storage: &storage{
				provider:         brtypes.SnapstoreProviderGCS,
				secretData:       secretData,
				emulatorURL:      emulatorURL,
				endpointOverride: endpointOverride,
			},
		}
	case providerAzure:
		azureCredsDir := getEnvOrFallback(envAzureCreds, "")
		if azureCredsDir == "" {
			return testProvider{}, fmt.Errorf("azure credential directory not found")
		}
		azureStorageAccount := readCredentialFileFromDir(azureCredsDir, fileAzureStorageAccount)
		azureStorageKey := readCredentialFileFromDir(azureCredsDir, fileAzureStorageKey)
		secretData := map[string]interface{}{
			"storageAccount": azureStorageAccount,
			"storageKey":     azureStorageKey,
		}
		emulatorURL := getEnvOrFallback(envEmulatorURL, "")
		endpointOverride := getEnvOrFallback(envAzureEndpointOverride, "")
		provider = testProvider{
			name: "azure",
			storage: &storage{
				provider:         brtypes.SnapstoreProviderABS,
				secretData:       secretData,
				emulatorURL:      emulatorURL,
				endpointOverride: endpointOverride,
			},
		}
	default:
		return testProvider{}, fmt.Errorf("unsupported provider: %s", providerName)
	}
	return provider, nil
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

func readCredentialFileFromDir(dir string, filename string) string {
	file, err := os.ReadFile(fmt.Sprintf("%s/%s", dir, filename))
	if err != nil {
		return ""
	}
	return string(file)
}

func getKubernetesTypedClient(kubeconfigPath string) (*kubernetes.Clientset, error) {
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
	var (
		chart *helmchart.Chart
		err   error
	)

	chart, err = helmloader.Load(chartPath)
	if err != nil {
		return err
	}

	actionConfig := new(helmaction.Configuration)
	if err = actionConfig.Init(helmkube.GetConfig(kubeconfigPath, "", releaseNamespace), releaseNamespace, "configmap", func(_ string, _ ...interface{}) {}); err != nil {
		return err
	}

	installClient := helmaction.NewInstall(actionConfig)
	installClient.Namespace = releaseNamespace
	installClient.ReleaseName = releaseName
	installClient.Wait = waitForResourcesToBeReady
	installClient.Timeout = timeout

	rel, err := installClient.Run(chart, chartValues)
	if err != nil {
		logger.Infof("failed to install chart %s: %v", chartPath, err)
		return err
	}

	logger.Infof("successfully installed chart %s with release name %s", chartPath, rel.Name)
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
			if !isFound {
				continue OuterLoop
			}
		}
		break
	}

	return nil
}

func installEtcdctl(kubeconfigPath, namespace, podName, containerName string) error {
	cmd := "cd /nonroot/hacks && ./install_etcdctl"
	_, _, err := executeContainerCommand(kubeconfigPath, namespace, podName, containerName, cmd)
	if err != nil {
		return err
	}
	return nil
}

// attachEphemeralContainer attaches the debug container to the Etcd pod targetting the etcd container to debug and/or execute commands
func attachEphemeralContainer(kubeconfigPath, namespace, podName, debugContainerName, targetContainer string) error {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return err
	}

	clientSet, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}

	pod, err := clientSet.CoreV1().Pods(namespace).Get(context.Background(), podName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	if pod.Spec.EphemeralContainers == nil {
		pod.Spec.EphemeralContainers = make([]corev1.EphemeralContainer, 0)
	} else {
		for _, container := range pod.Spec.EphemeralContainers {
			if container.Name == debugContainerName {
				// ephemeral container already exists
				for _, ephContainerStatus := range pod.Status.EphemeralContainerStatuses {
					if ephContainerStatus.Name == debugContainerName {
						if ephContainerStatus.State.Running != nil {
							return nil
						}
					}
				}
				return fmt.Errorf("ephemeral container %s already exists, but not running", debugContainerName)
			}
		}
	}

	log.Printf("Creating ephemeral container %s\n", debugContainerName)
	pod.Spec.EphemeralContainers = append(pod.Spec.EphemeralContainers, corev1.EphemeralContainer{
		TargetContainerName: targetContainer,
		EphemeralContainerCommon: corev1.EphemeralContainerCommon{
			Image:           "europe-docker.pkg.dev/sap-se-gcp-k8s-delivery/releases-public/eu_gcr_io/gardener-project/gardener/ops-toolbelt:0.25.0-mod1",
			Name:            debugContainerName,
			ImagePullPolicy: "IfNotPresent",
			Command:         []string{"/bin/bash", "-c", "--"},
			Args:            []string{"trap : TERM INT; sleep 9999999999d & wait"},
		},
	})
	_, err = clientSet.CoreV1().Pods(namespace).UpdateEphemeralContainers(context.Background(), podName, pod, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("unable to create ephemeral container: %v", err)
	}
	time.Sleep(1 * time.Minute)
	for _, ephContainerStatus := range pod.Status.EphemeralContainerStatuses {
		if ephContainerStatus.Name == debugContainerName {
			if ephContainerStatus.State.Running == nil {
				return fmt.Errorf("ephemeral container %s not running", debugContainerName)
			}
		}
	}
	return nil
}

// executeContainerCommand executes a remote shell command on the given pod and container
// and returns the stdout and stderr logs
func executeContainerCommand(kubeconfigPath, podNamespace, podName, containerName, command string) (string, string, error) {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return "", "", err
	}

	clientSet, err := kubernetes.NewForConfig(config)
	if err != nil {
		return "", "", err
	}
	req := clientSet.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(podNamespace).
		SubResource("exec").
		Param("container", containerName).
		VersionedParams(&corev1.PodExecOptions{
			Command: []string{"/bin/bash", "-c", command},
			Stdin:   false,
			Stdout:  true,
			Stderr:  true,
			TTY:     false,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(config, "POST", req.URL())
	if err != nil {
		return "", "", err
	}

	var stdout, stderr bytes.Buffer
	err = exec.StreamWithContext(context.Background(), remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if err != nil {
		return "", "", err
	}

	return strings.TrimSpace(stdout.String()), strings.TrimSpace(stderr.String()), nil
}

func getSnapstore(storageProvider, storageContainer, storePrefix, endpoint string) (brtypes.SnapStore, error) {
	snapstoreConfig := &brtypes.SnapstoreConfig{
		Provider:         storageProvider,
		Container:        storageContainer,
		Prefix:           storePrefix,
		EndpointOverride: endpoint,
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
		switch snap.Kind {
		case brtypes.SnapshotKindFull:
			numFulls++
		case brtypes.SnapshotKindDelta:
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
			cmd = fmt.Sprintf("ETCDCTL_API=3 ./nonroot/hacks/etcdctl put foo-%d bar-%d", i, i)
			stdout, stderr, err = executeContainerCommand(kubeconfigPath, namespace, podName, containerName, cmd)
			if err != nil || stderr != "" || stdout != "OK" {
				logger.Infof("failed to put (foo-%d, bar-%d). Retrying", i, i)
				continue
			}
			if i%10 == 0 {
				logger.Infof("put (foo-%d, bar-%d) successful", i, i)
				cmd = fmt.Sprintf("ETCDCTL_API=3 ./nonroot/hacks/etcdctl del foo-%d", i)
				stdout, stderr, err = executeContainerCommand(kubeconfigPath, namespace, podName, containerName, cmd)
				if err != nil || stderr != "" || stdout != "1" {
					logger.Infof("failed to delete (foo-%d, bar-%d). Retrying", i, i)
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

type endpointStatusResult struct {
	Endpoint string                      `json:"Endpoint"`
	Status   etcdserverpb.StatusResponse `json:"Status"`
}

func getDbSizeAndRevision(kubeconfigPath, namespace, podName, containerName string) (int64, int64, error) {
	cmd := "ETCDCTL_API=3 ./nonroot/hacks/etcdctl endpoint status -w=json"
	stdout, stderr, err := executeContainerCommand(kubeconfigPath, namespace, podName, containerName, cmd)
	if err != nil {
		return 0, 0, err
	}
	if stderr != "" {
		return 0, 0, fmt.Errorf("stderr: %s", stderr)
	}
	var endpointStatusResults []endpointStatusResult
	if err = json.Unmarshal([]byte(stdout), &endpointStatusResults); err != nil {
		return 0, 0, err
	}
	statusResponse := endpointStatusResults[0].Status
	dbSize := statusResponse.DbSize
	revision := statusResponse.Header.Revision
	return dbSize, revision, nil
}

func triggerOnDemandSnapshot(kubeconfigPath, namespace, podName, containerName string, port int, snapshotKind string) (*brtypes.Snapshot, error) {
	var snapshot *brtypes.Snapshot
	cmd := fmt.Sprintf("curl http://localhost:%d/snapshot/%s -s", port, snapshotKind)
	stdout, _, err := executeContainerCommand(kubeconfigPath, namespace, podName, containerName, cmd)
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
	stdout, _, err := executeContainerCommand(kubeconfigPath, namespace, podName, containerName, cmd)
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
