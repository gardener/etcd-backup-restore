// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package e2e

import (
	"context"
	"fmt"
	"path"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	envSourcePath          = "SOURCE_PATH"
	envKubeconfigPath      = "KUBECONFIG"
	envEtcdbrVersion       = "ETCDBR_VERSION"
	envEtcdbrImage         = "ETCDBR_IMAGE"
	envEtcdWrapperVersion  = "ETCD_WRAPPER_VERSION"
	envEtcdWrapperImage    = "ETCD_WRAPPER_IMAGE"
	helmChartBasePath      = "chart/etcd-backup-restore"
	releaseNamePrefix      = "main"
	releaseNamespacePrefix = "e2e-test"
	envStorageContainer    = "STORAGE_CONTAINER"
	envProvider            = "PROVIDER"
	debugContainerName     = "debug"
	timeoutPeriod          = 5 * time.Minute
	etcdClientPort         = 2379
	backupClientPort       = 8080
)

var (
	logger           = logrus.New()
	kubeconfigPath   string
	typedClient      *kubernetes.Clientset
	storageContainer string
	storageProvider  string
	releaseNamespace string
	providerName     string
	storePrefix      = fmt.Sprintf("%s-etcd", releaseNamePrefix)
)

func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "E2E Suite")
}

var _ = BeforeSuite(func() {
	var (
		etcdbrVersion      = getEnvAndExpectNoError(envEtcdbrVersion)
		etcdbrImage        = getEnvAndExpectNoError(envEtcdbrImage)
		etcdWrapperVersion = getEnvAndExpectNoError(envEtcdWrapperVersion)
		etcdWrapperImage   = getEnvAndExpectNoError(envEtcdWrapperImage)
		chartPath          = path.Join(getEnvOrFallback(envSourcePath, "."), helmChartBasePath)
		chartValues        map[string]interface{}
	)
	storageContainer = getEnvAndExpectNoError(envStorageContainer)
	kubeconfigPath = getEnvAndExpectNoError(envKubeconfigPath)
	Expect(kubeconfigPath).ShouldNot(BeEmpty())

	providerName = getEnvAndExpectNoError(envProvider)
	provider, err := getProvider(providerName)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(provider).ShouldNot(BeNil())

	logger.Infof("provider: %s", provider.name)
	storageProvider = provider.storage.provider
	store, err := getSnapstore(storageProvider, storageContainer, storePrefix)
	Expect(err).ShouldNot(HaveOccurred())

	// purge any existing backups in bucket
	err = purgeSnapstore(store)
	Expect(err).ShouldNot(HaveOccurred())

	logger.Printf("setting up k8s client using KUBECONFIG=%s", kubeconfigPath)
	typedClient, err = getKubernetesTypedClient(kubeconfigPath)
	Expect(err).NotTo(HaveOccurred())
	namespacesClient := typedClient.CoreV1().Namespaces()
	releaseNamespace = fmt.Sprintf("%s-%s", releaseNamespacePrefix, providerName)
	logger.Infof("creating namespace %s", releaseNamespace)
	ns, err := namespacesClient.Create(context.TODO(), &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: releaseNamespace,
		},
	}, metav1.CreateOptions{})
	Expect(err).NotTo(HaveOccurred())

	err = waitForNamespaceToBeCreated(typedClient, ns.Name)
	Expect(err).NotTo(HaveOccurred())

	logger.Infof("created namespace %s", ns.Name)

	chartValues = map[string]interface{}{
		"images": map[string]interface{}{
			"etcdWrapper": map[string]interface{}{
				"repository": etcdWrapperImage,
				"tag":        etcdWrapperVersion,
				"pullPolicy": "IfNotPresent",
			},
			"etcdBackupRestore": map[string]interface{}{
				"repository": etcdbrImage,
				"tag":        etcdbrVersion,
				"pullPolicy": "Never",
			},
		},
		"backup": map[string]interface{}{
			"storageProvider":                storageProvider,
			"storageContainer":               storageContainer,
			strings.ToLower(storageProvider): provider.storage.secretData,
			"schedule":                       "*/1 * * * *",
			"deltaSnapshotPeriod":            "10s",
			"maxBackups":                     2,
			"garbageCollectionPolicy":        "LimitBased",
			"garbageCollectionPeriod":        "30s",
			"defragmentationSchedule":        "*/1 * * * *",
		},
	}
	err = helmDeployChart(logger, timeoutPeriod, kubeconfigPath, chartPath, fmt.Sprintf("%s-%s", releaseNamePrefix, providerName), releaseNamespace, chartValues, true)
	if err != nil {
		fmt.Printf("error deploying helm chart for provider %s: %v\n", providerName, err)
	}
	Expect(err).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	kubeconfigPath, err := getEnvOrError(envKubeconfigPath)
	Expect(err).NotTo(HaveOccurred())
	logger.Printf("setting up k8s client using KUBECONFIG=%s", kubeconfigPath)

	typedClient, err = getKubernetesTypedClient(kubeconfigPath)
	Expect(err).NotTo(HaveOccurred())
	logger.Infof("deleting namespace %s", releaseNamespace)
	namespacesClient := typedClient.CoreV1().Namespaces()
	err = namespacesClient.Delete(context.TODO(), releaseNamespace, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		logger.Infof("namespace %s does not exist", releaseNamespace)
	} else {
		Expect(err).NotTo(HaveOccurred())
	}

	err = waitForNamespaceToBeDeleted(typedClient, releaseNamespace)
	Expect(err).NotTo(HaveOccurred())

	logger.Infof("deleted namespace %s", releaseNamespace)
})

func getEnvAndExpectNoError(key string) string {
	val, err := getEnvOrError(key)
	Expect(err).NotTo(HaveOccurred())
	return val
}
