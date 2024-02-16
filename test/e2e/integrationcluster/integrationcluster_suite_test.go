// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package integrationcluster

import (
	"context"
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	envSourcePath       = "SOURCE_PATH"
	envKubeconfigPath   = "KUBECONFIG"
	envEtcdVersion      = "ETCD_VERSION"
	envEtcdbrVersion    = "ETCDBR_VERSION"
	helmChartBasePath   = "chart/etcd-backup-restore"
	releaseName         = "main"
	releaseNamespace    = "integration-test"
	envStorageContainer = "STORAGE_CONTAINER"
	envAccessKeyID      = "ACCESS_KEY_ID"
	envSecretAccessKey  = "SECRET_ACCESS_KEY"
	envRegion           = "REGION"
	timeoutPeriod       = time.Second * 60
	etcdClientPort      = 2379
	backupClientPort    = 8080
)

var (
	logger           = logrus.New()
	err              error
	kubeconfigPath   string
	typedClient      *kubernetes.Clientset
	storageContainer string
	storageProvider  string
	storePrefix      = fmt.Sprintf("%s-etcd", releaseName)
)

func TestIntegrationcluster(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Integrationcluster Suite")
}

var _ = BeforeSuite(func() {
	var (
		etcdVersion     = getEnvAndExpectNoError(envEtcdVersion)
		etcdbrVersion   = getEnvAndExpectNoError(envEtcdbrVersion)
		region          = getEnvAndExpectNoError(envRegion)
		secretAccessKey = getEnvAndExpectNoError(envSecretAccessKey)
		accessKeyID     = getEnvAndExpectNoError(envAccessKeyID)
		chartValues     map[string]interface{}
	)

	kubeconfigPath = getEnvAndExpectNoError(envKubeconfigPath)
	storageContainer = getEnvAndExpectNoError(envStorageContainer)
	storageProvider = "S3" // support for only S3 buckets at the moment

	store, err := getSnapstore(storageProvider, storageContainer, storePrefix)
	Expect(err).ShouldNot(HaveOccurred())

	// purge any existing backups in bucket
	err = purgeSnapstore(store)
	Expect(err).ShouldNot(HaveOccurred())

	logger.Printf("setting up k8s client using KUBECONFIG=%s", kubeconfigPath)
	typedClient, err = getKubernetesTypedClient(logger, kubeconfigPath)
	Expect(err).NotTo(HaveOccurred())
	namespacesClient := typedClient.CoreV1().Namespaces()

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

	chartPath := path.Join(getEnvOrFallback(envSourcePath, "."), helmChartBasePath)

	logger.Infof("deploying helm chart at %s", chartPath)

	chartValues = map[string]interface{}{
		"images": map[string]interface{}{
			"etcd": map[string]interface{}{
				"tag": etcdVersion,
			},
			"etcdBackupRestore": map[string]interface{}{
				"tag": etcdbrVersion,
			},
		},
		"backup": map[string]interface{}{
			"storageProvider":  storageProvider,
			"storageContainer": storageContainer,
			"s3": map[string]interface{}{
				"region":          region,
				"secretAccessKey": secretAccessKey,
				"accessKeyID":     accessKeyID,
			},
			"schedule":                "*/1 * * * *",
			"deltaSnapshotPeriod":     "10s",
			"maxBackups":              2,
			"garbageCollectionPolicy": "LimitBased",
			"garbageCollectionPeriod": "30s",
			"defragmentationSchedule": "*/1 * * * *",
		},
	}

	err = helmDeployChart(logger, timeoutPeriod, kubeconfigPath, chartPath, releaseName, releaseNamespace, chartValues, true)
	Expect(err).NotTo(HaveOccurred())
	logger.Infof("deployed helm chart to release '%s'", releaseName)
})

var _ = AfterSuite(func() {
	kubeconfigPath, err := getEnvOrError(envKubeconfigPath)
	Expect(err).NotTo(HaveOccurred())
	logger.Printf("setting up k8s client using KUBECONFIG=%s", kubeconfigPath)

	typedClient, err = getKubernetesTypedClient(logger, kubeconfigPath)
	Expect(err).NotTo(HaveOccurred())
	namespacesClient := typedClient.CoreV1().Namespaces()

	logger.Infof("deleting namespace %s", releaseNamespace)
	namespacesClient = typedClient.CoreV1().Namespaces()
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
