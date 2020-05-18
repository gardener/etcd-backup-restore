// Copyright (c) 2020 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package integrationcluster

import (
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	. "github.com/onsi/ginkgo"
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
	ns, err := namespacesClient.Create(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: releaseNamespace,
		},
	})
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
	err = namespacesClient.Delete(releaseNamespace, &metav1.DeleteOptions{})
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
