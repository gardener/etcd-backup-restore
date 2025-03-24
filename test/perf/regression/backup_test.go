// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package regression

import (
	"context"
	"path"
	"time"

	"github.com/sirupsen/logrus"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	envSourcePath                = "SOURCE_PATH"
	envTargetTimeoutDuration     = "TARGET_TIMEOUT_DURATION"
	defaultTargetTimeoutDuration = "660s"
	envEtcdImage                 = "ETCD_IMAGE"
	envEtcdbrImage               = "ETCDBR_IMAGE"
	resourcesBasePath            = "test/perf/regression/resources"
)

func newLogger(purpose string) *logrus.Logger {
	logger := logrus.New()
	logger.SetOutput(GinkgoWriter)
	return logger
}

var _ = Describe("Backup", func() {
	var (
		t       *target
		timeout time.Duration
	)

	BeforeEach(func() {
		logger := newLogger("BeforeEach")
		kubeconfigPath, err := getKubeconfigPath()
		Expect(err).To(BeNil())
		logger.Printf("Setting up target using KUBECONFIG=%s", kubeconfigPath)

		config, err := getKubeconfig(kubeconfigPath)
		Expect(err).To(BeNil())

		typedClient, err := kubernetes.NewForConfig(config)
		Expect(err).To(BeNil())

		discoveryClient, err := discovery.NewDiscoveryClientForConfig(config)
		Expect(err).To(BeNil())

		untypedClient, err := dynamic.NewForConfig(config)
		Expect(err).To(BeNil())

		timeout, err = time.ParseDuration(getEnvOrFallback(envTargetTimeoutDuration, defaultTargetTimeoutDuration))

		sourcePath := getEnvOrFallback(envSourcePath, ".")
		resourcePaths := []string{
			path.Join(sourcePath, resourcesBasePath, "etcd", "configmap.yaml"),
			path.Join(sourcePath, resourcesBasePath, "etcd", "pvc.yaml"),
			path.Join(sourcePath, resourcesBasePath, "etcd", "pod.yaml"),
			path.Join(sourcePath, resourcesBasePath, "etcd", "service.yaml"),
			path.Join(sourcePath, resourcesBasePath, "loadtest", "configmap.yaml"),
			path.Join(sourcePath, resourcesBasePath, "loadtest", "job.yaml"),
		}

		t = &target{
			typedClient:     typedClient,
			discoveryClient: discoveryClient,
			untypedClient:   untypedClient,
			etcdImage:       getEnvOrFallback(envEtcdImage, ""),
			etcdbrImage:     getEnvOrFallback(envEtcdbrImage, ""),
			namespacePrefix: "etcdbr-",
			resourcePaths:   resourcePaths,
			logger:          newLogger("target"),
		}

		err = t.setup()
		t.logger.Infof("setup returned %s", err)
		Expect(err).To(BeNil())
	})

	AfterEach(func() {
		t.teardown()
	})

	Describe("Workload", func() {
		It("Should complete", func() {
			Expect(t).ToNot(BeNil())

			ctx, cancelContext := context.WithTimeout(context.Background(), timeout)
			defer cancelContext()

			readyCh := make(chan interface{})
			go t.watchForJob(ctx, "name=loadtest", readyCh)
			Eventually(readyCh, 10*time.Minute).Should(BeClosed())

			running, err := t.isPodRunning("name=etcd")
			Expect(err).To(BeNil())
			Expect(running).To(BeTrue())
		})
	})
})
