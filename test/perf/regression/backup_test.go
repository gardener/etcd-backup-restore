// Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
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

package regression

import (
	"context"
	"path"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
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
		kubeconfigPath := getKubeconfigPath()
		logger.Printf("Setting up target using KUBECONFIG=%s", kubeconfigPath)

		var err error
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
