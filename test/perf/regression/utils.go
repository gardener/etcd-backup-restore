// SPDX-FileCopyrightText: Contributors to the Gardener project
//
// SPDX-License-Identifier: Apache-2.0

package regression

import (
	"fmt"
	"os"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	envKubeconfig = "KUBECONFIG"
)

func getKubeconfigPath() (string, error) {
	if value, ok := os.LookupEnv(envKubeconfig); ok {
		return value, nil
	}
	return "", fmt.Errorf("KUBECONFIG ENV is not set")
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
