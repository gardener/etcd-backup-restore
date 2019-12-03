// Copyright (c) 2018 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
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

package cmd

import (
	"bufio"
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"os"
	"strings"

	"github.com/ghodss/yaml"
	"github.com/spf13/cobra"
)

// NewServerCommand create cobra command for snapshot
func NewServerCommand(ctx context.Context) *cobra.Command {
	opts := newServerOptions()
	var serverCmd = &cobra.Command{
		Use:   "server",
		Short: "start the http server with backup scheduler.",
		Long:  `Server will keep listening for http request to deliver its functionality through http endpoints.`,
		Run: func(cmd *cobra.Command, args []string) {
			printVersionInfo()

			if err := opts.loadConfigFromFile(); err != nil {
				opts.Logger.Fatalf("failed to load the config from file: %v", err)
				return
			}

			if err := opts.validate(); err != nil {
				opts.Logger.Fatalf("failed to validate the options: %v", err)
				return
			}

			opts.complete()

			optsJSON, err := yaml.Marshal(opts.Config)
			if err != nil {
				opts.Logger.Fatalf("failed to print the options: %v", err)
				return
			}
			opts.Logger.Infof("Config: \n%s", optsJSON)

			// id, err := determineIdentity()
			// if err != nil {
			// 	logger.Fatalf("failed to compute the identity: %v", err)
			// }

			if err := opts.run(ctx); err != nil {
				opts.Logger.Fatalf("failed to run server: %v", err)
			}
		},
	}

	opts.addFlags(serverCmd.Flags())
	return serverCmd
}

// We want to determine the Docker container id of the currently running Gardener controller manager because
// we need to identify for still ongoing operations whether another Gardener controller manager instance is
// still operating the respective Shoots. When running locally, we generate a random string because
// there is no container id.
func determineIdentity() (string, error) {
	var (
		id  string
		err error
	)

	// If running inside a Kubernetes cluster (as container) we can read the container id from the proc file system.
	// Otherwise generate a random string for the gardenerID
	if cGroupFile, err := os.Open("/proc/self/cgroup"); err == nil {
		defer cGroupFile.Close()
		reader := bufio.NewReaderSize(cGroupFile, 4096)
		line, con, err := reader.ReadLine()
		if !con && err == nil {
			splitBySlash := strings.Split(string(line), "/")
			id = splitBySlash[len(splitBySlash)-1]
			return id, nil
		}
	}

	id, err = generateRandomString(64)
	if err != nil {
		return "", fmt.Errorf("unable to generate ID: %v", err)
	}

	return id, nil
}

// generateRandomString uses crypto/rand to generate a random string of the specified length <n>.
// The set of allowed characters is [0-9a-zA-Z], thus no special characters are included in the output.
// Returns error if there was a problem during the random generation.
func generateRandomString(n int) (string, error) {
	allowedCharacters := "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	return generateRandomStringFromCharset(n, allowedCharacters)
}

// generateRandomStringFromCharset generates a cryptographically secure random string of the specified length <n>.
// The set of allowed characters can be specified. Returns error if there was a problem during the random generation.
func generateRandomStringFromCharset(n int, allowedCharacters string) (string, error) {
	output := make([]byte, n)
	max := new(big.Int).SetInt64(int64(len(allowedCharacters)))
	for i := range output {
		randomCharacter, err := rand.Int(rand.Reader, max)
		if err != nil {
			return "", err
		}
		output[i] = allowedCharacters[randomCharacter.Int64()]
	}
	return string(output), nil
}
