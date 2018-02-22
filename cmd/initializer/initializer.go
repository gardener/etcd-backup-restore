// Copyright © 2018 The Gardener Authors.
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

package initializer

import (
	"fmt"
	"log"

	"github.com/gardener/etcd-backup-restore/pkg/initializer"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	dataDir string
)

// NewInitializeCommand returns the command to initialize etcd by validating the data
// directory and restoring from cloud store if needed.
func NewInitializeCommand(stopCh <-chan struct{}) *cobra.Command {

	// restoreCmd represents the restore command
	initializeCmd := &cobra.Command{
		Use:   "initialize",
		Short: "initialize an etcd instance.",
		Long:  fmt.Sprintf(`Initializes an etcd instance. Data directory is checked for corruption and restored in case of corruption.`),
		Run: func(cmd *cobra.Command, args []string) {
			logger := logrus.New()
			etcdInitializer := initializer.NewInitializer(dataDir, storageProvider, logger)
			err := etcdInitializer.Initialize()
			if err != nil {
				log.Fatal(err)
			}
		},
	}
	initializeFlags(initializeCmd)
	return initializeCmd
}
