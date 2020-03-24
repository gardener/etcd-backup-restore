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
	"context"

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
			opts.Logger.Infof("%s", optsJSON)

			if err := opts.run(ctx); err != nil {
				opts.Logger.Fatalf("failed to run server: %v", err)
			}
		},
	}

	opts.addFlags(serverCmd.Flags())
	return serverCmd
}
