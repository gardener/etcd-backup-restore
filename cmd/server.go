// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"

	"sigs.k8s.io/yaml"

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
