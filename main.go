// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/gardener/etcd-backup-restore/cmd"
)

var onlyOneSignalHandler = make(chan struct{})

func main() {
	if len(os.Getenv("GOMAXPROCS")) == 0 {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}

	ctx := setupSignalHandler()
	command := cmd.NewBackupRestoreCommand(ctx)
	if err := command.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// setupSignalHandler creates context carrying system signals. A context is returned
// which is cancelled on one of these signals. If a second signal is caught, the program
// is terminated with exit code 1.
func setupSignalHandler() context.Context {
	close(onlyOneSignalHandler) // panics when called twice

	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		cancel()
		<-c
		os.Exit(1) // second signal. Exit directly.
	}()

	return ctx
}
