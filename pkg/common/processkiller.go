// Copyright (c) 2021 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
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

package common

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
)

// Command represents an external command being prepared or run.
type Command interface {
	// Run starts the specified command and waits for it to complete.
	Run() error
	// Output runs the command and returns its standard output.
	Output() ([]byte, error)
}

// CommandFactory creates Command instances.
type CommandFactory interface {
	// NewCommand creates a new Command to execute the named program with the given arguments.
	NewCommand(ctx context.Context, name string, arg ...string) Command
}

// NewExecCommandFactory creates a new CommandFactory that uses exec.CommandContext to create Command instances.
func NewExecCommandFactory() CommandFactory {
	return &execCommandFactory{}
}

type execCommandFactory struct{}

// NewCommand creates a new Command to execute the named program with the given arguments.
func (f *execCommandFactory) NewCommand(ctx context.Context, name string, arg ...string) Command {
	return exec.CommandContext(ctx, name, arg...)
}

// ProcessKiller kills OS processes.
type ProcessKiller interface {
	// Kill kills the corresponding process.
	Kill(ctx context.Context) (bool, error)
}

// NewPIDCommandProcessKiller creates a new ProcessKiller that uses the given pid command to determine
// the pid of the process to kill, and the given command factory to create the commands to be executed.
func NewPIDCommandProcessKiller(processName string, pidCommand string, commandFactory CommandFactory, logger *logrus.Entry) ProcessKiller {
	return &pidCommandProcessKiller{
		processName:    processName,
		pidCommand:     pidCommand,
		commandFactory: commandFactory,
		logger:         logger,
	}
}

type pidCommandProcessKiller struct {
	processName    string
	pidCommand     string
	commandFactory CommandFactory
	logger         *logrus.Entry
}

// Kill kills the corresponding process.
func (k *pidCommandProcessKiller) Kill(ctx context.Context) (bool, error) {
	// Get the process pid by executing a command
	k.logger.Infof("Determining %s process pid by executing command `%s`...", k.processName, k.pidCommand)
	out, err := k.commandFactory.NewCommand(ctx, "sh", "-c", k.pidCommand).Output()
	if err != nil {
		return false, fmt.Errorf("could not determine %s process pid: %w", k.processName, err)
	}
	pid := strings.TrimSpace(string(out))

	// If the output is empty, assume that the process doesn't exist and return false
	if pid == "" {
		return false, nil
	}

	// If the output can't be parsed to a positive integer, return an error
	if n, err := strconv.Atoi(pid); err != nil {
		return false, fmt.Errorf("could not parse pid command output: %w", err)
	} else if n <= 0 {
		return false, fmt.Errorf("parsing pid command output returned a non-positive integer %d", n)
	}

	// If the process exists, kill it (with SIGTERM, to allow it to terminate gracefully) and return true
	k.logger.Infof("Killing %s process with pid %s...", k.processName, pid)
	if err := k.commandFactory.NewCommand(ctx, "sh", "-c", fmt.Sprintf("kill %s", pid)).Run(); err != nil {
		return false, fmt.Errorf("could not kill %s process with pid %s: %w", k.processName, pid, err)
	}
	return true, nil
}
