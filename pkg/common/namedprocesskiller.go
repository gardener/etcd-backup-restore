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

	"github.com/shirou/gopsutil/v3/process"
	"github.com/sirupsen/logrus"
)

// Process represents a running OS process.
type Process interface {
	// Pid returns the process pid.
	Pid() int32
	// NameWithContext returns the process name.
	NameWithContext(ctx context.Context) (string, error)
	// TerminateWithContext sends SIGTERM to the process.
	TerminateWithContext(ctx context.Context) error
}

type gopsutilProcess struct{ *process.Process }

func (p *gopsutilProcess) Pid() int32 { return p.Process.Pid }

// ProcessLister lists all currently running processes.
type ProcessLister interface {
	// ProcessesWithContext returns a slice of Process instances for all currently running processes.
	ProcessesWithContext(ctx context.Context) ([]Process, error)
}

// NewGopsutilProcessLister creates a new ProcessLister that uses process.ProcessesWithContext to list all currently running processes.
func NewGopsutilProcessLister() ProcessLister {
	return &gopsutilProcessLister{}
}

type gopsutilProcessLister struct{}

// ProcessesWithContext returns a slice of Process instances for all currently running processes.
func (f *gopsutilProcessLister) ProcessesWithContext(ctx context.Context) ([]Process, error) {
	ps, err := process.ProcessesWithContext(ctx)
	if err != nil {
		return nil, err
	}
	var processes []Process
	for _, p := range ps {
		processes = append(processes, &gopsutilProcess{Process: p})
	}
	return processes, nil
}

// ProcessKiller kills OS processes.
type ProcessKiller interface {
	// Kill kills the corresponding process.
	Kill(ctx context.Context) (bool, error)
}

// NewNamedProcessKiller creates a new ProcessKiller that searches through all running processes
// to find the process to be killed, and then terminates it, using appropriate library functions.
func NewNamedProcessKiller(processName string, processLister ProcessLister, logger *logrus.Entry) ProcessKiller {
	return &namedProcessKiller{
		processName:   processName,
		processLister: processLister,
		logger:        logger,
	}
}

type namedProcessKiller struct {
	processName   string
	processLister ProcessLister
	logger        *logrus.Entry
}

// Kill kills the corresponding process.
func (k *namedProcessKiller) Kill(ctx context.Context) (bool, error) {
	// Find the process to be killed by searching through all running processes
	k.logger.Infof("Determining process %q by searching through all running processes...", k.processName)
	processes, err := k.processLister.ProcessesWithContext(ctx)
	if err != nil {
		return false, fmt.Errorf("could not list running processes: %w", err)
	}
	var process Process
	for _, p := range processes {
		processName, err := p.NameWithContext(ctx)
		if err != nil {
			return false, fmt.Errorf("could not get process name for pid %d: %w", p.Pid(), err)
		}
		if processName == k.processName {
			process = p
			break
		}
	}

	// If the process doesn't exist, return false
	if process == nil {
		k.logger.Infof("Process %q not found", k.processName)
		return false, nil
	}

	// If the process exists, terminate it (using SIGTERM) and return true
	k.logger.Infof("Terminating process %q with pid %d...", k.processName, process.Pid())
	if err := process.TerminateWithContext(ctx); err != nil {
		return false, fmt.Errorf("could not terminate process %q with pid %d: %w", k.processName, process.Pid(), err)
	}
	return true, nil
}
