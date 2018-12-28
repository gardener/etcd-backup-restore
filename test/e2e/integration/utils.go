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

package integration

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"

	"github.com/sirupsen/logrus"
)

// Cmd holds the information needed to run a process
type Cmd struct {
	Task    string
	Flags   []string
	process *exec.Cmd
	Logfile string
	Logger  *logrus.Logger
}

// StopProcess is used to signal SIGTERM to the running process.
func (cmd *Cmd) StopProcess() {
	cmd.process.Process.Signal(os.Interrupt)
}

// RunCmdWithFlags creates a process out of the  Cmd object.
func (cmd *Cmd) RunCmdWithFlags() error {
	cmd.process = exec.Command(cmd.Task, cmd.Flags...)
	file, err := os.Create(cmd.Logfile)
	if err != nil {
		cmd.Logger.Errorf("failed to create log file for command %s: %v", cmd.Task, err)
		return err
	}
	defer file.Close()
	cmd.Logger.Infof("%v %v", cmd.Task, cmd.Flags)
	logWriter := bufio.NewWriter(file)
	stderr, err := cmd.process.StderrPipe()
	if err != nil {
		cmd.Logger.Errorf("failed to get standard error pipe connecting to command %s: %v", cmd.Task, err)
		return err
	}
	stdout, err := cmd.process.StdoutPipe()
	if err != nil {
		cmd.Logger.Errorf("failed to get standard output pipe connecting to command %s: %v", cmd.Task, err)
		return err
	}
	if err := cmd.process.Start(); err != nil {
		cmd.Logger.Errorf("failed to start command %s: %v", cmd.Task, err)
		return err
	}

	go func() {
		reader := bufio.NewReader(stderr)
		for {
			line, _, err := reader.ReadLine()
			if err == io.EOF {
				break
			}
			if len(line) > 0 {
				logWriter.WriteString(fmt.Sprintf("%s\n", line))
				logWriter.Flush()
			}
		}
	}()

	go func() {
		reader := bufio.NewReader(stdout)
		for {
			line, _, err := reader.ReadLine()
			if err == io.EOF {
				break
			}
			if len(line) > 0 {
				logWriter.WriteString(fmt.Sprintf("%s\n", line))
				logWriter.Flush()
			}
		}
	}()

	if err := cmd.process.Wait(); err != nil {
		if exiterr, ok := err.(*exec.ExitError); ok {
			cmd.Logger.Infof("command execution stopped (signal: %s)", exiterr.Error())
		} else if err != nil {
			cmd.Logger.Errorf("command %s failed with error: %v", cmd.Task, err)
			return err
		}
	}
	return nil
}
