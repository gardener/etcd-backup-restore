// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"bufio"
	"errors"
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
	if err := cmd.process.Process.Signal(os.Interrupt); err != nil {
		cmd.Logger.Errorf("failed to stop process %s: %v", cmd.Task, err)
	}
}

// RunCmdWithFlags creates a process out of the  Cmd object.
func (cmd *Cmd) RunCmdWithFlags() error {
	cmd.process = exec.Command(cmd.Task, cmd.Flags...) // #nosec G204 -- only used by integration tests.
	file, err := os.Create(cmd.Logfile)
	if err != nil {
		cmd.Logger.Errorf("failed to create log file for command %s: %v", cmd.Task, err)
		return err
	}
	defer func() {
		if err1 := file.Close(); err1 != nil {
			cmd.Logger.Errorf("failed to close log file for command %s: %v", cmd.Task, err1)
		}
	}()
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
				if _, err1 := logWriter.WriteString(fmt.Sprintf("%s\n", line)); err1 != nil {
					cmd.Logger.Errorf("failed to write to log file for command %s: %v", cmd.Task, err1)
				}
				if err1 := logWriter.Flush(); err1 != nil {
					cmd.Logger.Errorf("failed to flush log file for command %s: %v", cmd.Task, err1)
				}
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
				if _, err1 := logWriter.WriteString(fmt.Sprintf("%s\n", line)); err1 != nil {
					cmd.Logger.Errorf("failed to write to log file for command %s: %v", cmd.Task, err1)
				}
				if err1 := logWriter.Flush(); err1 != nil {
					cmd.Logger.Errorf("failed to flush log file for command %s: %v", cmd.Task, err1)
				}
			}
		}
	}()

	if err1 := cmd.process.Wait(); err1 != nil {
		var exitErr *exec.ExitError
		if errors.As(err1, &exitErr) {
			cmd.Logger.Infof("command execution stopped (signal: %s)", exitErr.Error())
		}
	}
	return nil
}
