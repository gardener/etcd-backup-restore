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
	process *exec.Cmd
	Logger  *logrus.Logger
	Task    string
	Logfile string
	Flags   []string
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
		if err = file.Close(); err != nil {
			cmd.Logger.Errorf("failed to close log file for command %s: %v", cmd.Task, err)
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
				if _, err = logWriter.WriteString(fmt.Sprintf("%s\n", line)); err != nil {
					cmd.Logger.Errorf("failed to write to log file for command %s: %v", cmd.Task, err)
				}
				if err = logWriter.Flush(); err != nil {
					cmd.Logger.Errorf("failed to flush log file for command %s: %v", cmd.Task, err)
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
				if _, err = logWriter.WriteString(fmt.Sprintf("%s\n", line)); err != nil {
					cmd.Logger.Errorf("failed to write to log file for command %s: %v", cmd.Task, err)
				}
				if err = logWriter.Flush(); err != nil {
					cmd.Logger.Errorf("failed to flush log file for command %s: %v", cmd.Task, err)
				}
			}
		}
	}()

	if err = cmd.process.Wait(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			cmd.Logger.Errorf("command execution stopped (signal: %s)", exitErr.Error())
		}
	}
	return nil
}
