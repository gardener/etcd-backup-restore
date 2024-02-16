// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

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
