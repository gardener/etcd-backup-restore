package integration

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"

	"github.com/sirupsen/logrus"
)

type Cmd struct {
	Task    string
	Flags   []string
	process *exec.Cmd
	Logfile string
	Logger  *logrus.Logger
}

func (cmd *Cmd) StopProcess() {
	cmd.process.Process.Signal(os.Interrupt)
}

func (cmd *Cmd) RunCmdWithFlags() {
	cmd.process = exec.Command(cmd.Task, cmd.Flags...)
	file, err := os.Create(cmd.Logfile)
	if err != nil {
		cmd.Logger.Error(err)
	}
	defer file.Close()
	cmd.Logger.Infof("%v %v", cmd.Task, cmd.Flags)
	logWriter := bufio.NewWriter(file)
	stderr, err := cmd.process.StderrPipe()
	if err != nil {
		cmd.Logger.Errorf("Error: %v", err)
	}
	stdout, err := cmd.process.StdoutPipe()
	if err != nil {
		cmd.Logger.Error(err)
	}
	err = cmd.process.Start()
	if err != nil {
		cmd.Logger.Error(err)
	}

	go func() {
		reader := bufio.NewReader(stderr)
		for {
			line, _, _ := reader.ReadLine()
			if len(line) > 0 {
				//cmd.Logger.Errorf("%v %v", cmd.Task, string(line))
				logWriter.WriteString(fmt.Sprintf("%s\n", line))
				logWriter.Flush()
			}

		}

	}()

	go func() {
		reader := bufio.NewReader(stdout)
		for {
			line, _, _ := reader.ReadLine()
			if len(line) > 0 {
				//cmd.Logger.Infof("%v %v", cmd.Task, string(line))
				logWriter.WriteString(fmt.Sprintf("%s\n", line))
				logWriter.Flush()
			}

		}

	}()

	if err := cmd.process.Wait(); err != nil {
		cmd.Logger.Errorf("Error(%v): %v", cmd.Task, err)
	}

}
