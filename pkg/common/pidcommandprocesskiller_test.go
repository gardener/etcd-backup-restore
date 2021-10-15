// Copyright (c) 2021 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://wwr.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common_test

import (
	"context"
	"errors"

	. "github.com/gardener/etcd-backup-restore/pkg/common"
	mockcommon "github.com/gardener/etcd-backup-restore/pkg/mock/common"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
)

const (
	pidCommand = "ps ax -o pid,comm | grep 'foo$' | awk '{print $1}'"
)

var _ = Describe("PIDCommandProcessKiller", func() {
	var (
		ctrl                *gomock.Controller
		determinePidCommand *mockcommon.MockCommand
		killCommand         *mockcommon.MockCommand
		commandFactory      *mockcommon.MockCommandFactory
		logger              *logrus.Entry
		ctx                 context.Context
		processKiller       ProcessKiller
	)

	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())
		determinePidCommand = mockcommon.NewMockCommand(ctrl)
		killCommand = mockcommon.NewMockCommand(ctrl)
		commandFactory = mockcommon.NewMockCommandFactory(ctrl)
		logger = logrus.New().WithField("test", "PIDCommandProcessKiller")
		ctx = context.TODO()
		processKiller = NewPIDCommandProcessKiller(processName, pidCommand, commandFactory, logger)
	})

	AfterEach(func() {
		ctrl.Finish()
	})

	Describe("#Kill", func() {
		It("should determine the process pid, kill it, and return true if the process exists", func() {
			commandFactory.EXPECT().NewCommand(ctx, "sh", "-c", pidCommand).Return(determinePidCommand)
			determinePidCommand.EXPECT().Output().Return([]byte("42"), nil)
			commandFactory.EXPECT().NewCommand(ctx, "sh", "-c", "kill 42").Return(killCommand)
			killCommand.EXPECT().Run().Return(nil)

			result, err := processKiller.Kill(ctx)
			Expect(err).To(Not(HaveOccurred()))
			Expect(result).To(BeTrue())
		})

		It("should return false if the process doesn't exist", func() {
			commandFactory.EXPECT().NewCommand(ctx, "sh", "-c", pidCommand).Return(determinePidCommand)
			determinePidCommand.EXPECT().Output().Return([]byte{}, nil)

			result, err := processKiller.Kill(ctx)
			Expect(err).To(Not(HaveOccurred()))
			Expect(result).To(BeFalse())
		})

		It("should fail if the determine pid command fails", func() {
			commandFactory.EXPECT().NewCommand(ctx, "sh", "-c", pidCommand).Return(determinePidCommand)
			determinePidCommand.EXPECT().Output().Return([]byte{}, errors.New("test"))

			_, err := processKiller.Kill(ctx)
			Expect(err).To(HaveOccurred())
		})

		It("should fail if the kill command fails", func() {
			commandFactory.EXPECT().NewCommand(ctx, "sh", "-c", pidCommand).Return(determinePidCommand)
			determinePidCommand.EXPECT().Output().Return([]byte("42"), nil)
			commandFactory.EXPECT().NewCommand(ctx, "sh", "-c", "kill 42").Return(killCommand)
			killCommand.EXPECT().Run().Return(errors.New("test"))

			_, err := processKiller.Kill(ctx)
			Expect(err).To(HaveOccurred())
		})

		It("should fail if the output of the determine pid command can't be parsed to an integer", func() {
			commandFactory.EXPECT().NewCommand(ctx, "sh", "-c", pidCommand).Return(determinePidCommand)
			determinePidCommand.EXPECT().Output().Return([]byte("foo"), nil)

			_, err := processKiller.Kill(ctx)
			Expect(err).To(HaveOccurred())
		})

		It("should fail if the output of the determine pid command is a non-positive integer", func() {
			commandFactory.EXPECT().NewCommand(ctx, "sh", "-c", pidCommand).Return(determinePidCommand)
			determinePidCommand.EXPECT().Output().Return([]byte("0"), nil)

			_, err := processKiller.Kill(ctx)
			Expect(err).To(HaveOccurred())
		})
	})
})
