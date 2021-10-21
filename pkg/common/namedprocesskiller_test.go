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
	processName       = "foo"
	pid         int32 = 42
)

var _ = Describe("NamedProcessKiller", func() {
	var (
		ctrl          *gomock.Controller
		process       *mockcommon.MockProcess
		processLister *mockcommon.MockProcessLister
		logger        *logrus.Entry
		ctx           context.Context
		processKiller ProcessKiller
	)

	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())
		process = mockcommon.NewMockProcess(ctrl)
		processLister = mockcommon.NewMockProcessLister(ctrl)
		logger = logrus.New().WithField("test", "NamedProcessKiller")
		ctx = context.TODO()
		processKiller = NewNamedProcessKiller(processName, processLister, logger)
	})

	AfterEach(func() {
		ctrl.Finish()
	})

	Describe("#Kill", func() {
		It("should determine the process, terminate it, and return true if the process exists", func() {
			processLister.EXPECT().ProcessesWithContext(ctx).Return([]Process{process}, nil)
			process.EXPECT().Pid().Return(pid)
			process.EXPECT().NameWithContext(ctx).Return(processName, nil)
			process.EXPECT().TerminateWithContext(ctx).Return(nil)

			result, err := processKiller.Kill(ctx)
			Expect(err).To(Not(HaveOccurred()))
			Expect(result).To(BeTrue())
		})

		It("should return false if the process doesn't exist", func() {
			processLister.EXPECT().ProcessesWithContext(ctx).Return([]Process{process}, nil)
			process.EXPECT().NameWithContext(ctx).Return("bar", nil)

			result, err := processKiller.Kill(ctx)
			Expect(err).To(Not(HaveOccurred()))
			Expect(result).To(BeFalse())
		})

		It("should fail if listing processes fails", func() {
			processLister.EXPECT().ProcessesWithContext(ctx).Return(nil, errors.New("test"))

			_, err := processKiller.Kill(ctx)
			Expect(err).To(HaveOccurred())
		})

		It("should fail if getting the process name fails", func() {
			processLister.EXPECT().ProcessesWithContext(ctx).Return([]Process{process}, nil)
			process.EXPECT().Pid().Return(pid)
			process.EXPECT().NameWithContext(ctx).Return("", errors.New("test"))

			_, err := processKiller.Kill(ctx)
			Expect(err).To(HaveOccurred())
		})

		It("should fail if terminating the process fails", func() {
			processLister.EXPECT().ProcessesWithContext(ctx).Return([]Process{process}, nil)
			process.EXPECT().Pid().Return(pid).Times(2)
			process.EXPECT().NameWithContext(ctx).Return(processName, nil)
			process.EXPECT().TerminateWithContext(ctx).Return(errors.New("test"))

			_, err := processKiller.Kill(ctx)
			Expect(err).To(HaveOccurred())
		})
	})
})
