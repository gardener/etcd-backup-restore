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
	"time"

	. "github.com/gardener/etcd-backup-restore/pkg/common"
	mockcommon "github.com/gardener/etcd-backup-restore/pkg/mock/common"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
)

const (
	ownerName = "foo.example.com"
	ownerID   = "foo"
	timeout   = 50 * time.Millisecond
)

var _ = Describe("OwnerChecker", func() {
	var (
		ctrl         *gomock.Controller
		resolver     *mockcommon.MockResolver
		logger       *logrus.Entry
		ctx          context.Context
		ownerChecker Checker
	)

	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())
		resolver = mockcommon.NewMockResolver(ctrl)
		logger = logrus.New().WithField("test", "OwnerChecker")
		ctx = context.TODO()
		ownerChecker = NewOwnerChecker(ownerName, ownerID, timeout, resolver, logger)
	})

	AfterEach(func() {
		ctrl.Finish()
	})

	Describe("#Check", func() {
		It("should return true if the owner domain name resolves to the specified owner ID", func() {
			resolver.EXPECT().LookupTXT(gomock.Any(), ownerName).Return([]string{ownerID}, nil)

			result, err := ownerChecker.Check(ctx)
			Expect(err).To(Not(HaveOccurred()))
			Expect(result).To(BeTrue())
		})

		It("should return false if the owner domain name resolves to a different owner ID", func() {
			resolver.EXPECT().LookupTXT(gomock.Any(), ownerName).Return([]string{"bar"}, nil)

			result, err := ownerChecker.Check(ctx)
			Expect(err).To(Not(HaveOccurred()))
			Expect(result).To(BeFalse())
		})

		It("should fail if the owner domain name could not be resolved", func() {
			resolver.EXPECT().LookupTXT(gomock.Any(), ownerName).Return(nil, errors.New("test"))

			_, err := ownerChecker.Check(ctx)
			Expect(err).To(HaveOccurred())
		})

		It("should fail if the owner domain name resolution times out", func() {
			resolver.EXPECT().LookupTXT(gomock.Any(), ownerName).DoAndReturn(func(ctx context.Context, name string) ([]string, error) {
				// Take twice as long as the timeout to succeed
				for {
					select {
					case <-time.After(timeout * 2):
						return []string{ownerID}, nil
					case <-ctx.Done():
						return nil, errors.New("context cancelled")
					}
				}
			})

			_, err := ownerChecker.Check(ctx)
			Expect(err).To(HaveOccurred())
		})
	})
})
