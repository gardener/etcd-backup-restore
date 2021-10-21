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
	"time"

	. "github.com/gardener/etcd-backup-restore/pkg/common"
	mockcommon "github.com/gardener/etcd-backup-restore/pkg/mock/common"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/util/clock"
)

const (
	ttl = 30 * time.Second
)

var _ = Describe("CachingResolver", func() {
	var (
		ctrl            *gomock.Controller
		resolver        *mockcommon.MockResolver
		fakeClock       *clock.FakeClock
		ctx             context.Context
		cachingResolver Resolver
	)

	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())
		resolver = mockcommon.NewMockResolver(ctrl)
		fakeClock = clock.NewFakeClock(time.Now())
		ctx = context.TODO()
		cachingResolver = NewCachingResolver(resolver, fakeClock, ttl)
	})

	AfterEach(func() {
		ctrl.Finish()
	})

	Describe("#LookupTXT", func() {
		It("should cache results and use the cache correctly", func() {
			resolver.EXPECT().LookupTXT(ctx, "foo").Return([]string{"bar"}, nil)
			resolver.EXPECT().LookupTXT(ctx, "foo").Return([]string{"baz"}, nil)

			// Since the cache is empty, lookup a new result and cache it
			result, err := cachingResolver.LookupTXT(ctx, "foo")
			Expect(err).To(Not(HaveOccurred()))
			Expect(result).To(Equal([]string{"bar"}))

			// Return the result from the cache
			result, err = cachingResolver.LookupTXT(ctx, "foo")
			Expect(err).To(Not(HaveOccurred()))
			Expect(result).To(Equal([]string{"bar"}))

			fakeClock.Step(ttl)

			// Since the TTL has passed, lookup a new result and cache it
			result, err = cachingResolver.LookupTXT(ctx, "foo")
			Expect(err).To(Not(HaveOccurred()))
			Expect(result).To(Equal([]string{"baz"}))

			// Return the result from the cache
			result, err = cachingResolver.LookupTXT(ctx, "foo")
			Expect(err).To(Not(HaveOccurred()))
			Expect(result).To(Equal([]string{"baz"}))
		})
	})
})
