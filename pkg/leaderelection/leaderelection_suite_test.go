// Copyright (c) 2021 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package leaderelection_test

import (
	"context"
	"testing"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/wrappers"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
)

const (
	mockTimeout = time.Second * 5
)

var (
	testCtx               = context.Background()
	reelectionPeriod      = wrappers.Duration{Duration: 1 * time.Second}
	etcdConnectionTimeout = wrappers.Duration{Duration: 1 * time.Second}
	logger                = logrus.New().WithField("suite", "leader-elector")
)

func TestCommon(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Leader Election Suite")
}
