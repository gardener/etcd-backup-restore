// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

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
