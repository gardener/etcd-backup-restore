// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package miscellaneous

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var (
	etcdDir = "./default.etcd"
	testCtx = context.TODO()
	logger  = logrus.New().WithField("suite", "miscellaneous")
)

func TestRestorer(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Miscellaneous")
}
