// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package membergarbagecollector_test

import (
	"io"
	"testing"

	"github.com/sirupsen/logrus"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var (
	logger = logrus.New().WithField("suite", "etcd-member-gc")
)

func TestMembergarbagecollector(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Membergarbagecollector Suite")
}

var _ = SynchronizedBeforeSuite(func() []byte {
	logger.Logger.Out = io.Discard
	return nil
}, func(_ []byte) {})
