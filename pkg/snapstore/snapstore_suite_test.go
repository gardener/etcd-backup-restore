// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore_test

import (
	"testing"

	th "github.com/gophercloud/gophercloud/testhelper"
	"github.com/sirupsen/logrus"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var (
	testObj *testing.T
)

func TestSnapstore(t *testing.T) {
	RegisterFailHandler(Fail)
	testObj = t
	RunSpecs(t, "Snapstore Suite")
}

var _ = BeforeSuite(func() {
	logrus.Infof("Starting test server...")
	th.SetupHTTP()
	initializeMockSwiftServer(testObj)
})

var _ = AfterSuite(func() {
	logrus.Infof("Shutting down test server...")
	th.TeardownHTTP()
})
