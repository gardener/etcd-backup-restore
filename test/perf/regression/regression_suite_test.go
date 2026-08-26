// SPDX-FileCopyrightText: Copyright Contributors to the Gardener project
//
// SPDX-License-Identifier: Apache-2.0

package regression

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestPerformanceRegression(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Performance Regression Test Suite")
}
