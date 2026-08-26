#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: Copyright Contributors to the Gardener project
#
# SPDX-License-Identifier: Apache-2.0


set -o errexit
set -o nounset
set -o pipefail

kind create cluster --name etcdbr-e2e --config hack/e2e-test/infrastructure/kind/cluster.yaml
