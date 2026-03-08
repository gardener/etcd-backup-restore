#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
#
# SPDX-License-Identifier: Apache-2.0
set -o errexit
set -o nounset
set -o pipefail

export ETCDBR_IMAGE="europe-docker.pkg.dev/gardener-project/public/gardener/etcdbrctl"
export ETCD_WRAPPER_IMAGE="europe-docker.pkg.dev/gardener-project/public/gardener/etcd-wrapper"
export ETCDBR_VERSION="dev-latest" # This version gets built from the current branch, loaded into the kind cluster and used for the e2e tests
export ETCD_WRAPPER_VERSION="latest"

TEST_PROVIDERS=${1:-"aws"}

make kind-up

trap 'make kind-down' EXIT

make PROVIDERS=${TEST_PROVIDERS} \
  KUBECONFIG=${KUBECONFIG} \
  STEPS="setup,test" \
  test-e2e
