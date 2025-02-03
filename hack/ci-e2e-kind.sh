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

source $(pwd)/hack/config/aws_config.sh
source $(pwd)/hack/config/gcp_config.sh
source $(pwd)/hack/config/azure_config.sh

TEST_PROVIDERS=${1:-"aws"}

make PROVIDERS=${TEST_PROVIDERS} \
  STEPS="setup,test,cleanup" \
  test-e2e
