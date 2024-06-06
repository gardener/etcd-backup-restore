#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
#
# SPDX-License-Identifier: Apache-2.0
set -o errexit
set -o nounset
set -o pipefail

export ETCD_VERSION="v0.1.1"
export ETCDBR_VERSION="dev-latest"
export ETCDBR_IMAGE="europe-docker.pkg.dev/gardener-project/snapshots/gardener/etcdbrctl"

source $(pwd)/hack/config/aws_config.sh
source $(pwd)/hack/config/gcp_config.sh
source $(pwd)/hack/config/azure_config.sh

TEST_PROVIDERS=${1:-"aws"}

make LOCALSTACK_HOST=${LOCALSTACK_HOST} \
  AWS_ENDPOINT_URL_S3=${AWS_ENDPOINT_URL_S3} \
  AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
  AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
  AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION} \
  GOOGLE_EMULATOR_ENABLED="true" \
  GOOGLE_EMULATOR_HOST=${GOOGLE_EMULATOR_HOST} \
  GOOGLE_STORAGE_API_ENDPOINT=${GOOGLE_STORAGE_API_ENDPOINT} \
  STORAGE_ACCOUNT=${STORAGE_ACCOUNT} \
  STORAGE_KEY=${STORAGE_KEY} \
  AZURE_STORAGE_API_ENDPOINT=${AZURE_STORAGE_API_ENDPOINT} \
  AZURE_EMULATOR_ENABLED="true" \
  AZURITE_HOST=${AZURITE_HOST} \
  AZURE_STORAGE_CONNECTION_STRING=${AZURE_STORAGE_CONNECTION_STRING} \
  PROVIDERS=${TEST_PROVIDERS} \
  STEPS="setup,test,cleanup" \
  test-e2e
