#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
#
# SPDX-License-Identifier: Apache-2.0
set -o errexit
set -o nounset
set -o pipefail

make kind-up

trap "
  ( make kind-down )
" EXIT

kubectl wait --for=condition=ready node --all
export ETCD_VERSION="v0.1.1" #v3.4.13-bootstrap-1
export ETCDBR_VERSION="v3.6" #v0.29.0-dev (for anveshreddy18 dockerhub)


STORAGE_ACCOUNT="devstoreaccount1"
STORAGE_KEY="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
AZURE_STORAGE_API_ENDPOINT="http://localhost:10000"
AZURITE_HOST="azurite-service.default:10000"
AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=http;AccountName=${STORAGE_ACCOUNT};AccountKey=${STORAGE_KEY};BlobEndpoint=${AZURE_STORAGE_API_ENDPOINT}/${STORAGE_ACCOUNT};"

export AZURE_APPLICATION_CREDENTIALS="/tmp/azuriteCredentials"
mkdir -p "${AZURE_APPLICATION_CREDENTIALS}"
echo -n "${STORAGE_ACCOUNT}" > "${AZURE_APPLICATION_CREDENTIALS}/storageAccount"
echo -n "${STORAGE_KEY}" > "${AZURE_APPLICATION_CREDENTIALS}/storageKey"

export AWS_APPLICATION_CREDENTIALS_JSON="/tmp/aws.json"
echo "{ \"accessKeyID\": \"ACCESSKEYAWSUSER\", \"secretAccessKey\": \"sEcreTKey\", \"region\": \"us-east-2\", \"endpoint\": \"http://127.0.0.1:4566\", \"s3ForcePathStyle\": true }" >/tmp/aws.json

# GOOGLE_APPLICATION_CREDENTIALS="/Users/i586337/Downloads/svc_acc.json" \
# GCP_PROJECT_ID="sap-se-gcp-k8s-dev-team" \


: ${TEST_PROVIDERS:="aws"}
TEST_PROVIDERS=${1:-$TEST_PROVIDERS}


make LOCALSTACK_HOST="localstack.default:4566" \
  AWS_ENDPOINT_URL_S3="http://localhost:4566" \
  AWS_ACCESS_KEY_ID="ACCESSKEYAWSUSER" \
  AWS_SECRET_ACCESS_KEY="sEcreTKey" \
  AWS_DEFAULT_REGION=us-east-2 \
  GOOGLE_EMULATOR_ENABLED="true" \
  GOOGLE_EMULATOR_HOST="fake-gcs.default:8000" \
  GOOGLE_STORAGE_API_ENDPOINT="http://localhost:8000/storage/v1/" \
  STORAGE_ACCOUNT=${STORAGE_ACCOUNT} \
  STORAGE_KEY=${STORAGE_KEY} \
  AZURE_STORAGE_API_ENDPOINT=${AZURE_STORAGE_API_ENDPOINT} \
  AZURE_EMULATOR_ENABLED="true" \
  AZURITE_HOST=${AZURITE_HOST} \
  AZURE_STORAGE_CONNECTION_STRING=${AZURE_STORAGE_CONNECTION_STRING} \
  PROVIDERS=${TEST_PROVIDERS} \
  test-e2e