#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
#
# SPDX-License-Identifier: Apache-2.0

set -o errexit
set -o nounset
set -o pipefail

# SOURCE_PATH - path to component repository root directory.
if [[ $(uname) == 'Darwin' ]]; then
  READLINK_BIN="greadlink"
else
  READLINK_BIN="readlink"
fi

export SOURCE_PATH="$(${READLINK_BIN} -f "$(dirname ${0})/..")"

TEST_ID_PREFIX="etcdbr-e2e-test"

export GOBIN="${SOURCE_PATH}/bin"
export PATH="${GOBIN}:${PATH}"
SOURCE_PATH=$(dirname "${SOURCE_PATH}")
cd "${SOURCE_PATH}"

function setup_ginkgo() {
    echo "Installing Ginkgo..."
    go install github.com/onsi/ginkgo/ginkgo@v1.14.1
    ginkgo version
    echo "Successfully installed Ginkgo."
}

function get_test_id() {
  git_commit=`git show -s --format="%H"`
  export TEST_ID=${TEST_ID_PREFIX}-${git_commit}
  echo "Test id: ${TEST_ID}"
}

function cleanup_aws_infrastructure() {
    echo "Cleaning up AWS infrastructure..."
    echo "Deleting test bucket..."
    result=$(aws s3api get-bucket-location --bucket ${TEST_ID} 2>&1 || true)
    if [[ $result == *NoSuchBucket* ]]; then
      echo "Bucket is already gone."
      return
    fi
    aws s3 rb s3://${TEST_ID} --force
    echo "Successfully deleted test bucket."
    
    unset AWS_ENDPOINT_URL_S3 AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION LOCALSTACK_HOST
    echo "Cleaning up AWS infrastructure completed."
}

function cleanup_gcs_infrastructure() {
  if [[ -n ${GOOGLE_APPLICATION_CREDENTIALS:-""} ]]; then
    result=$(gsutil list gs://${TEST_ID} 2>&1 || true)
    if [[ $result  == *"404"* ]]; then
      echo "GCS bucket is already deleted."
      return
    fi
    echo "Deleting GCS bucket ${TEST_ID} ..."
    gsutil rm -r gs://"${TEST_ID}"/
    echo "Successfully deleted GCS bucket ${TEST_ID} ."
  else 
    result=$(gsutil -o "Credentials:gs_json_host=127.0.0.1" -o "Credentials:gs_json_port=4443" -o "Boto:https_validate_certificates=False" list gs://${TEST_ID} 2>&1 || true)
    if [[ $result  == *"404"* ]]; then
      echo "GCS bucket is already deleted."
      return
    fi
    echo "Deleting GCS bucket ${TEST_ID} ..."
    gsutil -o "Credentials:gs_json_host=127.0.0.1" -o "Credentials:gs_json_port=4443" -o "Boto:https_validate_certificates=False" rm -r gs://"${TEST_ID}"/
    echo "Successfully deleted GCS bucket ${TEST_ID} ."
  fi

  unset GOOGLE_EMULATOR_HOST GOOGLE_EMULATOR_ENABLED GOOGLE_STORAGE_API_ENDPOINT
  echo "Cleaning up GCS infrastructure completed."
}

function cleanup_azure_infrastructure() {
  echo "Cleaning up Azure infrastructure..."
  echo "Deleting test container..."
  if [[ -n ${AZURE_EMULATOR_ENABLED:-""} ]]; then
    az storage container delete --connection-string "${AZURE_STORAGE_CONNECTION_STRING}" --name "${TEST_ID}"
  else
    az storage container delete --account-name "${STORAGE_ACCOUNT}" --account-key "${STORAGE_KEY}" --name "${TEST_ID}"
  fi
  echo "Successfully deleted test container."

  unset STORAGE_ACCOUNT STORAGE_KEY AZURE_STORAGE_API_ENDPOINT AZURE_EMULATOR_ENABLED AZURITE_HOST AZURE_STORAGE_CONNECTION_STRING
  echo "Cleaning up Azure infrastructure completed."
}

function cleanup() {
    echo "Cleaning up..."
    for p in ${1//,/ }; do
      case $p in
        aws) cleanup_aws_infrastructure & ;;
        gcp) cleanup_gcs_infrastructure & ;;
        azure) cleanup_azure_infrastructure & ;;
        *) echo "Provider: $p is not supported" ;;
        esac
    done
    wait
    kind delete cluster --name etcdbr-e2e
    echo "Cleaning up completed."
}

function setup_awscli() {
    echo "Installing awscli..."
    pip3 install --break-system-packages awscli
    echo "Successfully installed awscli."
}

function setup_aws_infrastructure() {
    echo "Setting up AWS infrastructure..."
    echo "Creating test bucket..."
    result=$(aws s3api get-bucket-location --bucket ${TEST_ID} 2>&1 || true)
    if [[ $result == *NoSuchBucket* ]]; then
      echo "Creating S3 bucket ${TEST_ID} in region ${AWS_DEFAULT_REGION}"
      aws s3api create-bucket --bucket ${TEST_ID} --region ${AWS_DEFAULT_REGION} --create-bucket-configuration LocationConstraint=${AWS_DEFAULT_REGION} --acl private
      # Block public access to the S3 bucket
      aws s3api put-public-access-block --bucket ${TEST_ID} --public-access-block-configuration "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"
      # Deny non-HTTPS requests to the S3 bucket, except for localstack which is exposed on an HTTP endpoint
      if [[ -z "${LOCALSTACK_HOST}" ]]; then
        aws s3api put-bucket-policy --bucket ${TEST_ID} --policy "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Deny\",\"Principal\":\"*\",\"Action\":\"s3:*\",\"Resource\":[\"arn:aws:s3:::${TEST_ID}\",\"arn:aws:s3:::${TEST_ID}/*\"],\"Condition\":{\"Bool\":{\"aws:SecureTransport\":\"false\"},\"NumericLessThan\":{\"s3:TlsVersion\":\"1.2\"}}}]}"
      fi
    else
      echo $result
      if [[ $result != *${AWS_DEFAULT_REGION}* ]]; then
        exit 1
      fi
    fi
    echo "Successfully created test bucket."
    echo "Setting up AWS infrastructure completed."
}

function usage_aws {
    cat <<EOM
Usage:
run-e2e-test.sh aws

Please make sure the following environment variables are set:
    AWS_ACCESS_KEY_ID       Key ID of the user.
    AWS_SECRET_ACCESS_KEY   Access key of the user.
    AWS_DEFAULT_REGION      Region in which the test bucket is created.
    LOCALSTACK_HOST         Host of the localstack service. ( optional: required for testing with localstack)
    AWS_ENDPOINT_URL_S3     URL of the S3 endpoint. ( optional: required for testing with localstack)
EOM
    exit 0
}

function setup_aws_e2e() {    
    ( [[ -z ${AWS_ACCESS_KEY_ID:-""} ]] || [[ -z ${AWS_SECRET_ACCESS_KEY:=""} ]]  || [[ -z ${AWS_DEFAULT_REGION:=""} ]] ) && usage_aws
    if [[ -n ${LOCALSTACK_HOST:-""} ]]; then
      make deploy-localstack
    else 
      echo "LOCALSTACK_HOST is not set. Using AWS services for testing."
    fi
    setup_awscli
    setup_aws_infrastructure
}

function setup_gcscli() {
  echo "Installing gsutil..."
  pip3 install gsutil
  echo "Successfully installed gsutil."
}

function setup_gcs_infrastructure() {
  echo "Setting up GCS infrastructure..."
  echo "Creating test bucket..."
  if [[ -n ${GOOGLE_APPLICATION_CREDENTIALS:-""} ]]; then
    gsutil mb "gs://${TEST_ID}"
  else 
    gsutil -o "Credentials:gs_json_host=127.0.0.1" -o "Credentials:gs_json_port=4443" -o "Boto:https_validate_certificates=False" mb "gs://${TEST_ID}"
  fi
  echo "Successfully created test bucket."
  echo "Setting up GCS infrastructure completed."
}

function usage_gcs() {
  cat <<EOM
Usage:
run-e2e-test.sh gcs

Please make sure the following environment variables are set:
    GOOGLE_APPLICATION_CREDENTIALS    Path to the service account key file. ( for real infra )
    GCP_PROJECT_ID                    Project ID of the GCP project. ( for real infra )
    GOOGLE_EMULATOR_HOST              Host of the fake GCS server. ( for fakegcs )
    GOOGLE_EMULATOR_ENABLED           Set to "true" to Enable the fake GCS server for testing. ( for fakegcs )
    GOOGLE_STORAGE_API_ENDPOINT       URL of the GCS storage endpoint ( for fakegcs )
EOM
  exit 0
}

function authorize_gcloud() {
  if ! $(which gcloud > /dev/null); then
    echo "gcloud is not installed. Please install gcloud and try again."
    exit 1
  fi
  echo "Authorizing access to Gcloud..."
  gcloud auth activate-service-account --key-file="${GOOGLE_APPLICATION_CREDENTIALS}" --project="${GCP_PROJECT_ID}"
  echo "Successfully authorized Gcloud."
}

function setup_gcs_e2e() {
  if [[ -n ${GOOGLE_APPLICATION_CREDENTIALS:-""} ]]; then
    ( [[ -z ${GCP_PROJECT_ID:-""} ]] ) && usage_gcs
    authorize_gcloud
  else
    ( [[ -z ${GOOGLE_EMULATOR_ENABLED:-""} ]] || [[ -z ${GOOGLE_EMULATOR_HOST:-""} ]] || [[ -z ${GOOGLE_STORAGE_API_ENDPOINT:-""} ]] ) && usage_gcs
    echo "GOOGLE_APPLICATION_CREDENTIALS is not set. Using fake GCS server for testing."
    make deploy-fakegcs
  fi
  setup_gcscli
  setup_gcs_infrastructure
}

function setup_azure_infrastructure() {
  export AZURE_APPLICATION_CREDENTIALS="/tmp/azuriteCredentials"
  mkdir -p "${AZURE_APPLICATION_CREDENTIALS}"
  echo -n "${STORAGE_ACCOUNT}" > "${AZURE_APPLICATION_CREDENTIALS}/storageAccount"
  echo -n "${STORAGE_KEY}" > "${AZURE_APPLICATION_CREDENTIALS}/storageKey"

  echo "Setting up Azure infrastructure..."
  echo "Creating test bucket..."
  if [[ -n ${AZURE_EMULATOR_ENABLED:-""} ]]; then
    az storage container create --connection-string "${AZURE_STORAGE_CONNECTION_STRING}" --name "${TEST_ID}"
  else
    az storage container create --account-name "${STORAGE_ACCOUNT}" --account-key "${STORAGE_KEY}" --name "${TEST_ID}"
  fi
  echo "Successfully created test bucket."
  echo "Setting up Azure infrastructure completed."
}

function setup_azcli() {
  if $(which az > /dev/null); then
    return
  fi
  echo "Installing azure-cli"
  apt update
  apt install -y curl
  curl -sL https://aka.ms/InstallAzureCLIDeb | bash
  echo "Successfully installed azure-cli."
}

function usage_azure() {
  cat <<EOM
Usage:
run-e2e-test.sh azure

Please make sure the following environment variables are set:

    STORAGE_ACCOUNT                   Name of the storage account.
    STORAGE_KEY                       Key of the storage account.
    AZURE_STORAGE_API_ENDPOINT        URL of the Azure storage endpoint. ( optional: required for testing with Azurite)
    AZURE_EMULATOR_ENABLED            Set to "true" to Enable the Azure emulator for testing. ( optional: required for testing with Azurite)
    AZURITE_HOST                      Host of the Azurite service. ( optional: required for testing with Azurite)
    AZURE_STORAGE_CONNECTION_STRING   Connection string for the Azure storage account. ( optional: required for testing with Azurite)
EOM
  exit 0
}

function setup_azure_e2e() {
  if [[ -n ${AZURE_EMULATOR_ENABLED:-""} ]]; then
    ( [[ -z ${STORAGE_ACCOUNT:-""} ]] || [[ -z ${STORAGE_KEY:-""} ]] || [[ -z ${AZURE_STORAGE_API_ENDPOINT:-""} ]] || [[ -z ${AZURITE_HOST:-""} ]] ) && usage_azure
    make deploy-azurite
  else
    ( [[ -z ${STORAGE_ACCOUNT:-""} ]] || [[ -z ${STORAGE_KEY:-""} ]] ) && usage_azure
    echo "AZURE_EMULATOR_ENABLED is not set. Using Azure services for testing."
  fi
  setup_azcli
  setup_azure_infrastructure
}

run_cluster_tests() {
    if ! [ -x "$(command -v ginkgo)" ]; then
    setup_ginkgo
    fi
    
    get_test_id
    export ETCD_VERSION=${ETCD_VERSION:-"v0.1.1"}
    echo "Etcd version: ${ETCD_VERSION}"
    export ETCDBR_VERSION=${ETCDBR_VERSION:-${ETCDBR_VER:-"v0.28.0"}}
    echo "Etcd-backup-restore version: ${ETCDBR_VERSION}"

    # Setup the infrastructure for the providers in parallel to reduce the setup time.
    for p in ${1//,/ }; do
      case $p in
        aws) setup_aws_e2e & ;;
        gcp) setup_gcs_e2e & ;;
        azure) setup_azure_e2e & ;;
        *) echo "Provider: $p is not supported" ;;
        esac
    done
    wait

    echo "Starting e2e tests on k8s cluster"

    set +e

    failed_providers=""
    if [ -r "$KUBECONFIG" ]; then
      for p in ${1//,/ }; do
        STORAGE_CONTAINER=$TEST_ID PROVIDER=$p ginkgo -v -timeout=30m -mod=vendor test/e2e/integrationcluster
        TEST_RESULT=$?
        echo "Tests have run for provider $p with result $TEST_RESULT"
        if [ $TEST_RESULT -ne 0 ]; then
          echo "Tests failed for provider $p"
          failed_providers="$failed_providers,$p"
        fi
      done
      if [ -n "$failed_providers" ]; then
        echo "Tests failed for providers: $failed_providers"
      else 
        echo "Tests passed for all providers"
      fi
    else
      echo "Invalid kubeconfig for e2e test $KUBECONFIG"
      TEST_RESULT=255
    fi

    set -e

    cleanup $1
}

: ${INFRA_PROVIDERS:=""}
export INFRA_PROVIDERS=${1}

run_cluster_tests ${INFRA_PROVIDERS}
