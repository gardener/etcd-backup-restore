#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
#
# SPDX-License-Identifier: Apache-2.0

set -o errexit
set -o nounset
set -o pipefail

function containsElement () {
  array=(${1//,/ })
  for i in "${!array[@]}"
  do
      if [[ "${array[i]}" == "$2" ]]; then
        return 0
      fi
  done
  return 1
}

if [[ $(uname) == 'Darwin' ]]; then
  READLINK_BIN="greadlink"
else
  READLINK_BIN="readlink"
fi

# SOURCE_PATH - path to component repository root directory.
export SOURCE_PATH="$(${READLINK_BIN} -f "$(dirname ${0})/..")"

TEST_ID_PREFIX="etcdbr-e2e-test"

export GOBIN="${SOURCE_PATH}/bin"
export PATH="${GOBIN}:${PATH}"
SOURCE_PATH=$(dirname "${SOURCE_PATH}")
cd "${SOURCE_PATH}"

function teardown_trap() {
  delete_containers $INFRA_PROVIDERS
  if [[ ${cleanup_done:="false"} != "true" ]]; then
    cleanup_required="true"
    cleanup
  fi
}

function get_test_id() {
  git_commit=`git show -s --format="%H"`
  export TEST_ID=${TEST_ID_PREFIX}-${git_commit}
  echo "Test id: ${TEST_ID}"
}

function cleanup_aws_container() {
    echo "Cleaning up AWS infrastructure..."
    echo "Deleting test bucket..."
    result=$(aws s3api get-bucket-location --bucket ${TEST_ID} 2>&1 || true)
    if [[ $result == *NoSuchBucket* ]]; then
      echo "Bucket doesn't exit. Might have been deleted already."
      return
    fi
    aws s3 rb s3://${TEST_ID} --force
    echo "Successfully deleted test bucket."
    
    unset LOCALSTACK_HOST AWS_ENDPOINT_URL_S3 AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION
    echo "Cleaning up AWS infrastructure completed."
}

function cleanup_gcp_container() {
  echo "Cleaning up GCS infrastructure..."
  if [[ -z ${GOOGLE_EMULATOR_ENABLED:-""} ]]; then
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
    gsutil -o "Credentials:gs_json_host=127.0.0.1" -o "Credentials:gs_json_port=4443" -o "Boto:https_validate_certificates=False" -m rm -r gs://"${TEST_ID}"/ > /dev/null 2>&1 || true
    echo "Successfully deleted GCS bucket ${TEST_ID} ."
  fi

  unset GOOGLE_EMULATOR_HOST GOOGLE_EMULATOR_ENABLED GOOGLE_STORAGE_API_ENDPOINT GOOGLE_APPLICATION_CREDENTIALS GCP_PROJECT_ID
  echo "Cleaning up GCS infrastructure completed."
}

function cleanup_azure_container() {
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

function setup_awscli() {
    if ! $(which aws > /dev/null); then
      echo "Installing awscli..."
      pip3 install --break-system-packages awscli
      echo "Successfully installed awscli."
    else
      echo "awscli is already installed."
    fi
}

function create_aws_container() {
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

Please make sure the following environment variables are set:
    AWS_ACCESS_KEY_ID       Key ID of the user.
    AWS_SECRET_ACCESS_KEY   Access key of the user.
    AWS_DEFAULT_REGION      Region in which the test bucket is created.
    LOCALSTACK_HOST         Host of the localstack service. ( required only for testing with localstack)
    AWS_ENDPOINT_URL_S3     URL of the S3 endpoint. ( required only for testing with localstack)
EOM
    exit 1
}

function setup_aws_e2e() {    
    if [[ -z ${AWS_ACCESS_KEY_ID:-""} ]] || [[ -z ${AWS_SECRET_ACCESS_KEY:=""} ]]  || [[ -z ${AWS_DEFAULT_REGION:=""} ]]; then
        usage_aws
    fi
    if [[ -n ${LOCALSTACK_HOST:-""} ]]; then
      make deploy-localstack
    else 
      echo "LOCALSTACK_HOST is not set. Using real AWS infra for testing."
    fi
    setup_awscli
}

function setup_gsutil() {
  if ! $(which gsutil > /dev/null); then
    echo "Installing gsutil..."
    pip3 install gsutil
    echo "Successfully installed gsutil."
  else
    echo "gsutil is already installed."
  fi
}

function create_gcp_container() {
  echo "Setting up GCS infrastructure..."
  echo "Creating test bucket..."
  if [[ -z ${GOOGLE_EMULATOR_ENABLED:-""} ]]; then
    gsutil mb "gs://${TEST_ID}"
  else 
    gsutil -o "Credentials:gs_json_host=127.0.0.1" -o "Credentials:gs_json_port=4443" -o "Boto:https_validate_certificates=False" mb "gs://${TEST_ID}"
  fi
  echo "Successfully created test bucket."
  echo "Setting up GCS infrastructure completed."
}

function usage_gcp() {
  cat <<EOM
Usage:

Please make sure the following environment variables are set:
    GOOGLE_APPLICATION_CREDENTIALS    Path to the service account key file. 
    GCP_PROJECT_ID                    Project ID of the GCP project.
    GOOGLE_EMULATOR_HOST              Host of the fake GCS server. ( required only for fakegcs )
    GOOGLE_EMULATOR_ENABLED           Set to "true" to Enable the fake GCS server for testing. ( required only for fakegcs )
    GOOGLE_STORAGE_API_ENDPOINT       URL of the GCS storage endpoint ( required only for fakegcs )
EOM
  exit 1
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

function setup_gcp_e2e() {
  if [[ -n ${GOOGLE_EMULATOR_ENABLED:-""} ]]; then
    if [[ -z ${GOOGLE_EMULATOR_HOST:-""} ]] || [[ -z ${GOOGLE_STORAGE_API_ENDPOINT:-""} ]]; then
        usage_gcp
    fi
    make deploy-fakegcs
  else
    if [[ -z ${GCP_PROJECT_ID:-""} ]] || [[ -z ${GOOGLE_APPLICATION_CREDENTIALS} ]]; then
        usage_gcp
    fi
    echo "GOOGLE_EMULATOR_ENABLED is not set. Using real GCS infra for testing."
    authorize_gcloud
  fi
  setup_gsutil
}

function create_azure_container() {
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
  if ! $(which az > /dev/null); then
    echo "Installing azure-cli"
    apt update
    apt install -y curl
    curl -sL https://aka.ms/InstallAzureCLIDeb | bash
    echo "Successfully installed azure-cli."
  else
    echo "azure-cli is already installed."
  fi
}

function usage_azure() {
  cat <<EOM
Usage:

Please make sure the following environment variables are set:

    STORAGE_ACCOUNT                   Name of the storage account.
    STORAGE_KEY                       Key of the storage account.
    AZURE_STORAGE_API_ENDPOINT        URL of the Azure storage endpoint. ( required only for testing with Azurite)
    AZURE_EMULATOR_ENABLED            Set to "true" to Enable the Azure emulator for testing. ( required only for testing with Azurite)
    AZURITE_HOST                      Host of the Azurite service. ( required only for testing with Azurite)
    AZURE_STORAGE_CONNECTION_STRING   Connection string for the Azure storage account. ( required only for testing with Azurite)
EOM
  exit 1
}

function setup_azure_e2e() {
  if [[ -z ${STORAGE_ACCOUNT:-""} ]] || [[ -z ${STORAGE_KEY:-""} ]]; then
      usage_azure
  fi
  if [[ -n ${AZURE_EMULATOR_ENABLED:-""} ]]; then
    if [[ -z ${AZURE_STORAGE_API_ENDPOINT:-""} ]] || [[ -z ${AZURITE_HOST:-""} ]] || [[ -z ${AZURE_STORAGE_CONNECTION_STRING:-""} ]]; then
        usage_azure
    fi
    make deploy-azurite
  else
    echo "AZURE_EMULATOR_ENABLED is not set. Using real Azure infra for testing."
  fi
  setup_azcli
}

build_and_load() {
    echo "Building the image and loading it into the kind cluster..."
    if ! docker image ls | grep -E "^\s*${ETCDBR_IMAGE}\s+${ETCDBR_VERSION}\s+"; then
      docker build -t ${ETCDBR_IMAGE}:${ETCDBR_VERSION} -f build/Dockerfile .
    fi
    kind load docker-image ${ETCDBR_IMAGE}:${ETCDBR_VERSION} --name etcdbr-e2e
    echo "Successfully loaded the image into the kind cluster."
}

function create_containers() {
  for p in ${1//,/ }; do
    case $p in
      aws) create_aws_container;;
      gcp) create_gcp_container;;
      azure) create_azure_container;;
      *) echo "Provider: $p is not supported" ;;
      esac
  done
}

function delete_containers() {
  for p in ${1//,/ }; do
    case $p in
      aws) cleanup_aws_container;;
      gcp) cleanup_gcp_container;;
      azure) cleanup_azure_container;;
      *) echo "Provider: $p is not supported" ;;
      esac
  done
}

function cleanup() {
  if containsElement $STEPS "cleanup" || [ $cleanup_required = true ]; then
    kind delete cluster --name etcdbr-e2e
    echo "Cleaning up completed."
    cleanup_done="true"
    cleanup_required="false"
  fi
}

function test() {
  if containsElement $STEPS "test"; then
    trap teardown_trap INT TERM
    get_test_id
    echo "Creating the containers for the providers..."
    create_containers $INFRA_PROVIDERS

    echo "Starting e2e tests on k8s cluster"

    set +e
    failed_providers=""
    for p in ${INFRA_PROVIDERS//,/ }; do
      STORAGE_CONTAINER=$TEST_ID PROVIDER=$p ginkgo -v -timeout=30m -mod=vendor test/integrationcluster
      TEST_RESULT=$?
      echo "Tests have run for provider $p with result $TEST_RESULT"
      if [ $TEST_RESULT -ne 0 ] && [ $TEST_RESULT -ne 197 ]; then
        echo "Tests failed for provider $p"
        failed_providers="$failed_providers,$p"
      fi
    done
    set -e

    # Remove the containers after the tests
    delete_containers $INFRA_PROVIDERS

    if [ -n "$failed_providers" ]; then
      echo "Tests failed for providers: $failed_providers"
      exit 1
    else 
      echo "Tests passed for all providers"
    fi
  fi
}

function setup() {
  if containsElement $STEPS "setup"; then
    trap teardown_trap INT TERM
    build_and_load
    # Setup the infrastructure for the providers in parallel
    pids=()
    providers=()
    index=0
    for p in ${INFRA_PROVIDERS//,/ }; do
      providers[index]=$p
      case $p in
        aws) setup_aws_e2e & pids[index]=$! ;;
        gcp) setup_gcp_e2e & pids[index]=$! ;;
        azure) setup_azure_e2e & pids[index]=$! ;;
        *) echo "Provider: $p is not supported" ;;
        esac
        index=$((index+1))
    done
    # Wait for all the provider setups to complete
    for i in ${!pids[@]}; do
      set +e
      wait ${pids[$i]}
      provider_status=$?
      set -e
      if [ $provider_status -ne 0 ]; then
        echo "Setup failed for provider ${providers[$i]}"
        cleanup_required="true"
        cleanup
        exit 1
      fi
    done
    echo "Setup completed for all providers."
  fi
}

function create_or_target_cluster() {
  if containsElement $STEPS "setup"; then
    # Create a kind cluster for the e2e tests
    make kind-up || exit 1
    kubectl wait --for=condition=ready node --all
    if [ ! -r "$KUBECONFIG" ]; then
      echo "KUBECONFIG is not set or unreadable, please set the KUBECONFIG environment variable to a valid kubeconfig file."
      exit 1
    fi
  else
    # Target the existing kind cluster
    if containsElement $STEPS "test" || containsElement $STEPS "cleanup"; then
      # Fetch the kubeconfig file for the kind cluster
      export KUBECONFIG=$(pwd)/hack/e2e-test/infrastructure/kind/kubeconfig
      if [ ! -r "$KUBECONFIG" ]; then
        echo "KUBECONFIG is unreadable, please set the KUBECONFIG environment variable to a valid kubeconfig file."
        exit 1
      fi
    fi
  fi 
}

: ${INFRA_PROVIDERS:=${1}}
: ${STEPS:="setup,test,cleanup"}
: ${cleanup_done:="false"}
: ${cleanup_required:="false"}

create_or_target_cluster
setup
test
cleanup
