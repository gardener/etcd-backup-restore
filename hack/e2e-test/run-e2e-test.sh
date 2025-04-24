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

# container_deletion_trap deletes the containers for the providers upon receiving a trap signal in the test() function
function container_deletion_trap() {
  delete_containers $INFRA_PROVIDERS
  exit 1
}

# get_test_id generates a unique test id based on the git commit
function get_test_id() {
  git_commit=`git show -s --format="%H"`
  export TEST_ID=${TEST_ID_PREFIX}-${git_commit}
  echo "Test id: ${TEST_ID}"
}

# cleanup_aws_container deletes the container for the AWS provider
function cleanup_aws_container() {
    echo "Cleaning up AWS infrastructure..."
    echo "Deleting test bucket..."
    result=$(aws s3api get-bucket-location --bucket ${TEST_ID} 2>&1 || true)
    if [[ $result == *NoSuchBucket* ]]; then
      echo "Bucket doesn't exit. Might have been deleted already."
      return
    fi
    if ! aws s3 rb s3://${TEST_ID} --force; then
      echo "Failed to delete AWS test bucket."
      return 1
    fi
    echo "Successfully deleted AWS test bucket."
    unset LOCALSTACK_HOST AWS_DEFAULT_REGION
    echo "Cleaning up AWS infrastructure completed."
}

# cleanup_gcp_container deletes the container for the GCP provider
function cleanup_gcp_container() {
  echo "Cleaning up GCS infrastructure..."
  if [[ -z ${GOOGLE_EMULATOR_HOST:-""} ]]; then
    echo "Deleting GCS bucket ${TEST_ID} ..."
    if ! gsutil rm -r gs://"${TEST_ID}"/; then
      echo "Failed to delete GCS bucket ${TEST_ID}."
      return 1
    fi
    echo "Successfully deleted GCS bucket ${TEST_ID} ."
  else 
    echo "Deleting GCS bucket ${TEST_ID} ..."
    gsutil -o "Credentials:gs_json_host=127.0.0.1" -o "Credentials:gs_json_port=4443" -o "Boto:https_validate_certificates=False" rm -rf gs://${TEST_ID}/
    echo "Successfully deleted GCS bucket ${TEST_ID} ."
  fi

  unset GOOGLE_EMULATOR_HOST GOOGLE_APPLICATION_CREDENTIALS GCP_PROJECT_ID
  echo "Cleaning up GCS infrastructure completed."
}

# cleanup_azure_container deletes the container for the Azure provider
function cleanup_azure_container() {
  echo "Cleaning up Azure infrastructure..."
  echo "Deleting test container..."
  if [[ -n ${AZURITE_DOMAIN:-""} ]]; then
    if ! az storage container delete --connection-string "${AZURE_STORAGE_CONNECTION_STRING}" --name "${TEST_ID}"; then
      echo "Failed to delete Azure test bucket."
      return 1
    fi
  else
    if ! az storage container delete --account-name "${STORAGE_ACCOUNT}" --account-key "${STORAGE_KEY}" --name "${TEST_ID}"; then
      echo "Failed to delete Azure test bucket."
      return 1
    fi
  fi
  echo "Successfully deleted test container."
  unset STORAGE_ACCOUNT STORAGE_KEY AZURITE_DOMAIN AZURE_STORAGE_CONNECTION_STRING
  echo "Cleaning up Azure infrastructure completed."
}

# setup_awscli installs the awscli
function setup_awscli() {
    if $(which aws > /dev/null); then
      return
    fi
    echo "Installing awscli..."
    apt update
    apt install -y curl
    apt install -y unzip
    cd $HOME
    curl -Lo "awscliv2.zip" "https://awscli.amazonaws.com/awscli-exe-$(uname -s | tr '[:upper:]' '[:lower:]')-$(uname -m).zip"
    unzip awscliv2.zip > /dev/null
    ./aws/install -i /usr/local/aws-cli -b /usr/local/bin
    echo "Successfully installed awscli."
}

# create_aws_container creates the container for the AWS provider
function create_aws_container() {
    echo "Setting up AWS infrastructure..."
    echo "Creating test bucket..."
    result=$(aws s3api get-bucket-location --bucket ${TEST_ID} 2>&1 || true)
    if [[ $result == *NoSuchBucket* ]]; then
      echo "Creating S3 bucket ${TEST_ID} in region ${AWS_DEFAULT_REGION}"
      accumulated_exit_code=0
      aws s3api create-bucket --bucket ${TEST_ID} --region ${AWS_DEFAULT_REGION} --create-bucket-configuration LocationConstraint=${AWS_DEFAULT_REGION} --acl private || accumulated_exit_code=1
      # Block public access to the S3 bucket
      aws s3api put-public-access-block --bucket ${TEST_ID} --public-access-block-configuration "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true" || accumulated_exit_code=1
      # Deny non-HTTPS requests to the S3 bucket, except for localstack which is exposed on an HTTP endpoint
      if [[ -z "${LOCALSTACK_HOST:-}" ]]; then
        aws s3api put-bucket-policy --bucket ${TEST_ID} --policy "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Deny\",\"Principal\":\"*\",\"Action\":\"s3:*\",\"Resource\":[\"arn:aws:s3:::${TEST_ID}\",\"arn:aws:s3:::${TEST_ID}/*\"],\"Condition\":{\"Bool\":{\"aws:SecureTransport\":\"false\"},\"NumericLessThan\":{\"s3:TlsVersion\":\"1.2\"}}}]}" || accumulated_exit_code=1
      fi
      if [ $accumulated_exit_code -ne 0 ]; then
        echo "Failed to create AWS test bucket."
        return 1
      fi
    else
      echo $result
      if [[ $result != *${AWS_DEFAULT_REGION}* ]]; then
        echo "Bucket already exists in a different region. Please delete the bucket and try again."
        return 1
      fi
    fi
    echo "Successfully created test bucket."
    echo "Setting up AWS infrastructure completed."
}

# usage_aws prints the usage for the AWS provider
function usage_aws {
    cat <<EOM
Usage:

Please make sure the following environment variables are set:

For real AWS provider:
    AWS_ACCESS_KEY_ID       Access key of the AWS user.
    AWS_SECRET_ACCESS_KEY   Secret key of the AWS user.
    AWS_DEFAULT_REGION      Region in which the test bucket is created.

For testing with Localstack:
    LOCALSTACK_HOST         Host of the localstack service.
    AWS_ENDPOINT_URL_S3     Endpoint URL of the localstack service.
    AWS_ACCESS_KEY_ID       Dummy access key of the AWS user.
    AWS_SECRET_ACCESS_KEY   Dummy secret key of the AWS user.
    AWS_DEFAULT_REGION      Region in which the test bucket should be created.
EOM
    return 1
}

# setup_aws_e2e sets up the AWS infrastructure for the e2e tests including deploying localstack and/or installing the awscli
function setup_aws_e2e() {    
    if [[ -n ${LOCALSTACK_HOST:-""} ]]; then
      make deploy-localstack $KUBECONFIG
    else
      if [[ -z ${AWS_DEFAULT_REGION:-""} ]]; then
          usage_aws
      fi
      echo "LOCALSTACK_HOST is not set. Using real AWS infra for testing."
    fi
    setup_awscli
}

# create_gcp_container creates the container for the GCP provider
function create_gcp_container() {
  echo "Setting up GCS infrastructure..."
  echo "Creating test bucket..."
  if [[ -z ${GOOGLE_EMULATOR_HOST:-""} ]]; then
    if ! gsutil mb "gs://${TEST_ID}"; then
      echo "Failed to create GCS bucket ${TEST_ID}."
      return 1
    fi
  else 
    if ! gsutil -o "Credentials:gs_json_host=127.0.0.1" -o "Credentials:gs_json_port=4443" -o "Boto:https_validate_certificates=False" mb "gs://${TEST_ID}"; then
      echo "Failed to create GCS bucket ${TEST_ID}."
      return 1
    fi
  fi
  echo "Successfully created test bucket."
  echo "Setting up GCS infrastructure completed."
}

# usage_gcp prints the usage for the GCP provider
function usage_gcp() {
  cat <<EOM
Usage:

Please make sure the following environment variables are set:

For real GCP provider:
    GOOGLE_APPLICATION_CREDENTIALS    Path to the service account key file.
    GCP_PROJECT_ID                    Project ID of the GCP project.

For testing with Fake GCS:
    GOOGLE_EMULATOR_HOST              Host of the fake-gcs service.

EOM
  return 1
}

# setup_gcloud installs the gcloud sdk
function setup_gcloud() {
  if $(which gcloud > /dev/null); then
    return
  fi
  echo "Installing gcloud..."
  cd $HOME
  apt update
  apt install -y curl
  apt install -y python3
  curl -Lo "google-cloud-sdk.tar.gz" https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-503.0.0-$(uname -s | tr '[:upper:]' '[:lower:]')-$(uname -m | sed 's/aarch64/arm/').tar.gz
  tar -xzf google-cloud-sdk.tar.gz
  ./google-cloud-sdk/install.sh -q
  export PATH=$PATH:${HOME}/google-cloud-sdk/bin
  cd "${SOURCE_PATH}"
  echo "Successfully installed gcloud."
}

# authorize_gcloud authorizes access to Gcloud
function authorize_gcloud() {
  if ! $(which gcloud > /dev/null); then
    echo "gcloud is not installed. Please install gcloud and try again."
    return 1
  fi
  if [[ -n ${GOOGLE_EMULATOR_HOST:-""} ]]; then
    gcloud config set project "dummy-project"
    return 0
  fi
  echo "Authorizing access to Gcloud..."
  if gcloud auth activate-service-account --key-file="${GOOGLE_APPLICATION_CREDENTIALS}" --project="${GCP_PROJECT_ID}"; then
    echo "Successfully authorized Gcloud."
  else
    echo "Failed to authorize Gcloud."
    return 1
  fi
}

# setup_gcp_e2e sets up the GCP infrastructure for the e2e tests including deploying installing the gcloud sdk and/or fake-gcs
function setup_gcp_e2e() {
  setup_gcloud
  authorize_gcloud
  if [[ -n ${GOOGLE_EMULATOR_HOST:-""} ]]; then
    make deploy-fakegcs $KUBECONFIG
  else
    echo "GOOGLE_EMULATOR_HOST is not set. Using real GCS infra for testing."
    if [[ -z ${GCP_PROJECT_ID:-""} ]] || [[ -z ${GOOGLE_APPLICATION_CREDENTIALS} ]]; then
        usage_gcp
    fi
  fi
}

# create_azure_container creates the container for the Azure provider
function create_azure_container() {
  echo "Setting up Azure infrastructure..."
  echo "Creating test bucket..."
  if [[ -n ${AZURITE_DOMAIN:-""} ]]; then
    if ! az storage container create --connection-string "${AZURE_STORAGE_CONNECTION_STRING}" --name "${TEST_ID}"; then
      echo "Failed to create Azure test bucket."
      return 1
    fi
  else
    if ! az storage container create --account-name "${STORAGE_ACCOUNT}" --account-key "${STORAGE_KEY}" --name "${TEST_ID}"; then
      echo "Failed to create Azure test bucket."
      return 1
    fi
  fi
  echo "Successfully created test bucket."
  echo "Setting up Azure infrastructure completed."
}

# setup_azcli installs the azcli
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

# usage_azure prints the usage for the Azure provider
function usage_azure() {
  cat <<EOM
Usage:

Please make sure the following environment variables are set:

For real Azure provider:
    STORAGE_ACCOUNT                   Name of the storage account.
    STORAGE_KEY                       Key of the storage account.

For testing with Azurite:
    AZURITE_DOMAIN                    Host of the Azurite service.
    AZURE_STORAGE_CONNECTION_STRING   Connection string for the Azure storage account.
EOM
  return 1
}

# setup_azure_e2e sets up the Azure infrastructure for the e2e tests including deploying Azurite and/or installing the azcli
function setup_azure_e2e() {
  if [[ -z ${STORAGE_ACCOUNT:-""} ]] || [[ -z ${STORAGE_KEY:-""} ]]; then
      usage_azure
  fi
  if [[ -n ${AZURITE_DOMAIN:-""} ]]; then
    if [[ -z ${AZURE_STORAGE_CONNECTION_STRING:-""} ]]; then
        usage_azure
    fi
    make deploy-azurite $KUBECONFIG
  else
    echo "AZURITE_DOMAIN is not set. Using real Azure infra for testing."
  fi
  setup_azcli
}

# build_and_load builds the etcdbr image and loads it into the k8s cluster
build_and_load() {
    echo "Building the image and loading it into the k8s cluster..."
    docker build -t ${ETCDBR_IMAGE}:${ETCDBR_VERSION} -f build/Dockerfile .
    kind load docker-image ${ETCDBR_IMAGE}:${ETCDBR_VERSION} --name etcdbr-e2e
    echo "Successfully loaded the image into the k8s cluster."
}

# create_containers creates the containers for the providers
function create_containers() {
  for p in ${1//,/ }; do
    case $p in
      aws) create_aws_container;;
      gcp) create_gcp_container;;
      azure) create_azure_container;;
      *) echo "Provider: $p is not supported" ;;
      esac
      # if any of the container creation fails, return 1 to indicate failure
      if [ $? -ne 0 ]; then
        return 1
      fi
  done
}

# delete_containers deletes the containers for the providers
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

# test creates the containers for the providers and runs the e2e tests on the k8s cluster for each provider and deletes the containers after the tests
function test() {
  if containsElement $STEPS "test"; then
    set +e
    get_test_id
    echo "Creating the containers for the providers..."
    if ! create_containers $INFRA_PROVIDERS; then
      echo "Failed to create the containers for all the providers."
      delete_containers $INFRA_PROVIDERS
      exit 1
    fi
    trap container_deletion_trap INT TERM

    echo "Starting e2e tests on k8s cluster"

    failed_providers=""
    for p in ${INFRA_PROVIDERS//,/ }; do
      STORAGE_CONTAINER=$TEST_ID PROVIDER=$p ginkgo -v -timeout=30m -mod=vendor test/e2e
      TEST_RESULT=$?
      echo "Tests have run for provider $p with result $TEST_RESULT"
      if [ $TEST_RESULT -ne 0 ] && [ $TEST_RESULT -ne 197 ]; then
        echo "Tests failed for provider $p"
        failed_providers="$failed_providers,$p"
      fi
    done

    # Remove the containers after the tests
    delete_containers $INFRA_PROVIDERS

    if [ -n "$failed_providers" ]; then
      echo "Tests failed for providers: $failed_providers"
      exit 1
    else 
      echo "Tests passed for all providers"
    fi
    set -e
    trap - INT TERM
  fi
}

# setup sets up the infrastructure for the providers like building and loading the etcdbr image, creating buckets for the emulators, installing the cli tools to interact with the providers, etc.
function setup() {
  if containsElement $STEPS "setup"; then
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
        exit 1
      fi
    done
    echo "Setup completed for all providers."
  fi
}

: ${INFRA_PROVIDERS:=${1}}
: ${KUBECONFIG:=$(pwd)/hack/e2e-test/infrastructure/kind/kubeconfig}
: ${STEPS:="setup,test"}

for p in ${INFRA_PROVIDERS//,/ }; do
  case $p in
    aws)
        source $(pwd)/hack/config/aws_config.sh
        ;;
    gcp)
        source $(pwd)/hack/config/gcp_config.sh
        ;;
    azure)
        source $(pwd)/hack/config/azure_config.sh
        ;;
    *)
        echo "Unknown provider: $p"
        ;;
  esac
done

setup
test
