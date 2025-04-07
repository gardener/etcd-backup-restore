#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
#
# SPDX-License-Identifier: Apache-2.0

# Declare global variables
TEST_ID=
ETCD_VER=
ETCDBR_VER=
ETCD_DATA_DIR=
TEST_DIR=
TEST_RESULT=

set +e
test -d "${HOME}/.aws"
USE_EXISTING_AWS_SECRET=$?
set -e

TEST_ID_PREFIX="etcdbr-test"
TM_TEST_ID_PREFIX="etcdbr-tm-test"

# Get the directory where the build is written to
export BINARY_DIR="${BINARY_DIR:-"$(dirname "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")")/bin"}"
export PATH="${BINARY_DIR}:${PATH}"

function setup_test_environment() {
  setup_etcd
  setup_etcdbrctl
  setup_awscli
}

function setup_etcd(){
  echo "Downloading and installing etcd..."
  export ETCD_VER=v3.4.34
  if [[ $(uname) == 'Darwin' ]]; then
    curl -L https://storage.googleapis.com/etcd/${ETCD_VER}/etcd-${ETCD_VER}-darwin-amd64.zip -o etcd-${ETCD_VER}-darwin-amd64.zip
    unzip etcd-${ETCD_VER}-darwin-amd64.zip > /dev/null
    chmod +x ./etcd-${ETCD_VER}-darwin-amd64/etcd
    chmod +x ./etcd-${ETCD_VER}-darwin-amd64/etcdctl
    mv ./etcd-${ETCD_VER}-darwin-amd64/etcdctl ${TOOLS_BIN_DIR}/etcdctl
    mv ./etcd-${ETCD_VER}-darwin-amd64/etcd ${TOOLS_BIN_DIR}/etcd
    rm -rf ./etcd-${ETCD_VER}-darwin-amd64
    rm -rf etcd-${ETCD_VER}-darwin-amd64.zip
  else
    curl -L https://storage.googleapis.com/etcd/${ETCD_VER}/etcd-${ETCD_VER}-linux-amd64.tar.gz -o etcd-${ETCD_VER}-linux-amd64.tar.gz
    tar xzvf etcd-${ETCD_VER}-linux-amd64.tar.gz > /dev/null
    chmod +x ./etcd-${ETCD_VER}-linux-amd64/etcd
    chmod +x ./etcd-${ETCD_VER}-linux-amd64/etcdctl
    mv ./etcd-${ETCD_VER}-linux-amd64/etcdctl ${TOOLS_BIN_DIR}/etcdctl
    mv ./etcd-${ETCD_VER}-linux-amd64/etcd ${TOOLS_BIN_DIR}/etcd
    rm -rf ./etcd-${ETCD_VER}-linux-amd64
    rm -rf etcd-${ETCD_VER}-linux-amd64.tar.gz
  fi
  echo "Successfully installed etcd."
}

function setup_etcdbrctl(){
    echo "Installing etcdbrctl..."
    make build
    chmod +x ${BINARY_DIR:-"./bin"}/etcdbrctl
    echo "Successfully installed etcdbrctl."
}

# More information at https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html
function setup_awscli() {
    if command -v aws > /dev/null; then
      return
    fi
    echo "Installing awscli..."
    # apt since the golang image that runs the integration tests is debian based
    if [[ $(uname) == 'Linux' ]]; then
      apt update && apt install -y curl unzip > /dev/null
      curl "https://awscli.amazonaws.com/awscli-exe-$(uname -s | tr '[:upper:]' '[:lower:]')-$(uname -m).zip" -o "awscliv2.zip" > /dev/null
      unzip awscliv2.zip > /dev/null
      ./aws/install -i /usr/local/aws-cli -b /usr/local/bin
      rm -rf awscliv2.zip aws
    else
      curl "https://awscli.amazonaws.com/AWSCLIV2.pkg" -o "AWSCLIV2.pkg"
      sudo installer -pkg AWSCLIV2.pkg -target /
      rm -rf AWSCLIV2.pkg
    fi
    echo "Successfully installed awscli."
}

function get_test_id() {
  git_commit=`git show -s --format="%H"`
  export TEST_ID=${TEST_ID_PREFIX}-${git_commit}
  echo "Test id: ${TEST_ID}"
}

function get_tm_test_id() {
  export TEST_ID=${TM_TEST_ID_PREFIX}-${GIT_REVISION}
  echo "Test id: ${TEST_ID}"
}

function create_etcd_data_directory() {
  export TEST_DIR=${PWD}/test/e2e_test_data
  export ETCD_DATA_DIR=${TEST_DIR}/etcd-data
  mkdir -p ${ETCD_DATA_DIR}
}

function get_aws_existing_region() {
  export REGION=`cat ${HOME}/.aws/config | grep -e "^.*region.*$" | sed "s/^.*region[ ]*=[ ]*//"`
}

#############################
#        AWS Setup          #
#############################

function write_aws_secret() {
  echo "Creating aws credentials for API access..."
  mkdir ${HOME}/.aws
  cat << EOF > ${HOME}/.aws/credentials
[default]
aws_access_key_id = $1
aws_secret_access_key = $2
EOF
  cat << EOF > ${HOME}/.aws/config
[default]
region = $3
EOF
  temp_dir=$(mktemp -d)
  credentials_file="${temp_dir}/credentials.json"
  cat <<EOF >"${credentials_file}"
{
  "accessKeyID": "$1",
  "secretAccessKey": "$2",
  "region": "$3"
}
EOF
  export AWS_APPLICATION_CREDENTIALS_JSON="${credentials_file}"
}

function create_aws_secret() {
  apt update && apt install -y pip > /dev/null
  pip install --break-system-packages gardener-cicd-cli > /dev/null
  echo "Fetching aws credentials from secret server..."
  export ACCESS_KEY_ID=`gardener-ci config $CC_CACHE_FILE_FLAG attribute --cfg-type aws --cfg-name etcd-backup-restore --key access_key_id`
  export SECRET_ACCESS_KEY=`gardener-ci config $CC_CACHE_FILE_FLAG attribute --cfg-type aws --cfg-name etcd-backup-restore --key secret_access_key`
  export REGION=`gardener-ci config $CC_CACHE_FILE_FLAG attribute --cfg-type aws --cfg-name etcd-backup-restore --key region`
  echo "Successfully fetched aws credentials from secret server."

  write_aws_secret "${ACCESS_KEY_ID}" "${SECRET_ACCESS_KEY}" "${REGION}"

  echo "Successfully created aws credentials."
}

function delete_aws_secret() {
  rm -rf ${HOME}/.aws
}

function create_s3_bucket() {
  echo "Creating S3 bucket ${TEST_ID} in region ${REGION}"
  aws s3api create-bucket --bucket ${TEST_ID} --region ${REGION} --create-bucket-configuration LocationConstraint=${REGION} --acl private
  # Block public access to the S3 bucket
  aws s3api put-public-access-block --bucket ${TEST_ID} --public-access-block-configuration "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"
  # Deny non-HTTPS requests to the S3 bucket
  aws s3api put-bucket-policy --bucket ${TEST_ID} --policy "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Deny\",\"Principal\":\"*\",\"Action\":\"s3:*\",\"Resource\":[\"arn:aws:s3:::${TEST_ID}\",\"arn:aws:s3:::${TEST_ID}/*\"],\"Condition\":{\"Bool\":{\"aws:SecureTransport\":\"false\"},\"NumericLessThan\":{\"s3:TlsVersion\":\"1.2\"}}}]}"
}

function delete_s3_bucket() {
  echo "Deleting S3 bucket ${TEST_ID}"
  aws s3 rb s3://${TEST_ID} --force
}

function setup-aws-infrastructure() {
  echo "Setting up AWS infrastructure..."
  if [[ "${USE_EXISTING_AWS_SECRET}" == "1" ]]; then
    create_aws_secret
  else
    get_aws_existing_region
  fi
  create_s3_bucket
  echo "AWS infrastructure setup completed."
}

function cleanup-aws-infrastructure() {
  echo "Cleaning up AWS infrastructure..."
  delete_s3_bucket
  if [[ "${USE_EXISTING_AWS_SECRET}" == "1" ]]; then
    delete_aws_secret
  fi
  echo "AWS infrastructure cleanup completed."
}

function remove-etcd-data-directory() {
   echo "Removing ETCD Data Directory"
   rm -rf ${ETCD_DATA_DIR}
 }

#############################
#        Azure Setup        #
#############################
function create_azure_secret() {
  echo "Creating Azure secret"
}

#############################
#        GCP Setup          #
#############################
function create_gcp_secret() {
echo "Creating GCP secret"
}

#############################
#        Openstack Setup    #
#############################
function create_openstack_secret() {
echo "Creating Openstack secret"
}

##############################################################################
function setup_test_cluster() {
  get_test_id
  setup-aws-infrastructure
  create_gcp_secret
  create_azure_secret
  create_openstack_secret
  create_etcd_data_directory
}

function cleanup_test_environment() {
  cleanup-aws-infrastructure
  remove-etcd-data-directory
}

###############################################################################

function run_test_as_processes() {
  setup_test_environment
  echo "Setting up test cluster..."
  setup_test_cluster

  echo "Starting integration tests..."
  cd test/integration

  set +e
  ginkgo -r -mod=vendor
  TEST_RESULT=$?
  set -e

  echo "Done with integration tests."

  echo "Deleting test enviornment..."
  cleanup_test_environment
  echo "Successfully completed all tests."

  if [ ${TEST_RESULT} -ne 0 ]; then
    echo "Printing etcdbrctl.log:"
    cat ${TEST_DIR}/etcdbrctl.log
  fi
}

run_test_as_processes

exit $TEST_RESULT
