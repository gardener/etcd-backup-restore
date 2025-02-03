#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
#
# SPDX-License-Identifier: Apache-2.0

# Env vars for AWS S3 to be used for etcdbr e2e tests #
export LOCALSTACK_HOST="localstack.default:4566"   # Remove this line if you are not using localstack
export AWS_ENDPOINT_URL_S3="http://localhost:4566" # Remove this line if you are not using localstack
export AWS_ACCESS_KEY_ID="ACCESSKEYAWSUSER"        # Replace with your AWS access key
export AWS_SECRET_ACCESS_KEY="sEcreTKey"           # Replace with your AWS secret key
export AWS_DEFAULT_REGION="us-east-2"              # Replace with your AWS region

export AWS_APPLICATION_CREDENTIALS="/tmp/.aws"

rm -rf /tmp/.aws
mkdir -p /tmp/.aws
echo -n "${AWS_ACCESS_KEY_ID}"     > "${AWS_APPLICATION_CREDENTIALS}/accessKeyID"
echo -n "${AWS_SECRET_ACCESS_KEY}" > "${AWS_APPLICATION_CREDENTIALS}/secretAccessKey"
echo -n "${AWS_DEFAULT_REGION}"    > "${AWS_APPLICATION_CREDENTIALS}/region"
echo -n "${AWS_ENDPOINT_URL_S3}"   > "${AWS_APPLICATION_CREDENTIALS}/endpoint"           # Remove this line if you are not using localstack
echo -n "true"                     > "${AWS_APPLICATION_CREDENTIALS}/s3ForcePathStyle"   # Remove this line if you are not using localstack
