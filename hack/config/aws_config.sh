#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
#
# SPDX-License-Identifier: Apache-2.0

# Env vars for AWS S3 to be used for etcdbr e2e tests #
LOCALSTACK_HOST="localstack.default:4566"
AWS_ENDPOINT_URL_S3="http://localhost:4566"
AWS_ACCESS_KEY_ID="ACCESSKEYAWSUSER"
AWS_SECRET_ACCESS_KEY="sEcreTKey"
AWS_DEFAULT_REGION=us-east-2

export AWS_APPLICATION_CREDENTIALS_JSON="/tmp/aws.json"
echo "{ \"accessKeyID\": \"${AWS_ACCESS_KEY_ID}\", \"secretAccessKey\": \"${AWS_SECRET_ACCESS_KEY}\", \"region\": \"${AWS_DEFAULT_REGION}\", \"endpoint\": \"${AWS_ENDPOINT_URL_S3}\" , \"s3ForcePathStyle\": true }" > "${AWS_APPLICATION_CREDENTIALS_JSON}"