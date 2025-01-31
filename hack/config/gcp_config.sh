#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
#
# SPDX-License-Identifier: Apache-2.0

# Config for Google Cloud Storage to be used for etcdbr e2e tests #
export GOOGLE_EMULATOR_HOST="fake-gcs.default:8000"              # Remove this line if you are not using fake-gcs
export GCP_PROJECT_ID="your-project-id"                          # Replace with your GCP project ID
GCP_SERVICE_ACCOUNT_JSON="place-your-service-account-json-here"  # Replace with your service account JSON
FAKEGCS_LOCAL_URL="http://localhost:8000/storage/v1/"            # Remove this line if you are not using fake-gcs

rm -rf /tmp/.gcp
mkdir -p /tmp/.gcp
echo -n "${GCP_SERVICE_ACCOUNT_JSON}"  > /tmp/.gcp/service-account.json  # Replace with your service account JSON
echo -n "${FAKEGCS_LOCAL_URL}"         > /tmp/.gcp/storageAPIEndpoint    # Remove this line if you are not using fake-gcs
echo -n "true"                         > /tmp/.gcp/emulatorEnabled       # Remove this line if you are not using fake-gcs

export GOOGLE_APPLICATION_CREDENTIALS="/tmp/.gcp/service-account.json"