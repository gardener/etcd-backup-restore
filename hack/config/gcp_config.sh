#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
#
# SPDX-License-Identifier: Apache-2.0

# Check if all required GCP environment variables are set
if [[ -z "${GCP_PROJECT_ID:-}" || -z "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]]; then
  if [[ -n "${GCP_PROJECT_ID:-}" || -n "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]]; then
    echo "Error: Partial GCP environment variables set. Please set both or none of GCP_PROJECT_ID and GOOGLE_APPLICATION_CREDENTIALS."
    exit 1
  fi
  # Use fake GCS emulator if none of the GCP environment variables are set
  export GOOGLE_EMULATOR_HOST="fake-gcs.default:8000"
  GCP_SERVICE_ACCOUNT_JSON="place-your-service-account-json-here"
  USE_FAKEGCS=true
else
  USE_FAKEGCS=false
fi

if [[ "${USE_FAKEGCS}" == "true" ]]; then
  rm -rf /tmp/.gcp
  mkdir -p /tmp/.gcp
  export EMULATOR_URL="http://localhost:8000/storage/v1/"
  export GOOGLE_ENDPOINT_OVERRIDE="http://${GOOGLE_EMULATOR_HOST}/storage/v1/"
  # Create a dummy service account JSON file
  cat <<EOF > /tmp/.gcp/service-account.json
  {
    "project_id": "dummy-project-id",
    "type": "service_account"
  }
EOF
  export GOOGLE_APPLICATION_CREDENTIALS="/tmp/.gcp/service-account.json"
fi
