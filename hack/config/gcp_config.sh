#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
#
# SPDX-License-Identifier: Apache-2.0

# Config for Google Cloud Storage to be used for etcdbr e2e tests #
GOOGLE_EMULATOR_HOST="fake-gcs.default:8000"
GOOGLE_STORAGE_API_ENDPOINT="http://localhost:8000/storage/v1/"

export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account.json"
export GCP_PROJECT_ID="your-project-id"