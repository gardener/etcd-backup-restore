#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
#
# SPDX-License-Identifier: Apache-2.0

# Check if all required Azure environment variables are set
if [[ -z "${STORAGE_ACCOUNT:-}" || -z "${STORAGE_KEY:-}" ]]; then
  if [[ -n "${STORAGE_ACCOUNT:-}" || -n "${STORAGE_KEY:-}" ]]; then
    echo "Error: Partial Azure environment variables set. Please set both or none of STORAGE_ACCOUNT and STORAGE_KEY."
    exit 1
  fi
  # Use Azurite if none of the Azure environment variables are set
  AZURITE_DOMAIN_LOCAL="localhost:10000"
  export AZURITE_DOMAIN="azurite-service.default:10000"
  export STORAGE_ACCOUNT="devstoreaccount1"
  export STORAGE_KEY="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
  export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=http;AccountName=${STORAGE_ACCOUNT};AccountKey=${STORAGE_KEY};BlobEndpoint=http://${AZURITE_DOMAIN_LOCAL}/${STORAGE_ACCOUNT};"
  export USE_AZURITE=true
else
  export USE_AZURITE=false
fi

export AZURE_APPLICATION_CREDENTIALS="/tmp/.azure"
rm -rf "${AZURE_APPLICATION_CREDENTIALS}"
mkdir -p "${AZURE_APPLICATION_CREDENTIALS}"
echo -n "${STORAGE_ACCOUNT}"      > "${AZURE_APPLICATION_CREDENTIALS}/storageAccount"
echo -n "${STORAGE_KEY}"          > "${AZURE_APPLICATION_CREDENTIALS}/storageKey"

if [[ "${USE_AZURITE}" == "true" ]]; then
  echo -n "${AZURITE_DOMAIN_LOCAL}" > "${AZURE_APPLICATION_CREDENTIALS}/domain"
  echo -n "true"                    > "${AZURE_APPLICATION_CREDENTIALS}/emulatorEnabled"
fi
