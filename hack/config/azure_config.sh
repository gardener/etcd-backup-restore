#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
#
# SPDX-License-Identifier: Apache-2.0

# Config for Azure Blob Storage to be used for etcdbr e2e tests #
AZURITE_DOMAIN_LOCAL="localhost:10000"         # Remove this line if you are not using azurite
export STORAGE_ACCOUNT="devstoreaccount1"      # Replace with your Azure storage account
export STORAGE_KEY="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="   # Replace with your Azure storage key
export AZURITE_DOMAIN="azurite-service.default:10000"    # Remove this line if you are not using azurite
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=http;AccountName=${STORAGE_ACCOUNT};AccountKey=${STORAGE_KEY};BlobEndpoint=http://${AZURITE_DOMAIN_LOCAL}/${STORAGE_ACCOUNT};" # Remove this line if you are not using azurite

export AZURE_APPLICATION_CREDENTIALS="/tmp/.azure"
rm -rf "${AZURE_APPLICATION_CREDENTIALS}"
mkdir -p "${AZURE_APPLICATION_CREDENTIALS}"
echo -n "${STORAGE_ACCOUNT}"      > "${AZURE_APPLICATION_CREDENTIALS}/storageAccount"
echo -n "${STORAGE_KEY}"          > "${AZURE_APPLICATION_CREDENTIALS}/storageKey"
echo -n "${AZURITE_DOMAIN_LOCAL}" > "${AZURE_APPLICATION_CREDENTIALS}/domain"           # Remove this line if you are not using azurite
echo -n "true"                    > "${AZURE_APPLICATION_CREDENTIALS}/emulatorEnabled"  # Remove this line if you are not using azurite
