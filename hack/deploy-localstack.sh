#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
#
# SPDX-License-Identifier: Apache-2.0
set -o errexit
set -o nounset
set -o pipefail

kubectl apply -f ./hack/e2e-test/infrastructure/localstack/localstack.yaml
kubectl rollout status deploy/localstack
kubectl wait --for=condition=ready pod -l app=localstack --timeout=240s