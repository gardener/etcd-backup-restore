#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
#
# SPDX-License-Identifier: Apache-2.0
set -o errexit
set -o nounset
set -o pipefail

if [[ -z "${LOCALSTACK_AUTH_TOKEN:-}" ]]; then
  printf '%s\n' "Error: LOCALSTACK_AUTH_TOKEN is not set. Get a token from https://app.localstack.cloud." >&2
  exit 1
fi

kubectl --kubeconfig=${KUBECONFIG} create secret generic localstack-auth \
  --from-literal=auth-token="${LOCALSTACK_AUTH_TOKEN}" \
  --dry-run=client -o yaml | kubectl --kubeconfig=${KUBECONFIG} apply -f -

KUBECONFIG=$1

kubectl --kubeconfig=${KUBECONFIG} apply -f ./hack/e2e-test/infrastructure/localstack/localstack.yaml
kubectl --kubeconfig=${KUBECONFIG} rollout status deploy/localstack
kubectl --kubeconfig=${KUBECONFIG} wait --for=condition=ready pod -l app=localstack --timeout=240s

# Wait for LocalStack to be reachable from the host via the kind node port mapping.
# The pod may be ready before the hostPort is actually forwarded.
printf '%s' "Waiting for LocalStack to be reachable on localhost:4566"
for i in $(seq 1 60); do
  if curl -sf http://localhost:4566/_localstack/health > /dev/null 2>&1; then
    printf '\n%s\n' "LocalStack is ready."
    exit 0
  fi
  printf '%s' "."
  sleep 2
done
printf '\n%s\n' "Error: LocalStack did not become reachable on localhost:4566 within 120s." >&2
exit 1