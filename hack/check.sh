#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
#
# SPDX-License-Identifier: Apache-2.0


set -e

GOLANGCI_LINT_CONFIG_FILE=""

for arg in "$@"; do
  case $arg in
    --golangci-lint-config=*)
    GOLANGCI_LINT_CONFIG_FILE="-c ${arg#*=}"
    shift
    ;;
  esac
done

echo "> Check"

echo "Executing golangci-lint"
golangci-lint run $GOLANGCI_LINT_CONFIG_FILE "$@"

echo "Executing gofmt/goimports"
folders=()
for f in "$@"; do
  folders+=( "$(echo $f | sed 's/\.\.\.//')" )
done
unformatted_files="$(goimports -l ${folders[*]})"
if [[ "$unformatted_files" ]]; then
  echo "Unformatted files detected:"
  echo "$unformatted_files"
  exit 1
fi

# Execute lint checks on helm chart.
HELM_CHART_PATH="./chart/etcd-backup-restore"
echo "Checking Helm charts linting"
helm lint "${HELM_CHART_PATH}"

echo "Checking Go version"
for module_go_version in $(go list -f {{.GoVersion}} -m)
do
  if ! [[ $module_go_version =~ ^[0-9]+\.[0-9]+\.0$ ]]; then
    echo "Go version is invalid, please adhere to x.y.0 version"
    echo "See https://github.com/gardener/etcd-druid/pull/925"
    exit 1
  fi
done

echo "All checks successful"

