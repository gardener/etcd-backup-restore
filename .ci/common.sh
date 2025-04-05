# SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
#
# SPDX-License-Identifier: Apache-2.0

set -o errexit
set -o pipefail

VCS="github.com"
ORGANIZATION="gardener"
PROJECT="etcd-backup-restore"
REPOSITORY=${VCS}/${ORGANIZATION}/${PROJECT}
URL=https://${REPOSITORY}.git
VERSION_FILE="$(readlink -f "${SOURCE_PATH}/VERSION")"
VERSION="$(cat "${VERSION_FILE}")"

# The `go <cmd>` commands requires to see the target repository to be part of a
# Go workspace. Thus, if we are not yet in a Go workspace, let's create one
# temporarily by using symbolic links.
if [[ -z "$SOURCE_PATH" ]]; then
    echo "Environment variable SOURCE_PATH must be provided"
    exit 1
fi

# if [[ "${SOURCE_PATH}" != *"src/${REPOSITORY}" ]]; then
#   SOURCE_SYMLINK_PATH="${SOURCE_PATH}/tmp/src/${REPOSITORY}"
#   if [[ -d "${SOURCE_PATH}/tmp" ]]; then
#     rm -rf "${SOURCE_PATH}/tmp"
#   fi
#   mkdir -p "${SOURCE_PATH}/tmp/src/${VCS}/${ORGANIZATION}"
#   ln -s "${SOURCE_PATH}" "${SOURCE_SYMLINK_PATH}"
#   cd "${SOURCE_SYMLINK_PATH}"

#   export GOPATH="${SOURCE_PATH}/tmp"
#   export GOBIN="${SOURCE_PATH}/tmp/bin"
#   export PATH="${GOBIN}:${PATH}"
# fi

# Turn colors in this script off by setting the NO_COLOR variable in your
# environment to any value:
#
# $ NO_COLOR=1 test.sh
NO_COLOR=${NO_COLOR:-""}
if [ -z "$NO_COLOR" ]; then
  header=$'\e[1;33m'
  reset=$'\e[0m'
else
  header=''
  reset=''
fi

function header_text {
  echo "$header$*$reset"
}
