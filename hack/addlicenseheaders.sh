#!/usr/bin/env bash
#  SPDX-FileCopyrightText: Copyright Contributors to the Gardener project
#
#  SPDX-License-Identifier: Apache-2.0

set -e

echo "> Adding Apache License header to all go files where it is not present"

YEAR="$(date +%Y)"

# addlicense with a license file (parameter -f) expects no comments in the file.
# boilerplate.go.txt is however also used also when generating go code.
# Therefore we remove '//' from boilerplate.go.txt here before passing it to addlicense.

temp_file=$(mktemp)
trap "rm -f $temp_file" EXIT
sed -e "s/YEAR/${YEAR}/g" -e 's|^// *||' hack/boilerplate.go.txt > $temp_file

addlicense \
  -f $temp_file \
  -ignore "**/*.md" \
  -ignore "**/*.yaml" \
  -ignore "**/*.yml" \
  -ignore "**/Dockerfile" \
  .
