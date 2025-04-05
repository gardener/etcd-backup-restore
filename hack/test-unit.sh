#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
#
# SPDX-License-Identifier: Apache-2.0

#
set -o errexit
set -o nounset
set -o pipefail

###############################################################################

test_with_coverage() {
  echo "[INFO] Test coverage is enabled."
  local output_dir=test/output
  local coverprofile_file=coverprofile.out
  mkdir -p "${output_dir}"

  ginkgo "${GINKGO_COMMON_FLAGS[@]}" -gcflags=all=-d=checkptr=0 --coverprofile "${coverprofile_file}" -covermode=set -outputdir "${output_dir}" ${TEST_PACKAGES}
  sed -i='' '/mode: set/d' "${output_dir}/${coverprofile_file}"
  { echo "mode: set"; cat "${output_dir}/${coverprofile_file}"; } > "${output_dir}/${coverprofile_file}.temp"
  mv "${output_dir}/${coverprofile_file}.temp" "${output_dir}/${coverprofile_file}"

  go tool cover -func "${output_dir}/${coverprofile_file}"
}

################################################################################

# To run a specific package, run TEST_PACKAGES=<PATH_TO_PACKAGE> make test
# If the TEST_PACKAGES is not set, then define a list of packages to run.
TEST_PACKAGES="${TEST_PACKAGES:-"
./pkg/backoff \
./pkg/compactor \
./pkg/defragmentor \
./pkg/health/... \
./pkg/initializer/... \
./pkg/leaderelection \
./pkg/member \
./pkg/metrics \
./pkg/miscellaneous \
./pkg/server \
./pkg/snapshot/... \
./pkg/snapstore"
}"

RUN_NEGATIVE="${RUN_NEGATIVE:-"true"}"

GINKGO_COMMON_FLAGS="-r -timeout=1h0m0s --show-node-events --fail-on-pending -mod=vendor"

# First run the `pkg/defragmentor` tests to fail fast as it has flaky tests.
# TODO: Remove this once the flaky tests are fixed.
FIRST_PACKAGE_TO_RUN="./pkg/defragmentor"

# Check if the FIRST_PACKAGE_TO_RUN is part of the TEST_PACKAGES
IS_FIRST_PACKAGE_PRESENT=false
if [[ " ${TEST_PACKAGES} " == *" ${FIRST_PACKAGE_TO_RUN} "* ]]; then
  IS_FIRST_PACKAGE_PRESENT=true
  # Remove FIRST_PACKAGE_TO_RUN from TEST_PACKAGES
  TEST_PACKAGES=$(echo "${TEST_PACKAGES}" | sed "s|${FIRST_PACKAGE_TO_RUN}||")
fi

if [[ "${COVER:-false}" == "false" ]]; then
  echo "[INFO] Test coverage is disabled."

  # If the FIRST_PACKAGE_TO_RUN is present, run non-negative tests on it first.
  if [[ "${IS_FIRST_PACKAGE_PRESENT}" == "true" ]]; then
    echo "[INFO] Running tests for the first package: ${FIRST_PACKAGE_TO_RUN}"
    ginkgo -race -trace $GINKGO_COMMON_FLAGS -gcflags=all=-d=checkptr=0 --randomize-all --skip="NEGATIVE\:.*" ${FIRST_PACKAGE_TO_RUN}
  fi

  # Run the non-negative scenarios for remaining packages with randomize-all parameters.
  ginkgo -race -trace $GINKGO_COMMON_FLAGS -gcflags=all=-d=checkptr=0 --randomize-all --randomize-suites --skip="NEGATIVE\:.*" $TEST_PACKAGES

  if [[ "${RUN_NEGATIVE}" == "true" ]]; then
    echo "[INFO] Running negative tests now..."

    # If the FIRST_PACKAGE_TO_RUN is present, run negative tests on it first.
    if [[ "${IS_FIRST_PACKAGE_PRESENT}" == "true" ]]; then
      echo "[INFO] Running negative tests for the first package: ${FIRST_PACKAGE_TO_RUN}"
      ginkgo -race -trace $GINKGO_COMMON_FLAGS -gcflags=all=-d=checkptr=0 --focus="NEGATIVE\:.*" ${FIRST_PACKAGE_TO_RUN}
    fi

    # Run negative scenarios sequentially for remaining packages (removed failOnPending as one spec in restore test is marked as 'X' for excluding)
    ginkgo -race -trace $GINKGO_COMMON_FLAGS -gcflags=all=-d=checkptr=0 --focus="NEGATIVE\:.*" ${TEST_PACKAGES}
  fi

else
  test_with_coverage
fi
