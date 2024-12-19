#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
#
# SPDX-License-Identifier: Apache-2.0

set -e

root_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." &> /dev/null && pwd )"

gosec_report="false"
gosec_report_parse_flags=""

parse_flags() {
  while test $# -gt 1; do
    case "$1" in
      --gosec-report)
        shift; gosec_report="$1"
        ;;
      *)
        echo "Unknown argument: $1"
        exit 1
        ;;
    esac
    shift
  done
}

parse_flags "$@"

echo "> Running gosec"
gosec --version
if [[ "$gosec_report" != "false" ]]; then
  echo "Exporting report to $root_dir/gosec-report.sarif"
  gosec_report_parse_flags="-track-suppressions -fmt=sarif -out=gosec-report.sarif -stdout"
fi

# exclude generated code, hack directory (where hack scripts reside)
# and tmp directory (where temporary mod files are downloaded)
# shellcheck disable=SC2086
gosec -exclude-generated -exclude-dir=hack -exclude-dir=tmp $gosec_report_parse_flags ./...
