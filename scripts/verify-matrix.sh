#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p build-reports
for version in 1.13 1.14 1.15 1.16; do
  mvn -B -P"flink-$version" clean verify "$@" > "build-reports/flink-$version.log" 2>&1
  mkdir -p "build-reports/flink-$version"
  cp target/surefire-reports/TEST-*.xml "build-reports/flink-$version/"
  echo "Flink $version: verification passed"
done
