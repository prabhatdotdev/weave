#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT_DIR"

COVERAGE_MIN=${COVERAGE_MIN:-75.0}
COVERAGE_FILE=${COVERAGE_FILE:-coverage.critical.out}
COVERAGE_HTML_FILE=${COVERAGE_HTML_FILE:-coverage.critical.html}
COVERAGE_PACKAGES=${COVERAGE_PACKAGES:-"./core ./runtime ./testkit ./transport/amqp ./transport/kafka"}
GOCACHE_DIR=${GOCACHE:-"$ROOT_DIR/.gocache"}

mkdir -p "$GOCACHE_DIR"

echo "Running coverage for critical packages: $COVERAGE_PACKAGES"
env GOCACHE="$GOCACHE_DIR" go test -coverprofile="$COVERAGE_FILE" $COVERAGE_PACKAGES

TOTAL_COVERAGE=$(env GOCACHE="$GOCACHE_DIR" go tool cover -func="$COVERAGE_FILE" | awk '/^total:/ {gsub("%", "", $3); print $3}')

echo "Critical package coverage: ${TOTAL_COVERAGE}%"
env GOCACHE="$GOCACHE_DIR" go tool cover -html="$COVERAGE_FILE" -o "$COVERAGE_HTML_FILE"
echo "Coverage report generated: $COVERAGE_HTML_FILE"

awk -v total="$TOTAL_COVERAGE" -v min="$COVERAGE_MIN" 'BEGIN {
	if ((total + 0) < (min + 0)) {
		printf("Coverage check failed: %.1f%% is below required %.1f%%\n", total + 0, min + 0)
		exit 1
	}
	printf("Coverage check passed: %.1f%% meets required %.1f%%\n", total + 0, min + 0)
}'
