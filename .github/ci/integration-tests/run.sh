#!/usr/bin/env bash
set -euo pipefail

suite="${1:?Usage: run-integration-tests SUITE}"
if [[ ! "$suite" =~ ^[a-z][a-z0-9_]*$ ]] || [[ ! -d "integration_tests/$suite" ]]; then
    echo "Unknown integration test suite: $suite" >&2
    exit 2
fi

mkdir -p /test-results
exec python -m pytest --verbose -rA \
    --dist worksteal -n "${PYTEST_WORKERS:-2}" --timeout=900 \
    --junitxml="/test-results/$suite.xml" "integration_tests/$suite"
