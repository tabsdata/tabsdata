#!/usr/bin/env bash
#
# Copyright 2026 Tabsdata Inc.
#

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TDKSERVER="${TDKSERVER:-tdkserver}"
RUNS="${RUNS:-1}"
STOP_ON_FAILURE="${STOP_ON_FAILURE:-1}"

cd "$HERE"

session() {
    pytest \
        --verbose \
        --rootdir="$HERE" \
        --config-file="$HERE/pyproject.toml" \
        lifecycle.py \
        --tdkserver="$TDKSERVER" \
        "$@"
}

passed=0
failed=0
for run in $(seq 1 "$RUNS"); do
    echo "=== run $run / $RUNS ==="
    if session "$@"; then
        passed=$((passed + 1))
    else
        rc=$?
        failed=$((failed + 1))
        echo "=== run $run failed (rc=$rc) ==="
        if [ "$STOP_ON_FAILURE" = "1" ]; then
            break
        fi
    fi
done

echo "=== summary: $passed passed, $failed failed (of $RUNS scheduled) ==="

if [ "$failed" -gt 0 ]; then
    exit 1
fi
