#!/bin/bash

#
# Copyright 2026 Tabsdata Inc.
#

set -e

TD_VENV_MODE="${TD_VENV_MODE:-copy}"
RUNTIMES="/root/.tabsdata/instances/tabsdata/runtimes/python"
ARCHIVES="/mnt/venvs"

if [ "${TD_VENV_MODE}" = "copy" ]; then
    mkdir -p "${RUNTIMES}"
    shopt -s nullglob
    archives=("${ARCHIVES}"/*.tar.zst)
    shopt -u nullglob
    if [ ${#archives[@]} -eq 0 ]; then
        echo "No venv archives found in ${ARCHIVES}." >&2
        exit 1
    fi
    for archive in "${archives[@]}"; do
        echo "Uncompressing $(basename "${archive}")..."
        tar --use-compress-program=unzstd -xf "${archive}" -C "${RUNTIMES}"
    done
elif [ "${TD_VENV_MODE}" = "mount" ]; then
    echo "Using mounted venvs at ${RUNTIMES}."
else
    echo "Unknown TD_VENV_MODE: ${TD_VENV_MODE} (expected copy or mount)." >&2
    exit 1
fi

echo ""
echo "===================="
echo "  tabsdata runner is ready."
echo "  Validate with: tdrun"
echo "===================="
echo ""

if [ $# -gt 0 ]; then
    exec bash --login -c "$*"
elif [ -t 0 ]; then
    exec bash --login
else
    exec tail -f /dev/null
fi
