#!/bin/bash

#
# Copyright 2026 Tabsdata Inc.
#

set -e

REPOSITORY="${1:?repository name required}"
NO_COMPILE="${NO_COMPILE:-false}"

WORKSPACE="/workspace"
REPOSITORY_FOLDER="${WORKSPACE}/${REPOSITORY}"
WHEELS_FOLDER="${REPOSITORY_FOLDER}/target/wheels"

DEVPI_PORT="${DEVPI_PORT:-3141}"
DEVPI_INDEX="tabsdata/td"
DEVPI_URL="http://localhost:${DEVPI_PORT}/${DEVPI_INDEX}/"
DEVPI_VENV="/root/.uv/envs/devpi"

STAGE_VENV="/root/.uv/envs/stage"

source /root/.td/bin/devpi-start.sh

echo "Building and packaging ${REPOSITORY}..."
cd "${REPOSITORY_FOLDER}"
if [ "${NO_COMPILE}" = "true" ]; then
    cargo make --env profile=assembly wheels --devpi --no-compile
else
    cargo make --env profile=assembly wheels --devpi
fi

echo "Publishing wheels to devpi..."
EXTRACT_FOLDER="$(mktemp -d)"
for archive in "${WHEELS_FOLDER}"/*.tar.gz; do
    tar -xzf "${archive}" -C "${EXTRACT_FOLDER}"
done
"${DEVPI_VENV}/bin/twine" upload \
    --repository-url "${DEVPI_URL}" \
    --username "tabsdata" \
    --password "tabsdata" \
    "${EXTRACT_FOLDER}"/*.whl
rm -rf "${EXTRACT_FOLDER}"

echo "Installing tabsdata from devpi into staging environment..."
uv venv --seed --python 3.12.13 "${STAGE_VENV}"
PIP_INDEX_URL="${DEVPI_URL}" UV_INDEX_URL="${DEVPI_URL}" UV_DEFAULT_INDEX="${DEVPI_URL}" \
    uv pip install --python "${STAGE_VENV}/bin/python" "tabsdata[all]"

echo "Creating runtime venvs via tdkserver quickstart..."
source "${STAGE_VENV}/bin/activate"
PIP_INDEX_URL="${DEVPI_URL}" UV_INDEX_URL="${DEVPI_URL}" UV_DEFAULT_INDEX="${DEVPI_URL}" \
    tdkserver quickstart
tdkserver stop

echo "Runtime venvs ready under ~/.tabsdata/instances/tabsdata/runtimes/python/."
