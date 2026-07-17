#!/bin/bash

#
# Copyright 2026 Tabs Data Inc.
#

set -e

DEVPI_VENV="/root/.uv/envs/devpi"
DEVPI_DATA="/root/.devpi/server"

mkdir -p /root/.uv/envs \
         "$(dirname "${DEVPI_VENV}")" \
         "$(dirname "${DEVPI_DATA}")"

uv venv --seed --python 3.12.13 "${DEVPI_VENV}"
"${DEVPI_VENV}/bin/pip" install --no-cache-dir \
    devpi-server \
    devpi-client \
    devpi-web \
    twine

"${DEVPI_VENV}/bin/devpi-init" --serverdir "${DEVPI_DATA}"

rm -rf /root/.cache/pip /root/.cache/uv
