#!/bin/bash

#
# Copyright 2026 Tabsdata Inc.
#

export PYTHONWARNINGS="ignore::UserWarning"

DEVPI_PORT="${DEVPI_PORT:-3141}"
DEVPI_USER="tabsdata"
DEVPI_PASSWORD="tabsdata"
DEVPI_INDEX="tabsdata/td"
DEVPI_BASES="root/pypi"

DEVPI_VENV="/root/.uv/envs/devpi"
DEVPI_DATA="/root/.devpi/server"
DEVPI_LOG="/root/.devpi/server.log"
DEVPI_PID_FILE="/root/.devpi/.pid"
DEVPI_LOGGER="/root/.devpi/logger.json"

DEVPI_SERVER="${DEVPI_VENV}/bin/devpi-server"
DEVPI_CLIENT="${DEVPI_VENV}/bin/devpi"

if curl -sf "http://localhost:${DEVPI_PORT}/" > /dev/null 2>&1; then
    echo "Server devpi already running on port ${DEVPI_PORT}."
    return 0 2>/dev/null || exit 0
fi

if [ ! -f "${DEVPI_DATA}/.nodeinfo" ]; then
    "${DEVPI_VENV}/bin/devpi-init" --serverdir "${DEVPI_DATA}"
fi

"${DEVPI_SERVER}" \
    --host 0.0.0.0 \
    --port "${DEVPI_PORT}" \
    --serverdir "${DEVPI_DATA}" \
    --indexer-backend null \
    --max-request-body-size 2147483648 \
    --logger-cfg "${DEVPI_LOGGER}" \
    > "${DEVPI_LOG}" 2>&1 &
echo $! > "${DEVPI_PID_FILE}"

waited=0
while ! curl -sf "http://localhost:${DEVPI_PORT}/" > /dev/null 2>&1; do
    if [ "${waited}" -ge 30 ]; then
        echo "Server devpi did not start within 30 seconds." >&2
        return 1 2>/dev/null || exit 1
    fi
    sleep 1
    waited=$((waited + 1))
done

"${DEVPI_CLIENT}" use "http://localhost:${DEVPI_PORT}" > /dev/null 2>&1
"${DEVPI_CLIENT}" login root --password "" > /dev/null 2>&1 || true
"${DEVPI_CLIENT}" user -c "${DEVPI_USER}" password="${DEVPI_PASSWORD}" > /dev/null 2>&1 || true
"${DEVPI_CLIENT}" index -c "${DEVPI_INDEX}" bases="${DEVPI_BASES}" > /dev/null 2>&1 || true

echo "Server devpi ready at http://localhost:${DEVPI_PORT}/${DEVPI_INDEX}/ (${waited}s)."

export PIP_INDEX_URL="http://localhost:${DEVPI_PORT}/${DEVPI_INDEX}/"
export UV_DEFAULT_INDEX="http://localhost:${DEVPI_PORT}/${DEVPI_INDEX}/"
export UV_INDEX_URL="http://localhost:${DEVPI_PORT}/${DEVPI_INDEX}/"
