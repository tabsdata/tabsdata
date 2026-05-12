#!/bin/bash

#
# Copyright 2026 Tabs Data Inc.
#

set -e

VENV_TD=/root/.uv/envs/td
VENV_TABSDATA=/root/.uv/envs/tabsdata

if [ -d "${VENV_TABSDATA}" ] && [ ! -d "${VENV_TD}" ]; then
    cp -a "${VENV_TABSDATA}" "${VENV_TD}"
fi

exec "$@"
