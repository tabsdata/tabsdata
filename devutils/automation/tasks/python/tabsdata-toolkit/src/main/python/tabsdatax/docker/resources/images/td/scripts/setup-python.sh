#!/bin/bash

#
# Copyright 2026 Tabs Data Inc.
#

set -e

PACKAGES_FOLDER=/tmp/packages
VENV_TD=/root/.uv/envs/td
VENV_TABSDATA=/root/.uv/envs/tabsdata

PRODUCTION_REQUIREMENTS="${PACKAGES_FOLDER}/tabsdata-requirements.txt"
DEVELOPMENT_REQUIREMENTS="${PACKAGES_FOLDER}/tabsdata-requirements-dev.txt"

curl -LsSf https://astral.sh/uv/install.sh | sh
uv python install 3.12.13

UV_PYTHON_LIB="$(python3.12 -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR"))')"
echo "${UV_PYTHON_LIB}" > /etc/ld.so.conf.d/uv-python.conf
ldconfig

uv venv --seed --python 3.12.13 "${VENV_TD}"
uv venv --seed --python 3.12.13 --relocatable "${VENV_TABSDATA}"

if [ -f "${PRODUCTION_REQUIREMENTS}" ]; then
    uv pip install --python "${VENV_TD}/bin/python" --requirement "${PRODUCTION_REQUIREMENTS}"
fi
if [ -f "${DEVELOPMENT_REQUIREMENTS}" ]; then
    uv pip install --python "${VENV_TD}/bin/python" --requirement "${DEVELOPMENT_REQUIREMENTS}"
fi

if [ -f "${PRODUCTION_REQUIREMENTS}" ]; then
    uv pip install --python "${VENV_TABSDATA}/bin/python" --requirement "${PRODUCTION_REQUIREMENTS}"
fi
