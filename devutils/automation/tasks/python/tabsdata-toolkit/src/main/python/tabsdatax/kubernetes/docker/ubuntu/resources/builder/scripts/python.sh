#!/bin/bash

#
# Copyright 2026 Tabs Data Inc.
#

set -e

VENV_TABSDATA=/root/.uv/envs/tabsdata

curl -LsSf https://astral.sh/uv/install.sh | sh
uv python install 3.12.13

UV_PYTHON_LIB="$(python3.12 -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR"))')"
echo "${UV_PYTHON_LIB}" > /etc/ld.so.conf.d/uv-python.conf
ldconfig

uv venv --seed --python 3.12.13 "${VENV_TABSDATA}"
uv pip install --python "${VENV_TABSDATA}/bin/python" zstandard
