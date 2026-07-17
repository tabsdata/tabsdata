#!/bin/bash

#
# Copyright 2026 Tabs Data Inc.
#

set -e

curl -LsSf https://astral.sh/uv/install.sh | sh
uv python install 3.12.13

UV_PYTHON_LIB="$(python3.12 -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR"))')"
echo "${UV_PYTHON_LIB}" > /etc/ld.so.conf.d/uv-python.conf
ldconfig
