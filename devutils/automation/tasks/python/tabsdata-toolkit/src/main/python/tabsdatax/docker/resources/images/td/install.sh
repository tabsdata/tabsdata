#!/bin/bash

#
# Copyright 2026 Tabs Data Inc.
#

set -e

VENV=/root/.uv/envs/tabsdata

cd "/workspace/$PROJECT"
uv pip install --python "${VENV}/bin/python" --quiet -e ./devutils/automation/tasks/python/tabsdata-toolkit --config-settings editable_mode=compat --no-build-isolation
VIRTUAL_ENV="${VENV}" PATH="${VENV}/bin:${PATH}" cargo make hydrate --no-permit --no-compile "$@"
