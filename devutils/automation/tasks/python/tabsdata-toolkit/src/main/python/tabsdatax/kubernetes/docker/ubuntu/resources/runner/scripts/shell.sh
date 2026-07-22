#!/bin/bash

#
# Copyright 2026 Tabsdata Inc.
#

set -e

cat >> /root/.bash_profile <<'BASH_PROFILE'
TD_VENV="/root/.tabsdata/instances/tabsdata/runtimes/python/td"
if [ -f "${TD_VENV}/bin/activate" ]; then
    source "${TD_VENV}/bin/activate"
fi
BASH_PROFILE

chmod +x /root/.td/bin/entrypoint.sh
chmod +x /root/.td/bin/tdrun

rm -rf /root/.cache/pip /tmp/* /var/tmp/* /var/log/*
