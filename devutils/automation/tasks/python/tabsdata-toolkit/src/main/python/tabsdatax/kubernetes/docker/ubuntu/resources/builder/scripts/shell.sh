#!/bin/bash

#
# Copyright 2026 Tabs Data Inc.
#

set -e

cat >> /root/.bash_profile <<'BASH_PROFILE'
source /root/.uv/envs/tabsdata/bin/activate
BASH_PROFILE

chmod +x /root/.td/bin/devpi-start.sh
chmod +x /root/.td/bin/entrypoint.sh
chmod +x /root/.td/sbin/package.sh
