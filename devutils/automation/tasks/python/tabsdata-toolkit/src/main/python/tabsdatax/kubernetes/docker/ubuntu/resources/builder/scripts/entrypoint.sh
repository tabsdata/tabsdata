#!/bin/bash

#
# Copyright 2026 Tabs Data Inc.
#

source /root/.td/bin/devpi-start.sh

if [ $# -gt 0 ]; then
    exec bash --login -c "$*"
elif [ -t 0 ]; then
    exec bash --login
else
    exec tail -f /dev/null
fi
