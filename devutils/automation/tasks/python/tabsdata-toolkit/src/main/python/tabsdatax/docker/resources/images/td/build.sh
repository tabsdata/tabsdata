#!/bin/bash

#
# Copyright 2026 Tabs Data Inc.
#

set -e

if [ -f /workspace/tabsdata-ui/package.json ] && [ ! -e /workspace/tabsdata-ui/node_modules/.package-lock.json ]; then
    cd /workspace/tabsdata-ui && npm ci
fi

case "$PROJECT" in
    tabsdata|tabsdata-os|tabsdata-ee)
        if [ -d /workspace/tabsdata-km ]; then
            cd /workspace/tabsdata-km && cargo make assembly --no-permit
        elif [ -d /workspace/kminus ]; then
            cd /workspace/kminus && cargo make assembly --no-permit
        fi
        ;;
esac

cd "/workspace/$PROJECT"
cargo make "${TARGET:-build}" --no-permit "$@"
