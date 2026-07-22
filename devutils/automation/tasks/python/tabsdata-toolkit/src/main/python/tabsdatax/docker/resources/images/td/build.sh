#!/bin/bash

#
# Copyright 2026 Tabsdata Inc.
#

set -e

if [ -f /workspace/tabsdata-ui/package.json ] && [ ! -e /workspace/tabsdata-ui/node_modules/.package-lock.json ]; then
    cd /workspace/tabsdata-ui && npm ci
fi

case "$PROJECT" in
    tabsdata|tabsdata-os|tabsdata-ee)
        if [ -d /workspace/tabsdata-km ]; then
            cd /workspace/tabsdata-km && cargo make --env profile=assembly assembly --no-permit
        elif [ -d /workspace/kminus ]; then
            cd /workspace/kminus && cargo make --env profile=assembly assembly --no-permit
        fi
        ;;
esac

cd "/workspace/$PROJECT"
cargo make --env profile=assembly "${TARGET:-build}" --no-permit "$@"
