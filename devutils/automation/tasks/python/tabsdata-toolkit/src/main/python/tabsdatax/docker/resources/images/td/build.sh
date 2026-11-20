#!/bin/bash

#
# Copyright 2026 Tabsdata Inc.
#

set -e

for project in ui old-ui; do
    folder="/workspace/tabsdata-ui/${project}"
    if [ -f "${folder}/package.json" ] && [ ! -e "${folder}/node_modules/.package-lock.json" ]; then
        (cd "${folder}" && npm ci)
    fi
done

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
