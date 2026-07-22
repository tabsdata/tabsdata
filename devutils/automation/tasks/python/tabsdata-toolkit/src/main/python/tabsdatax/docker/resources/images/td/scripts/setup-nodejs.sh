#!/bin/bash

#
# Copyright 2026 Tabsdata Inc.
#

set -e

PACKAGES_FOLDER=/tmp/packages
NODE_VERSION="24.13.1"

curl -fsSL "https://nodejs.org/dist/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-arm64.tar.xz" \
    | tar -xJ -C /usr/local --strip-components=1

if [ -f "${PACKAGES_FOLDER}/package.json" ] && [ -f "${PACKAGES_FOLDER}/package-lock.json" ]; then
    cd "${PACKAGES_FOLDER}"
    npm ci
fi
