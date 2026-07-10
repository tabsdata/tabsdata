#!/bin/bash

#
# Copyright 2026 Tabs Data Inc.
#

set -e

TARGETARCH="${TARGETARCH:-arm64}"
PACKAGES_FOLDER=/tmp/packages
NODE_VERSION="24.13.1"

case "${TARGETARCH}" in
    arm64) NODE_ARCH="arm64" ;;
    amd64) NODE_ARCH="x64" ;;
    *) echo "Unsupported architecture: ${TARGETARCH}" >&2; exit 1 ;;
esac

curl -fsSL "https://nodejs.org/dist/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-${NODE_ARCH}.tar.xz" \
    | tar -xJ -C /usr/local --strip-components=1

if [ -f "${PACKAGES_FOLDER}/package.json" ] && [ -f "${PACKAGES_FOLDER}/package-lock.json" ]; then
    cd "${PACKAGES_FOLDER}"
    npm ci
fi
