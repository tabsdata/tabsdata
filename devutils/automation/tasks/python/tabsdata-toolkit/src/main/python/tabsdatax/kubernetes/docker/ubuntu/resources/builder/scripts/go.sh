#!/bin/bash

#
# Copyright 2026 Tabs Data Inc.
#

set -e

TARGETARCH="${TARGETARCH:-arm64}"

GO_VERSION="$(curl -fsSL 'https://go.dev/VERSION?m=text' | head -n 1)"

curl -fsSL "https://go.dev/dl/${GO_VERSION}.linux-${TARGETARCH}.tar.gz" \
    | tar -xz -C /usr/local
