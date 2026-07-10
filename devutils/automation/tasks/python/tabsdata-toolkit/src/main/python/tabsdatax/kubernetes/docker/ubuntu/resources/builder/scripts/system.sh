#!/bin/bash

#
# Copyright 2026 Tabs Data Inc.
#

set -e

apt-get update
apt-get upgrade -y

apt-get install -y --no-install-recommends \
    binutils \
    build-essential \
    ca-certificates \
    curl \
    file \
    git \
    libclang-dev \
    libssl-dev \
    pkg-config \
    xz-utils \
    zstd

rm -rf /var/lib/apt/lists/*
