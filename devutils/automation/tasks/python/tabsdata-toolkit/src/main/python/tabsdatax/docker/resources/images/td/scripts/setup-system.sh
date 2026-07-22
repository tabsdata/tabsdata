#!/bin/bash

#
# Copyright 2026 Tabsdata Inc.
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
    xz-utils
