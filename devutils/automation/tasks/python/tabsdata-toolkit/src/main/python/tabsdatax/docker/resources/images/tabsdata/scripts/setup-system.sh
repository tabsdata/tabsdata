#!/bin/bash

#
# Copyright 2026 Tabsdata Inc.
#

set -e

apt-get update
apt-get upgrade -y

apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    unzip
