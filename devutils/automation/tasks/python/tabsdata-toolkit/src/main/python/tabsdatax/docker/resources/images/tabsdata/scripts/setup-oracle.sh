#!/bin/bash

#
# Copyright 2026 Tabs Data Inc.
#

set -e

ORACLE_INSTANT_CLIENT_VERSION="2326100"
ORACLE_INSTANT_CLIENT_BASE="https://download.oracle.com/otn_software/linux/instantclient"

mkdir -p /opt/oracle/downloads
cd /opt/oracle/downloads

curl -LO "${ORACLE_INSTANT_CLIENT_BASE}/${ORACLE_INSTANT_CLIENT_VERSION}/instantclient-basic-linux.arm64-23.26.1.0.0.zip"

unzip -o instantclient-basic-linux.arm64-23.26.1.0.0.zip

find . -maxdepth 1 -type d -name "instantclient*" -exec mv {} ../instantclient \;
cd ../instantclient && mkdir -p lib && cd lib && ln -s ../libclntsh.so libclntsh.so

rm -rf /opt/oracle/downloads
