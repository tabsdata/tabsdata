#!/bin/bash

#
# Copyright 2026 Tabsdata Inc.
#

set -e

TARGETARCH="${TARGETARCH:-arm64}"

ORACLE_INSTANT_CLIENT_VERSION="2326100"
ORACLE_INSTANT_CLIENT_BASE="https://download.oracle.com/otn_software/linux/instantclient"

case "${TARGETARCH}" in
    arm64) ORACLE_ARCH="arm64" ;;
    amd64) ORACLE_ARCH="x64" ;;
    *) echo "Unsupported architecture: ${TARGETARCH}" >&2; exit 1 ;;
esac

mkdir -p /opt/oracle/downloads
cd /opt/oracle/downloads

curl -LO "${ORACLE_INSTANT_CLIENT_BASE}/${ORACLE_INSTANT_CLIENT_VERSION}/instantclient-basic-linux.${ORACLE_ARCH}-23.26.1.0.0.zip"

unzip -o "instantclient-basic-linux.${ORACLE_ARCH}-23.26.1.0.0.zip"

find . -maxdepth 1 -type d -name "instantclient*" -exec mv {} ../instantclient \;
cd ../instantclient && mkdir -p lib && cd lib && ln -s ../libclntsh.so libclntsh.so

rm -rf /opt/oracle/downloads
