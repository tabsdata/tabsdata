#!/bin/bash

#
# Copyright 2026 Tabs Data Inc.
#

set -e

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
source "${CARGO_HOME}/env"
rustup component add rustfmt clippy

cargo install --locked --jobs ${TD_CARGO_INSTALL_JOBS:-8} \
    cargo-make \
    cargo-upgrades \
    cargo-workspaces \
    mdbook \
    mdbook-d2 \
    mdbook-mermaid \
    sccache
