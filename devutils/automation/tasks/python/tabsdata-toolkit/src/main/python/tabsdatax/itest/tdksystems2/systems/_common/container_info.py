#
# Copyright 2026 Tabsdata Inc.
#

"""Docker image coordinates for every container-based system, in one place.

Each container system references its :data:`Image` here for the repository name,
tag and platform, so bumping a version or repinning an arch is a one-line edit in
a single file (grep the constant name). The tag can be overridden per system with
an env var (e.g. ``POSTGRES_IMAGE_VERSION``) for local experimentation.
"""

from __future__ import annotations

import os
import platform
from dataclasses import dataclass

from tabsdatax.itest.tdksystems2 import constants

_ARM = platform.machine() in ("arm64", "aarch64")
NATIVE = "linux/arm64" if _ARM else "linux/amd64"
AMD64 = "linux/amd64"


@dataclass(frozen=True)
class Image:
    name: str  # repository, e.g. "postgres" or "mcr.microsoft.com/mssql/server"
    version: str  # tag
    platform: str  # docker --platform value

    @property
    def ref(self) -> str:
        return f"{self.name}:{self.version}"


def _tag(env: str, default: str) -> str:
    return os.environ.get(env, default)


# --- SQL databases ---------------------------------------------------------
MYSQL = Image(
    "mysql",
    _tag(constants.ENV_MYSQL_IMAGE_VERSION, constants.DEFAULT_MYSQL_IMAGE_VERSION),
    NATIVE,
)
# Tracks the major of the POSTGRESQL_VERSION td-database pins its embedded
# server to (18.x), so the Rust db tests run against the major the product
# ships. Only the major is pinned: the official image publishes its own patch
# cadence and has no tag for theseus's exact patch level.
POSTGRES = Image("postgres", _tag("POSTGRES_IMAGE_VERSION", "18"), NATIVE)
MSSQLSERVER = Image(
    "mcr.microsoft.com/mssql/server",
    _tag("MSSQL_IMAGE_VERSION", "2022-CU14-ubuntu-22.04"),
    AMD64,
)
ORACLE = Image(
    "container-registry.oracle.com/database/free",
    _tag("ORACLE_IMAGE_VERSION", "23.6.0.0-lite"),
    AMD64,
)
STARROCKS = Image(
    "starrocks/allin1-ubuntu", _tag("STARROCKS_IMAGE_VERSION", "3.4.8"), NATIVE
)
DB2 = Image("icr.io/db2_community/db2", _tag("DB2_IMAGE_VERSION", "11.5.9.0"), AMD64)

# --- other container systems ----------------------------------------------
MONGODB = Image("mongo", _tag("MONGODB_IMAGE_VERSION", "6.0.21"), NATIVE)
REDPANDA = Image(
    "redpandadata/redpanda", _tag("REDPANDA_IMAGE_VERSION", "v24.3.5"), NATIVE
)
CLICKHOUSE = Image(
    "clickhouse/clickhouse-server", _tag("CLICKHOUSE_IMAGE_VERSION", "24.8"), NATIVE
)
