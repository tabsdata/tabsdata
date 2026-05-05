#
# Copyright 2026 Tabs Data Inc.
#
"""Live CDC extractor tests — one system per CDC flavor.

The CDC scripts provision their own independent containers, so each
CDC extractor has its own gate:

* ``CDCMYSQL``    → ``cdc_mysql.sh`` symlink
* ``CDCPOSTGRES`` → ``cdc_postgres.sh`` symlink
* ``CDCDB2``      → ``cdc_db2.sh`` symlink
"""

from __future__ import annotations

from tabsdata_testutils.cdcdb2 import CDCDB2
from tabsdata_testutils.cdcmysql import CDCMYSQL
from tabsdata_testutils.cdcpostgres import CDCPOSTGRES
from tabsdata_testutils.external import external

from tabsdata._cli.tdconn._internal.extractor.connector.cdc.db2.extractor import (
    Db2CdcConnectionExtractor,
)
from tabsdata._cli.tdconn._internal.extractor.connector.cdc.mysql.extractor import (
    MySQLCdcConnectionExtractor,
)
from tabsdata._cli.tdconn._internal.extractor.connector.cdc.postgres.extractor import (
    PostgresCdcConnectionExtractor,
)


def _sql_info(
    envs: dict,
    scheme: str,
    default_port: str,
    schema: bool = False,
) -> dict:
    """Build a Conn-shaped info dict from a CDC system's env bundle."""
    host = envs.get("HOST") or "localhost"
    port = envs.get("PORT") or default_port
    database = envs.get("DATABASE") or ""
    uri = (
        f"{scheme}://{host}:{port}/{database}"
        if database
        else f"{scheme}://{host}:{port}"
    )
    info: dict = {
        "uri": uri,
        "credentials": {
            "user": envs.get("USERNAME") or "",
            "password": envs.get("PASSWORD") or "",
        },
    }
    if schema:
        s = envs.get("SCHEMA")
        if s:
            info["schema"] = s
    return info


# ---------------------------------------------------------------------------
# MySQL CDC
# ---------------------------------------------------------------------------


@external(system=CDCMYSQL)
def test_mysql_cdc_check_connect(envs: dict):
    MySQLCdcConnectionExtractor().check_connect(_sql_info(envs, "mysql", "3306"))


@external(system=CDCMYSQL)
def test_mysql_cdc_metadata(envs: dict):
    assert isinstance(
        MySQLCdcConnectionExtractor().metadata(_sql_info(envs, "mysql", "3306")),
        list,
    )


# ---------------------------------------------------------------------------
# Postgres CDC
# ---------------------------------------------------------------------------


@external(system=CDCPOSTGRES)
def test_postgres_cdc_check_connect(envs: dict):
    PostgresCdcConnectionExtractor().check_connect(
        _sql_info(envs, "postgresql", "5432", schema=True),
    )


@external(system=CDCPOSTGRES)
def test_postgres_cdc_metadata(envs: dict):
    assert isinstance(
        PostgresCdcConnectionExtractor().metadata(
            _sql_info(envs, "postgresql", "5432", schema=True),
        ),
        list,
    )


# ---------------------------------------------------------------------------
# Db2 CDC
# ---------------------------------------------------------------------------


@external(system=CDCDB2)
def test_db2_cdc_check_connect(envs: dict):
    Db2CdcConnectionExtractor().check_connect(
        _sql_info(envs, "db2", "50000", schema=True),
    )


@external(system=CDCDB2)
def test_db2_cdc_metadata(envs: dict):
    assert isinstance(
        Db2CdcConnectionExtractor().metadata(
            _sql_info(envs, "db2", "50000", schema=True),
        ),
        list,
    )
