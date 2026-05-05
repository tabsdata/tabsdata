#
# Copyright 2026 Tabs Data Inc.
#
"""Live MySQL extractor tests — gated by the ``MYSQL`` env var."""

from __future__ import annotations

from tabsdata_testutils.external import external
from tabsdata_testutils.mysql import MYSQL

from tabsdata._cli.tdconn._internal.extractor.connector.mysql.extractor import (
    MySQLConnectionExtractor,
)


def _info(envs: dict) -> dict:
    """Build a MySQLConn-shaped info dict from the system env vars."""
    host = envs.get("HOST") or "localhost"
    port = envs.get("PORT") or "3306"
    database = envs.get("DATABASE") or ""
    uri = f"mysql://{host}:{port}/{database}" if database else f"mysql://{host}:{port}"
    return {
        "uri": uri,
        "credentials": {
            # ``envs[...]`` is ``None`` when the sub-env is unset;
            # ``or ""`` normalizes to empty string so the credentials
            # dict contract (both fields str) stays intact.
            "user": envs.get("USERNAME") or "",
            "password": envs.get("PASSWORD") or "",
        },
    }


@external(system=MYSQL)
def test_mysql_check_connect(envs: dict):
    MySQLConnectionExtractor().check_connect(_info(envs))


@external(system=MYSQL)
def test_mysql_metadata(envs: dict):
    assert isinstance(MySQLConnectionExtractor().metadata(_info(envs)), list)
