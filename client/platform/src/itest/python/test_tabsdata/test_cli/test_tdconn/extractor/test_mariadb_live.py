#
# Copyright 2026 Tabs Data Inc.
#
"""Live MariaDB extractor tests — gated by the ``MARIADB`` env var."""

from __future__ import annotations

from tabsdata_testutils.external import external
from tabsdata_testutils.mariadb import MARIADB

from tabsdata._cli.tdconn._internal.extractor.connector.mariadb.extractor import (
    MariaDbConnectionExtractor,
)


def _info(envs: dict) -> dict:
    """Build a MariaDBConn-shaped info dict from the system env vars."""
    host = envs.get("HOST") or "localhost"
    port = envs.get("PORT") or "3306"
    database = envs.get("DATABASE") or ""
    uri = (
        f"mariadb://{host}:{port}/{database}"
        if database
        else f"mariadb://{host}:{port}"
    )
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


@external(system=MARIADB)
def test_mariadb_check_connect(envs: dict):
    MariaDbConnectionExtractor().check_connect(_info(envs))


@external(system=MARIADB)
def test_mariadb_metadata(envs: dict):
    assert isinstance(MariaDbConnectionExtractor().metadata(_info(envs)), list)
