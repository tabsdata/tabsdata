#
# Copyright 2026 Tabs Data Inc.
#
"""Live PostgreSQL extractor tests — gated by the ``POSTGRES`` env var."""

from __future__ import annotations

from tabsdata_testutils.external import external
from tabsdata_testutils.postgres import POSTGRES

from tabsdata._cli.tdconn._internal.extractor.connector.postgres.extractor import (
    PostgresConnectionExtractor,
)


def _info(envs: dict) -> dict:
    host = envs.get("HOST") or "localhost"
    port = envs.get("PORT") or "5432"
    database = envs.get("DATABASE") or ""
    uri = (
        f"postgresql://{host}:{port}/{database}"
        if database
        else f"postgresql://{host}:{port}"
    )
    info: dict = {
        "uri": uri,
        "credentials": {
            "user": envs.get("USERNAME") or "",
            "password": envs.get("PASSWORD") or "",
        },
    }
    schema = envs.get("SCHEMA")
    if schema:
        info["schema"] = schema
    return info


@external(system=POSTGRES)
def test_postgres_check_connect(envs: dict):
    PostgresConnectionExtractor().check_connect(_info(envs))


@external(system=POSTGRES)
def test_postgres_metadata(envs: dict):
    assert isinstance(PostgresConnectionExtractor().metadata(_info(envs)), list)
