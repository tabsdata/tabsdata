#
#  Copyright 2026. Tabs Data Inc.
#

"""End-to-end ``SnowflakeConnectionExplorer`` tests against a real
Snowflake account.

Snowflake has no local sandbox: these are live tests driven by the same
``SNW__*`` environment variables as the plugin integration suite (account,
user, PAT, database, schema, warehouse, optional role). They skip unless
all required variables are set. There is no fixed ``users`` / ``orders``
seed, so the catalog surface of ``SnowflakeConnectionExplorer`` --
``check_connect`` / ``metadata`` / ``data`` -- is exercised structurally:
the probe succeeds, ``metadata`` returns ``SQLEntity`` objects for the
configured schema, and ``data`` samples whatever entity is present.
"""

from __future__ import annotations

import os

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.sql._explorer import SQLEntity
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.snowflake import SnowflakeDestConn
from tabsdatak.conn.snowflake._explorer import SnowflakeConnectionExplorer

pytestmark = [
    pytest.mark.snowflake,
    pytest.mark.snowflake_live,
    pytest.mark.xdist_group("snowflake-plugin_integration_seq"),
]

_ACCOUNT = os.environ.get("SNW__ACCOUNT")
_USER = os.environ.get("SNW__USER")
_PASSWORD = os.environ.get("SNW__PAT")
_DATABASE = os.environ.get("SNW__DATABASE")
_SCHEMA = os.environ.get("SNW__SCHEMA")
_WAREHOUSE = os.environ.get("SNW__WAREHOUSE")
_ROLE = os.environ.get("SNW__ROLE")

_REQUIRED = (_ACCOUNT, _USER, _PASSWORD, _DATABASE, _SCHEMA, _WAREHOUSE)


def _conn() -> SnowflakeDestConn:
    if not all(_REQUIRED):
        pytest.skip(
            "Snowflake live tests require SNW__ACCOUNT + SNW__USER + "
            "SNW__PAT + SNW__DATABASE + SNW__SCHEMA + SNW__WAREHOUSE"
        )
    kwargs = dict(
        account=_ACCOUNT,
        credentials=UserPassword(user=Secret(_USER), password=Secret(_PASSWORD)),
        database=_DATABASE,
        schema=_SCHEMA,
        warehouse=_WAREHOUSE,
    )
    if _ROLE:
        kwargs["role"] = _ROLE
    return SnowflakeDestConn(**kwargs)


def test_check_connect_succeeds():
    assert SnowflakeConnectionExplorer().check_connect(_conn()) is None


def test_metadata_returns_sql_entities():
    entities = SnowflakeConnectionExplorer().metadata(_conn())
    assert isinstance(entities, list)
    assert all(isinstance(e, SQLEntity) for e in entities)


def test_data_reads_sample_rows():
    conn = _conn()
    explorer = SnowflakeConnectionExplorer()
    entities = explorer.metadata(conn)
    if not entities:
        pytest.skip("no tables present in the configured Snowflake schema")

    rows = explorer.data(conn, entities[0], limit=5)
    assert isinstance(rows, list)
    assert len(rows) <= 5
    expected = {c.name for c in entities[0].columns}
    assert all(set(r) == expected for r in rows)
