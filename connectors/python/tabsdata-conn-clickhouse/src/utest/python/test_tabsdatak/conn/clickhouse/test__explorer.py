#
#  Copyright 2026. Tabs Data Inc.
#

"""Driver-free unit tests for ``ClickHouseConnectionExplorer``.

These stub the ``clickhouse_connect`` client so the explorer's catalog
logic (and the fqn it derives) can be verified without a live ClickHouse
server -- unlike ``test__explorer_integration.py``, which needs one.
"""

from __future__ import annotations

import clickhouse_connect
import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.clickhouse import ClickHouseDestConn, S3Location
from tabsdatak.conn.clickhouse._explorer import ClickHouseConnectionExplorer
from tabsdatak.conn.common.types import AwsAccessSecretKey, UserPassword

pytestmark = pytest.mark.clickhouse


class _FakeResult:
    def __init__(self, rows: list[tuple]) -> None:
        self.result_rows = rows


class _FakeClient:
    """Minimal stand-in for a ``clickhouse_connect`` client: answers the
    ``system.tables`` / ``system.columns`` catalog queries the explorer
    issues and no-ops ``close``."""

    def query(self, sql: str, parameters: dict | None = None):
        if "system.tables" in sql:
            return _FakeResult([("events",), ("orders",)])
        if "system.columns" in sql:
            return _FakeResult([("id", "Int32", 1, "")])
        raise AssertionError(f"unexpected query: {sql}")

    def close(self) -> None:
        pass


def _conn(database: str = "analytics") -> ClickHouseDestConn:
    return ClickHouseDestConn(
        host="ch.example.invalid",
        port=8123,
        database=database,
        secure=False,
        staging=S3Location(
            bucket="staging-bucket",
            region="us-east-1",
            credentials=AwsAccessSecretKey(
                access_key_id=Secret("AKIA0000000000000000"),
                secret_access_key=Secret("secret-value"),
            ),
            base_path="staging/",
        ),
        credentials=UserPassword(user=Secret("u"), password=Secret("p")),
    )


def test_metadata_entity_fqn_is_database_dot_table(monkeypatch):
    monkeypatch.setattr(clickhouse_connect, "get_client", lambda **kw: _FakeClient())

    entities = ClickHouseConnectionExplorer().metadata(_conn("analytics"))

    # ClickHouse's two-level namespace collapses to ``database.table``.
    assert [e.fqn() for e in entities] == ["analytics.events", "analytics.orders"]
    # The schema level is always blank, so it must never leave a dangling
    # separator (e.g. ``analytics..events``).
    assert all(".." not in e.fqn() for e in entities)
