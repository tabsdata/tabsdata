"""Unit tests for the Databricks impl -- the pure-Python helpers
that don't need the SDK / SQL connector libraries at module level.

End-to-end coverage against a real Databricks workspace lives in
``test__plugin_integration.py``.
"""

from __future__ import annotations

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.databricks import DatabricksConn
# noinspection PyProtectedMember
from tabsdatak.conn.databricks import _impl
from tabsdatak.conn.databricks.error import (
    DatabricksErrorCode,
    DatabricksValidateException,
)


pytestmark = pytest.mark.databricks


# ---------------------------------------------------------------------------
# Helpers for building test DatabricksConn / DatabricksReadCfg /
# DatabricksWriteCfg instances
# ---------------------------------------------------------------------------


def _conn(
    *,
    catalog: str | None = "main",
    schema: str | None = "analytics",
    volume: str = "my_volume",
    warehouse: str | None = "MY_WAREHOUSE",
    warehouse_id: str | None = None,
) -> DatabricksConn:
    return DatabricksConn(
        host_url=Secret.lit("https://example.cloud.databricks.com"),
        token=Secret.lit("dapi-test"),
        volume=Secret.lit(volume),
        warehouse=(
            Secret.lit(warehouse) if warehouse is not None else None
        ),
        warehouse_id=(
            Secret.lit(warehouse_id)
            if warehouse_id is not None
            else None
        ),
        catalog=Secret.lit(catalog) if catalog is not None else None,
        schema=Secret.lit(schema) if schema is not None else None,
    )


# ---------------------------------------------------------------------------
# Conn helpers
# ---------------------------------------------------------------------------


def test_host_value_unwraps_secret():
    conn = _conn()
    assert (
        _impl._host_value(conn)
        == "https://example.cloud.databricks.com"
    )


def test_token_value_unwraps_secret():
    conn = _conn()
    assert _impl._token_value(conn) == "dapi-test"


# ---------------------------------------------------------------------------
# _fully_qualify_table
# ---------------------------------------------------------------------------


class TestFullyQualifyTable:
    def test_three_part_passes_through(self):
        conn = _conn()
        assert (
            _impl._fully_qualify_table(conn, "c.s.t") == "c.s.t"
        )

    def test_two_part_prepends_catalog(self):
        conn = _conn(catalog="main", schema=None)
        assert (
            _impl._fully_qualify_table(conn, "analytics.users")
            == "main.analytics.users"
        )

    def test_one_part_prepends_catalog_and_schema(self):
        conn = _conn(catalog="main", schema="analytics")
        assert (
            _impl._fully_qualify_table(conn, "users")
            == "main.analytics.users"
        )

    # ``schema`` without ``catalog`` is rejected at DatabricksConn
    # construction (DATABRICKS_2), so the "1-part with only schema"
    # case is unreachable here. The remaining unreachable cases are
    # covered below.

    def test_one_part_with_neither_catalog_nor_schema_raises(self):
        conn = _conn(catalog=None, schema=None)
        with pytest.raises(DatabricksValidateException) as ei:
            _impl._fully_qualify_table(conn, "users")
        assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_10

    def test_two_part_without_catalog_raises(self):
        conn = _conn(catalog=None, schema=None)
        with pytest.raises(DatabricksValidateException) as ei:
            _impl._fully_qualify_table(conn, "analytics.users")
        assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_10


# ---------------------------------------------------------------------------
# _volume_path
# ---------------------------------------------------------------------------


class TestVolumePath:
    def test_one_part_volume_uses_table_catalog_and_schema(self):
        conn = _conn(volume="my_volume")
        path = _impl._volume_path(conn, "main.analytics.users")
        assert path == "/Volumes/main/analytics/my_volume"

    def test_two_part_volume_uses_table_catalog(self):
        conn = _conn(volume="staging.shared_volume")
        path = _impl._volume_path(conn, "main.analytics.users")
        assert path == "/Volumes/main/staging/shared_volume"

    def test_three_part_volume_is_used_verbatim(self):
        conn = _conn(volume="catalog.schema.volume")
        path = _impl._volume_path(conn, "main.analytics.users")
        assert path == "/Volumes/catalog/schema/volume"


# ---------------------------------------------------------------------------
# _replace_initial_values
# ---------------------------------------------------------------------------


class TestReplaceInitialValues:
    def test_replaces_string_with_repr_quoted(self):
        out = _impl._replace_initial_values(
            "SELECT * FROM t WHERE name = {name}",
            {"name": "alice"},
        )
        assert out == "SELECT * FROM t WHERE name = 'alice'"

    def test_replaces_int_unquoted(self):
        out = _impl._replace_initial_values(
            "SELECT * FROM t WHERE id = {id}",
            {"id": 42},
        )
        assert out == "SELECT * FROM t WHERE id = 42"

    def test_replaces_multiple_keys(self):
        out = _impl._replace_initial_values(
            "SELECT * FROM t WHERE id > {min_id} AND id < {max_id}",
            {"min_id": 10, "max_id": 100},
        )
        assert out == "SELECT * FROM t WHERE id > 10 AND id < 100"

    def test_missing_key_leaves_placeholder(self):
        out = _impl._replace_initial_values(
            "SELECT * FROM t WHERE id = {missing}",
            {"other": "x"},
        )
        # Placeholder stays intact when no matching key.
        assert "{missing}" in out
