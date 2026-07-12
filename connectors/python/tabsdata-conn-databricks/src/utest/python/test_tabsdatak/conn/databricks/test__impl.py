"""Unit tests for the Databricks impl -- the pure-Python helpers
that don't need the SDK / SQL connector libraries at module level.

End-to-end coverage against a real Databricks workspace lives in
``test__plugin_integration.py``.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from tabsdatak.api import Secret

# noinspection PyProtectedMember
from tabsdatak.conn.databricks import DatabricksDestConn, DatabricksSrcConn, _impl
from tabsdatak.conn.databricks.error import (
    DatabricksErrorCode,
    DatabricksValidateException,
)

pytestmark = pytest.mark.databricks


# ---------------------------------------------------------------------------
# Helpers for building test DatabricksConn / DatabricksReadCfg /
# DatabricksWriteCfg instances
# ---------------------------------------------------------------------------


def _src_conn(
    *,
    catalog: str | None = "main",
    schema: str | None = "analytics",
    volume: str = "my_volume",
    warehouse: str | None = "MY_WAREHOUSE",
    warehouse_id: str | None = None,
) -> DatabricksSrcConn:
    return DatabricksSrcConn(
        host_url=Secret("https://example.cloud.databricks.com"),
        token=Secret("dapi-test"),
        volume=Secret(volume),
        warehouse=(Secret(warehouse) if warehouse is not None else None),
        warehouse_id=(Secret(warehouse_id) if warehouse_id is not None else None),
        catalog=Secret(catalog) if catalog is not None else None,
        schema=Secret(schema) if schema is not None else None,
    )


def _dest_conn(
    *,
    catalog: str | None = "main",
    schema: str | None = "analytics",
    volume: str = "my_volume",
    warehouse: str | None = "MY_WAREHOUSE",
    warehouse_id: str | None = None,
) -> DatabricksDestConn:
    return DatabricksDestConn(
        host_url=Secret("https://example.cloud.databricks.com"),
        token=Secret("dapi-test"),
        volume=Secret(volume),
        warehouse=(Secret(warehouse) if warehouse is not None else None),
        warehouse_id=(Secret(warehouse_id) if warehouse_id is not None else None),
        catalog=Secret(catalog) if catalog is not None else None,
        schema=Secret(schema) if schema is not None else None,
    )


# ---------------------------------------------------------------------------
# Conn helpers
# ---------------------------------------------------------------------------


def test_host_value_unwraps_secret():
    conn = _src_conn()
    assert _impl._host_value(conn) == "https://example.cloud.databricks.com"


def test_token_value_unwraps_secret():
    conn = _src_conn()
    assert _impl._token_value(conn) == "dapi-test"


# ---------------------------------------------------------------------------
# _fully_qualify_table
# ---------------------------------------------------------------------------


class TestFullyQualifyTable:
    def test_three_part_passes_through(self):
        conn = _src_conn()
        assert _impl._fully_qualify_table(conn, "c.s.t") == "c.s.t"

    def test_two_part_prepends_catalog(self):
        conn = _src_conn(catalog="main", schema=None)
        assert (
            _impl._fully_qualify_table(conn, "analytics.users")
            == "main.analytics.users"
        )

    def test_one_part_prepends_catalog_and_schema(self):
        conn = _src_conn(catalog="main", schema="analytics")
        assert _impl._fully_qualify_table(conn, "users") == "main.analytics.users"

    # ``schema`` without ``catalog`` is rejected at DatabricksConn
    # construction (DATABRICKS_2), so the "1-part with only schema"
    # case is unreachable here. The remaining unreachable cases are
    # covered below.

    def test_one_part_with_neither_catalog_nor_schema_raises(self):
        conn = _src_conn(catalog=None, schema=None)
        with pytest.raises(DatabricksValidateException) as ei:
            _impl._fully_qualify_table(conn, "users")
        assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_10

    def test_two_part_without_catalog_raises(self):
        conn = _src_conn(catalog=None, schema=None)
        with pytest.raises(DatabricksValidateException) as ei:
            _impl._fully_qualify_table(conn, "analytics.users")
        assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_10


# ---------------------------------------------------------------------------
# _volume_path
# ---------------------------------------------------------------------------


class TestVolumePath:
    def test_one_part_volume_uses_table_catalog_and_schema(self):
        conn = _src_conn(volume="my_volume")
        path = _impl._volume_path(conn, "main.analytics.users")
        assert path == "/Volumes/main/analytics/my_volume"

    def test_two_part_volume_uses_table_catalog(self):
        conn = _src_conn(volume="staging.shared_volume")
        path = _impl._volume_path(conn, "main.analytics.users")
        assert path == "/Volumes/main/staging/shared_volume"

    def test_three_part_volume_is_used_verbatim(self):
        conn = _src_conn(volume="catalog.schema.volume")
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


# ---------------------------------------------------------------------------
# execute_query -- statement is scoped to the connection's catalog/schema
# ---------------------------------------------------------------------------


class TestExecuteQueryScoping:
    """``execute_query`` runs each statement in the connection's
    catalog/schema so an unqualified table name (``SELECT * FROM t``)
    resolves there instead of against the warehouse default."""

    @staticmethod
    def _execute_statement_kwargs(conn, statement, tmp_path) -> dict:
        # SDK enum imported here, not at module level, so the test module
        # still collects when databricks-sdk is absent (see module docstring).
        from databricks.sdk.service.sql import StatementState

        cfg = _impl.DatabricksReadCfg(
            conn=conn,
            queries=[statement],
            download_threads=1,
        )
        ws_client = MagicMock()
        response = MagicMock()
        response.status.state = StatementState.SUCCEEDED
        ws_client.statement_execution.execute_statement.return_value = response
        # Stub out the result-download machinery: this test only cares about
        # the arguments handed to execute_statement.
        with (
            patch.object(_impl, "_chunk_links_iterator"),
            patch.object(_impl, "_process_chunks"),
            patch.object(_impl, "_ChunkWriter"),
        ):
            _impl.execute_query(ws_client, cfg, statement, tmp_path, 0)
        return ws_client.statement_execution.execute_statement.call_args.kwargs

    def test_passes_connection_catalog_and_schema(self, tmp_path):
        conn = _src_conn(
            catalog="main",
            schema="analytics",
            warehouse=None,
            warehouse_id="a1b2c3d4e5f60718",
        )
        kwargs = self._execute_statement_kwargs(
            conn, "SELECT * FROM wld_country", tmp_path
        )
        assert kwargs["catalog"] == "main"
        assert kwargs["schema"] == "analytics"
        assert kwargs["warehouse_id"] == "a1b2c3d4e5f60718"
        assert kwargs["statement"] == "SELECT * FROM wld_country"

    def test_passes_none_when_catalog_and_schema_unset(self, tmp_path):
        conn = _src_conn(
            catalog=None,
            schema=None,
            warehouse=None,
            warehouse_id="a1b2c3d4e5f60718",
        )
        kwargs = self._execute_statement_kwargs(conn, "SELECT 1", tmp_path)
        assert kwargs["catalog"] is None
        assert kwargs["schema"] is None
