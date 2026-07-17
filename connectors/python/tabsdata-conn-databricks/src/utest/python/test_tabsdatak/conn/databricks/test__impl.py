"""Unit tests for the Databricks impl -- the pure-Python helpers
that don't need the SDK / SQL connector libraries at module level.

End-to-end coverage against a real Databricks workspace lives in
``test__plugin_integration.py``.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from tabsdatak.api import Secret

# noinspection PyProtectedMember
from tabsdatak.conn.databricks import DatabricksDestConn, DatabricksSrcConn, _impl
from tabsdatak.conn.databricks.error import (
    DatabricksErrorCode,
    DatabricksRuntimeException,
    DatabricksValidateException,
)


def _col(type_value, *, name="c", precision=None, scale=None):
    """Build a minimal Databricks result-manifest ``ColumnInfo`` stand-in
    (only the attributes ``_databricks_type_to_arrow`` reads)."""
    return SimpleNamespace(
        name=name,
        type_name=SimpleNamespace(value=type_value),
        type_precision=precision,
        type_scale=scale,
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


# ---------------------------------------------------------------------------
# _databricks_type_to_arrow -- manifest column type -> pyarrow type
# ---------------------------------------------------------------------------


class TestDatabricksTypeToArrow:
    @pytest.mark.parametrize(
        "dbx_type, arrow_type",
        [
            ("BOOLEAN", pa.bool_()),
            ("BYTE", pa.int8()),
            ("SHORT", pa.int16()),
            ("INT", pa.int32()),
            ("LONG", pa.int64()),
            ("FLOAT", pa.float32()),
            ("DOUBLE", pa.float64()),
            ("STRING", pa.string()),
            ("BINARY", pa.binary()),
            ("DATE", pa.date32()),
        ],
    )
    def test_scalar_types(self, dbx_type, arrow_type):
        assert _impl._databricks_type_to_arrow(_col(dbx_type)) == arrow_type

    def test_decimal_uses_precision_and_scale(self):
        got = _impl._databricks_type_to_arrow(_col("DECIMAL", precision=18, scale=2))
        assert got == pa.decimal128(18, 2)

    def test_timestamp_is_utc(self):
        assert _impl._databricks_type_to_arrow(_col("TIMESTAMP")) == pa.timestamp(
            "us", tz="UTC"
        )

    def test_timestamp_ntz_is_naive(self):
        assert _impl._databricks_type_to_arrow(_col("TIMESTAMP_NTZ")) == pa.timestamp(
            "us"
        )

    def test_unmapped_type_falls_back_to_string(self):
        # Exotic types can't be rebuilt from the flat manifest; string keeps the
        # empty result valid rather than failing the read.
        assert _impl._databricks_type_to_arrow(_col("ARRAY")) == pa.string()


# ---------------------------------------------------------------------------
# execute_query -- zero-row result still materializes a typed parquet
# ---------------------------------------------------------------------------


class TestExecuteQueryEmptyResult:
    @staticmethod
    def _run(tmp_path, columns):
        from databricks.sdk.service.sql import StatementState

        conn = _src_conn(
            catalog="main",
            schema="analytics",
            warehouse=None,
            warehouse_id="a1b2c3d4e5f60718",
        )
        cfg = _impl.DatabricksReadCfg(
            conn=conn,
            queries=["SELECT id, name FROM t WHERE 1=0"],
            download_threads=1,
        )
        ws_client = MagicMock()
        response = MagicMock()
        response.status.state = StatementState.SUCCEEDED
        response.statement_id = "stmt-1"
        # No external links => the chunk iterator yields nothing => 0 chunks.
        response.result = None
        response.manifest.schema.columns = columns
        ws_client.statement_execution.execute_statement.return_value = response
        return _impl.execute_query(
            ws_client, cfg, "SELECT id, name FROM t WHERE 1=0", tmp_path, 0
        )

    def test_zero_rows_writes_typed_empty_parquet(self, tmp_path):
        import pyarrow.parquet as pq

        out = self._run(
            tmp_path,
            [_col("LONG", name="id"), _col("STRING", name="name")],
        )
        assert out.exists()
        table = pq.read_table(out)
        assert table.num_rows == 0
        assert table.column_names == ["id", "name"]
        assert table.schema.field("id").type == pa.int64()
        assert table.schema.field("name").type == pa.string()

    def test_zero_rows_without_manifest_schema_raises(self, tmp_path):
        with pytest.raises(DatabricksRuntimeException) as ei:
            self._run(tmp_path, None)
        assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_11
