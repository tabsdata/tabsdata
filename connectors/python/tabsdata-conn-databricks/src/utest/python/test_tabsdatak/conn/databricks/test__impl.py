#
# Copyright 2026 Tabsdata Inc.
#

"""Unit tests for the Databricks impl -- the pure-Python helpers
that don't need the SDK / SQL connector libraries at module level.

End-to-end coverage against a real Databricks workspace lives in the
Databricks integration suite.
"""

from __future__ import annotations

from pathlib import Path
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


class _FakeCtx:
    """The slice of ``DestContext`` a write reads: the trx id a merge stamps its
    watermark rows with."""

    trx_id = "TRX_1"


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
# Native :name parameter binding (replaces the legacy {name} substitution)
# ---------------------------------------------------------------------------


class TestParamSqlType:
    @pytest.mark.parametrize(
        "value, sql_type",
        [
            (True, "BOOLEAN"),  # bool before int (bool is an int subclass)
            (42, "BIGINT"),
            (3.5, "DOUBLE"),
            ("x", "STRING"),
            (None, None),
        ],
    )
    def test_type_mapping(self, value, sql_type):
        assert _impl._param_sql_type(value) == sql_type

    def test_decimal_and_temporal(self):
        from datetime import date, datetime
        from decimal import Decimal

        assert _impl._param_sql_type(Decimal("1.5")) == "DECIMAL"
        assert _impl._param_sql_type(datetime(2026, 1, 1)) == "TIMESTAMP"
        assert _impl._param_sql_type(date(2026, 1, 1)) == "DATE"


class TestParamValueStr:
    def test_bool_is_lowercase(self):
        assert _impl._param_value_str(True) == "true"
        assert _impl._param_value_str(False) == "false"

    def test_int_and_none(self):
        assert _impl._param_value_str(0) == "0"
        assert _impl._param_value_str(None) is None


class TestStatementParameters:
    def test_none_or_empty_binds_no_parameters(self):
        assert _impl._statement_parameters(None) is None
        assert _impl._statement_parameters({}) is None

    def test_builds_typed_parameter_items(self):
        from databricks.sdk.service.sql import StatementParameterListItem

        params = _impl._statement_parameters({"wm": 0, "name": "alice"})
        assert params == [
            StatementParameterListItem(name="wm", value="0", type="BIGINT"),
            StatementParameterListItem(name="name", value="alice", type="STRING"),
        ]


class TestExecuteQueryParameterBinding:
    """``execute_query`` binds ``:name`` markers natively via
    ``parameters=[...]`` and leaves the SQL text untouched."""

    @staticmethod
    def _execute_statement_kwargs(statement, initial_values, tmp_path) -> dict:
        from databricks.sdk.service.sql import StatementState

        conn = _src_conn(warehouse=None, warehouse_id="a1b2c3d4e5f60718")
        cfg = _impl.DatabricksReadCfg(
            conn=conn,
            queries=[statement],
            initial_values=initial_values,
            download_threads=1,
        )
        ws_client = MagicMock()
        response = MagicMock()
        response.status.state = StatementState.SUCCEEDED
        ws_client.statement_execution.execute_statement.return_value = response
        with (
            patch.object(_impl, "_chunk_links_iterator"),
            patch.object(_impl, "_process_chunks"),
            patch.object(_impl, "_ChunkWriter"),
        ):
            _impl.execute_query(ws_client, cfg, statement, tmp_path, 0)
        return ws_client.statement_execution.execute_statement.call_args.kwargs

    def test_binds_named_parameters_and_keeps_sql(self, tmp_path):
        from databricks.sdk.service.sql import StatementParameterListItem

        stmt = "SELECT * FROM t WHERE unix_timestamp(data_ts) > :wm"
        kwargs = self._execute_statement_kwargs(stmt, {"wm": 0}, tmp_path)
        # SQL text keeps the :wm marker -- no {wm}, no interpolated 0.
        assert kwargs["statement"] == stmt
        assert kwargs["parameters"] == [
            StatementParameterListItem(name="wm", value="0", type="BIGINT")
        ]

    def test_no_initial_values_passes_none_parameters(self, tmp_path):
        kwargs = self._execute_statement_kwargs("SELECT 1", None, tmp_path)
        assert kwargs["parameters"] is None


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


# ---------------------------------------------------------------------------
# write_all_tables -- up-front column rejection
# ---------------------------------------------------------------------------


def _parquet(path, **columns):
    """A real one-row parquet at `path`; `write_all_tables` reads its schema."""
    import pyarrow.parquet as pq

    pq.write_table(pa.table(columns or {"c": pa.array([1], pa.int64())}), str(path))
    return path


def test_write_all_tables_rejects_a_time_column_before_connecting(tmp_path):
    """A time column has to be refused up front, not by COPY INTO after the
    upload and the CREATE already ran."""
    import datetime

    parquet = _parquet(
        tmp_path / "t.parquet",
        ok=pa.array([1], pa.int64()),
        clock=pa.array([datetime.time(9, 30, 15)], pa.time64("ns")),
    )
    cfg = _impl.DatabricksWriteCfg(
        conn=_dest_conn(),
        target_tables=["main.analytics.events"],
        if_table_exists="append",
        schema_evolution="strict",
        ctx=_FakeCtx(),
    )
    # No workspace/SQL fixture: the check must fire before any connect.
    with pytest.raises(DatabricksRuntimeException) as ei:
        _impl.write_all_tables(cfg, [parquet])
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_13
    assert "'clock'" in str(ei.value)
    assert "'ok'" not in str(ei.value)
    assert "main.analytics.events" in str(ei.value)


@pytest.mark.parametrize("unit", ["s", "ms", "us", "ns"])
def test_write_all_tables_rejects_every_time_width(tmp_path, unit):
    import datetime

    arrow = pa.time32(unit) if unit in ("s", "ms") else pa.time64(unit)
    parquet = _parquet(
        tmp_path / f"t_{unit}.parquet",
        clock=pa.array([datetime.time(1, 2, 3)], arrow),
    )
    cfg = _impl.DatabricksWriteCfg(
        conn=_dest_conn(),
        target_tables=["main.analytics.events"],
        if_table_exists="append",
        schema_evolution="strict",
        ctx=_FakeCtx(),
    )
    with pytest.raises(DatabricksRuntimeException):
        _impl.write_all_tables(cfg, [parquet])


def test_write_all_tables_names_the_table_the_offending_file_targets(tmp_path):
    import datetime

    clean = _parquet(tmp_path / "clean.parquet")
    dirty = _parquet(
        tmp_path / "dirty.parquet",
        clock=pa.array([datetime.time(1, 2, 3)], pa.time64("us")),
    )
    cfg = _impl.DatabricksWriteCfg(
        conn=_dest_conn(),
        target_tables=["main.analytics.first", "main.analytics.second"],
        if_table_exists="append",
        schema_evolution="strict",
        ctx=_FakeCtx(),
    )
    with pytest.raises(DatabricksRuntimeException) as ei:
        _impl.write_all_tables(cfg, [clean, dirty])
    assert "main.analytics.second" in str(ei.value)


# ---------------------------------------------------------------------------
# Staging table from the volume file's schema
# ---------------------------------------------------------------------------


class _RecordingCursor:
    """Captures every statement, since the DDL is the whole behaviour here."""

    def __init__(self) -> None:
        self.log: list[tuple[str, dict]] = []

    def execute(self, statement: str, **parameters) -> None:
        self.log.append((statement, parameters))


def _staged(target: str, path: str = "/Volumes/c/s/v/in.parquet"):
    cursor = _RecordingCursor()
    name = _impl._create_temp_table_from_volume_schema(cursor, path, target)
    return name, cursor.log[0][0]


class TestTempTableName:
    def test_keeps_the_targets_catalog_and_schema(self):
        """A real catalog table, so it is created where the write is happening."""
        name = _impl._temp_table_name("main.analytics.orders")
        assert name.startswith("`main`.`analytics`.`orders_")

    def test_suffixes_only_the_last_part(self):
        name = _impl._temp_table_name("main.analytics.orders")
        assert len(_impl.unfqn(name, _impl._DATABRICKS_FQN_DEF)) == 3

    def test_is_unique_per_call(self):
        """Two writes to one target must not land on the same copy."""
        target = "main.analytics.orders"
        assert _impl._temp_table_name(target) != _impl._temp_table_name(target)

    def test_never_equals_the_target(self):
        """A copy spelled like the target would be merged into itself."""
        name = _impl._temp_table_name("main.analytics.orders")
        assert name != _impl._fq_for_sql("main.analytics.orders")

    def test_returns_a_quoted_name(self):
        assert _impl._temp_table_name("c.s.t").startswith("`c`.`s`.`t_")

    def test_quotes_a_part_that_needs_it(self):
        assert _impl._temp_table_name("`my-cat`.s.t").startswith("`my-cat`.`s`.`t_")

    @pytest.mark.parametrize(
        "target,prefix",
        [
            ("c.s.`weird tbl`", "`c`.`s`.`weird tbl_"),
            # A dot or a backtick inside a quoted part must not re-split the
            # name: quoting is applied to the split parts, never re-derived
            # from a bare string.
            ("c.s.`a.b`", "`c`.`s`.`a.b_"),
            ("c.s.`we``ird`", "`c`.`s`.`we``ird_"),
        ],
    )
    def test_stays_three_part_for_an_awkward_target(self, target, prefix):
        name = _impl._temp_table_name(target)
        assert name.startswith(prefix)
        assert len(_impl.unfqn(name, _impl._DATABRICKS_FQN_DEF)) == 3


class TestCreateTempTableFromVolumeSchema:
    def test_creates_a_delta_table_from_the_volume_file(self):
        name, sql = _staged("main.analytics.orders")
        assert sql == (
            f"CREATE TABLE {name} USING DELTA AS SELECT * FROM "
            f"read_files('/Volumes/c/s/v/in.parquet', format => 'parquet') LIMIT 0"
        )

    def test_is_not_temporary(self):
        """A real catalog table: it outlives the connection, which is why the
        caller has to drop it."""
        _, sql = _staged("c.s.t")
        assert "TEMPORARY" not in sql

    def test_takes_the_schema_without_the_rows(self):
        """``LIMIT 0`` is what makes this a schema copy rather than a load; the
        rows arrive via COPY INTO."""
        _, sql = _staged("c.s.t")
        assert sql.endswith("LIMIT 0")

    def test_reads_the_file_the_caller_named(self):
        _, sql = _staged("c.s.t", path="/Volumes/x/y/z/other.parquet")
        assert "read_files('/Volumes/x/y/z/other.parquet', format => 'parquet')" in sql

    def test_returns_the_name_it_created(self):
        name, sql = _staged("c.s.t")
        assert f"CREATE TABLE {name} " in sql

    def test_creates_beside_the_target_not_over_it(self):
        name, sql = _staged("main.analytics.orders")
        assert name.startswith("`main`.`analytics`.")
        assert "`orders`" not in sql

    def test_retries_a_path_not_found(self, monkeypatch):
        """The statement reads the volume, so it has to go through the retry that
        absorbs an upload not yet visible to the warehouse."""
        calls: list[str] = []

        def spy(cursor, statement, parameters):
            calls.append(statement)

        monkeypatch.setattr(_impl, "_execute_volume_statement", spy)
        _impl._create_temp_table_from_volume_schema(
            _RecordingCursor(), "/Volumes/c/s/v/in.parquet", "c.s.t"
        )
        assert len(calls) == 1 and calls[0].startswith("CREATE TABLE ")

    def test_passes_no_extra_execute_parameters(self):
        cursor = _RecordingCursor()
        _impl._create_temp_table_from_volume_schema(
            cursor, "/Volumes/c/s/v/in.parquet", "c.s.t"
        )
        assert cursor.log[0][1] == {}


class TestDropTempTable:
    def test_drops_the_table_it_is_given(self):
        cursor = _RecordingCursor()
        _impl.drop_temp_table(cursor, "`main`.`analytics`.`orders_abc12345`")
        assert cursor.log == [
            ("DROP TABLE IF EXISTS `main`.`analytics`.`orders_abc12345`", {})
        ]

    def test_is_conditional(self):
        """It belongs in a ``finally``, so it has to tolerate a temp table the
        create never got to make."""
        cursor = _RecordingCursor()
        _impl.drop_temp_table(cursor, "`c`.`s`.`t_1`")
        assert "IF EXISTS" in cursor.log[0][0]

    def test_round_trips_the_created_name(self):
        create = _RecordingCursor()
        name = _impl._create_temp_table_from_volume_schema(
            create, "/Volumes/c/s/v/in.parquet", "c.s.`weird tbl`"
        )
        drop = _RecordingCursor()
        _impl.drop_temp_table(drop, name)
        assert drop.log[0][0].endswith(name)


# ---------------------------------------------------------------------------
# The volume-schema projection
#
# The select list the merge dialect builds a target with, and the one a
# scratch table is built with.
# ---------------------------------------------------------------------------


class TestVolumeSchemaProjection:
    def test_copies_every_column_by_default(self):
        assert _impl._volume_schema_projection() == "*"

    def test_drops_one_column(self):
        assert _impl._volume_schema_projection(["op"]) == "* EXCEPT (`op`)"

    def test_drops_several_columns(self):
        projection = _impl._volume_schema_projection(["op", "seq"])
        assert projection == "* EXCEPT (`op`, `seq`)"

    def test_quotes_a_name_that_needs_it(self):
        projection = _impl._volume_schema_projection(["weird col"])
        assert projection == "* EXCEPT (`weird col`)"


# ---------------------------------------------------------------------------
# watermark_column on an append
#
# An append is not idempotent on its own: a rerun of the same transaction
# uploads the batch again and COPY INTO appends it a second time. Naming a
# watermark closes it -- the column is written into the parquet before it goes
# to the volume so the load stamps the rows it brings in, and a rerun that
# finds the target already carrying this trx's id does nothing at all.
# ---------------------------------------------------------------------------

_WATERMARK = "wm"
_WM_TABLE = "main.analytics.events"
_WM_SQL_FQ = "`main`.`analytics`.`events`"


class _BulkCursor:
    """Records every statement and answers the two probes the watermark makes.

    The column set has to move: the probe runs twice on one write -- once for
    the rerun guard and once before the ALTER -- and a fake answering from a
    frozen set would report a table its own DDL had already changed, so the
    ALTER would be emitted for a column that is already there.
    """

    def __init__(self, columns=(), *, stamped=False) -> None:
        self.log: list[str] = []
        self._columns = {name.lower() for name in columns}
        self._stamped = stamped
        self._rows: list[tuple] = []

    def execute(self, statement: str, **parameters) -> None:
        self.log.append(statement)
        if statement.startswith("SELECT COUNT(*) > 0"):
            self._rows = [(self._stamped,)]
            return
        if statement.startswith("SELECT COUNT(*)"):
            # The information_schema probe binds the column name as a literal.
            wanted = statement.rsplit("= '", 1)[1].rstrip("'")
            self._rows = [(1 if wanted in self._columns else 0,)]
            return
        if "ADD COLUMNS" in statement:
            self._columns.add(
                statement.rsplit("(`", 1)[1].split("`", 1)[0].lower()
            )
        self._rows = []

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


def _bulk_clients(monkeypatch, cursor):
    """Stub the workspace client and SQL connection ``write_all_tables`` opens,
    recording every file handed to the volume."""
    uploaded: list[str] = []

    def upload(target, contents):
        uploaded.append(target)

    ws_client = SimpleNamespace(files=SimpleNamespace(upload=upload))
    sql_conn = SimpleNamespace(cursor=lambda: cursor, close=lambda: None)
    monkeypatch.setattr(_impl, "_build_workspace_client", lambda conn: ws_client)
    monkeypatch.setattr(
        _impl, "_build_sql_connection", lambda ws, conn: sql_conn
    )
    return uploaded


def _staged_batches(monkeypatch):
    """Capture the local path and rows of every volume upload, read while the
    file still exists -- the rewritten batch lives in a temp directory removed
    as soon as it has been staged."""
    import polars as pl

    staged: list[tuple[Path, list[dict]]] = []
    original = _impl.upload_file_to_volume

    def capture(ws_client, conn, fq_table, local_path):
        staged.append((Path(local_path), pl.read_parquet(local_path).to_dicts()))
        return original(ws_client, conn, fq_table, local_path)

    monkeypatch.setattr(_impl, "upload_file_to_volume", capture)
    return staged


def _wm_cfg(**overrides) -> _impl.DatabricksWriteCfg:
    kwargs: dict = dict(
        conn=_dest_conn(),
        target_tables=[_WM_TABLE],
        if_table_exists="append",
        schema_evolution="strict",
        ctx=_FakeCtx(),
        watermark_column=_WATERMARK,
    )
    kwargs.update(overrides)
    return _impl.DatabricksWriteCfg(**kwargs)


def _wm_parquet(tmp_path) -> Path:
    import polars as pl

    path = tmp_path / "in.parquet"
    pl.DataFrame({"id": [1, 2]}).write_parquet(path)
    return path


def _alters(cursor) -> list[str]:
    return [s for s in cursor.log if s.startswith("ALTER TABLE")]


def test_append_watermark_stamps_every_staged_row(monkeypatch, tmp_path):
    cursor = _BulkCursor(("id", _WATERMARK))
    _bulk_clients(monkeypatch, cursor)
    staged = _staged_batches(monkeypatch)
    _impl.write_all_tables(_wm_cfg(), [_wm_parquet(tmp_path)])
    _, rows = staged[0]
    assert [row[_WATERMARK] for row in rows] == ["TRX_1", "TRX_1"]


def test_append_watermark_stages_a_copy_keeping_the_basename(monkeypatch, tmp_path):
    # The staged name is built from the local basename, so the rewrite has to be
    # indistinguishable from the file the write was handed.
    cursor = _BulkCursor(("id", _WATERMARK))
    _bulk_clients(monkeypatch, cursor)
    staged = _staged_batches(monkeypatch)
    parquet = _wm_parquet(tmp_path)
    _impl.write_all_tables(_wm_cfg(), [parquet])
    path, _ = staged[0]
    assert path != parquet
    assert path.name == parquet.name


def test_append_without_a_watermark_stages_what_it_was_handed(monkeypatch, tmp_path):
    cursor = _BulkCursor(("id",))
    _bulk_clients(monkeypatch, cursor)
    staged = _staged_batches(monkeypatch)
    parquet = _wm_parquet(tmp_path)
    _impl.write_all_tables(_wm_cfg(watermark_column=None), [parquet])
    path, rows = staged[0]
    assert path == parquet
    assert rows == [{"id": 1}, {"id": 2}]


def test_append_watermark_removes_the_copy_once_it_is_staged(monkeypatch, tmp_path):
    cursor = _BulkCursor(("id", _WATERMARK))
    _bulk_clients(monkeypatch, cursor)
    staged = _staged_batches(monkeypatch)
    _impl.write_all_tables(_wm_cfg(), [_wm_parquet(tmp_path)])
    path, _ = staged[0]
    assert not path.exists()


def test_append_watermark_adds_the_column_to_the_target(monkeypatch, tmp_path):
    cursor = _BulkCursor(("id",))
    _bulk_clients(monkeypatch, cursor)
    _staged_batches(monkeypatch)
    _impl.write_all_tables(_wm_cfg(), [_wm_parquet(tmp_path)])
    assert _alters(cursor) == [
        f"ALTER TABLE {_WM_SQL_FQ} ADD COLUMNS (`{_WATERMARK}` STRING)"
    ]


def test_append_watermark_leaves_an_existing_column_alone(monkeypatch, tmp_path):
    cursor = _BulkCursor(("id", _WATERMARK))
    _bulk_clients(monkeypatch, cursor)
    _staged_batches(monkeypatch)
    _impl.write_all_tables(_wm_cfg(), [_wm_parquet(tmp_path)])
    assert _alters(cursor) == []


def test_append_watermark_adds_the_column_between_create_and_copy(
    monkeypatch, tmp_path
):
    # Under strict mergeSchema is off, so a batch carrying a column the target
    # lacks fails the COPY -- and a column added afterwards would be NULL in the
    # rows just written.
    cursor = _BulkCursor(("id",))
    _bulk_clients(monkeypatch, cursor)
    _staged_batches(monkeypatch)
    _impl.write_all_tables(_wm_cfg(), [_wm_parquet(tmp_path)])
    create = next(i for i, s in enumerate(cursor.log) if s.startswith("CREATE TABLE"))
    alter = cursor.log.index(_alters(cursor)[0])
    copy = next(i for i, s in enumerate(cursor.log) if s.startswith("COPY INTO"))
    assert create < alter < copy


def test_append_watermark_is_added_under_update_too(monkeypatch, tmp_path):
    """mergeSchema='true' would land it, but one place owns the column so the
    two settings cannot disagree about who added it."""
    cursor = _BulkCursor(("id",))
    _bulk_clients(monkeypatch, cursor)
    _staged_batches(monkeypatch)
    _impl.write_all_tables(
        _wm_cfg(schema_evolution="update"), [_wm_parquet(tmp_path)]
    )
    assert len(_alters(cursor)) == 1


def test_append_skips_entirely_when_the_trx_is_already_stamped(monkeypatch, tmp_path):
    cursor = _BulkCursor(("id", _WATERMARK), stamped=True)
    _bulk_clients(monkeypatch, cursor)
    staged = _staged_batches(monkeypatch)
    _impl.write_all_tables(_wm_cfg(), [_wm_parquet(tmp_path)])
    assert staged == []
    assert not any(s.startswith("COPY INTO") for s in cursor.log)


def test_append_proceeds_when_the_target_carries_another_trx(monkeypatch, tmp_path):
    cursor = _BulkCursor(("id", _WATERMARK), stamped=False)
    _bulk_clients(monkeypatch, cursor)
    staged = _staged_batches(monkeypatch)
    _impl.write_all_tables(_wm_cfg(), [_wm_parquet(tmp_path)])
    assert len(staged) == 1
    assert any(s.startswith("COPY INTO") for s in cursor.log)


def test_append_checks_the_watermark_before_it_stages_anything(monkeypatch, tmp_path):
    # A rerun with nothing to do should not pay to rewrite and upload a file it
    # is about to decline to load.
    cursor = _BulkCursor(("id", _WATERMARK))
    _bulk_clients(monkeypatch, cursor)
    _staged_batches(monkeypatch)
    _impl.write_all_tables(_wm_cfg(), [_wm_parquet(tmp_path)])
    probe = next(
        i for i, s in enumerate(cursor.log) if s.startswith("SELECT COUNT(*) > 0")
    )
    create = next(i for i, s in enumerate(cursor.log) if s.startswith("CREATE TABLE"))
    assert probe < create


@pytest.mark.parametrize(
    "trx_id,literal",
    [("plain", "'plain'"), ("it's", "'it\\'s'"), ("a\\b", "'a\\\\b'")],
)
def test_append_escapes_the_trx_id_in_the_probe(monkeypatch, tmp_path, trx_id, literal):
    # There is nowhere to bind a value in a statement built by hand.
    class _Ctx:
        pass

    ctx = _Ctx()
    ctx.trx_id = trx_id
    cursor = _BulkCursor(("id", _WATERMARK))
    _bulk_clients(monkeypatch, cursor)
    _staged_batches(monkeypatch)
    _impl.write_all_tables(_wm_cfg(ctx=ctx), [_wm_parquet(tmp_path)])
    probe = next(s for s in cursor.log if s.startswith("SELECT COUNT(*) > 0"))
    assert probe.endswith(f"= {literal}")


def test_a_replace_never_stamps_even_when_handed_a_watermark(monkeypatch, tmp_path):
    """DatabricksDest refuses the pairing, and the write path declines it again
    rather than resting on a validation two layers away."""
    cursor = _BulkCursor(("id",))
    _bulk_clients(monkeypatch, cursor)
    staged = _staged_batches(monkeypatch)
    parquet = _wm_parquet(tmp_path)
    _impl.write_all_tables(_wm_cfg(if_table_exists="replace"), [parquet])
    assert staged[0][0] == parquet
    assert _alters(cursor) == []
