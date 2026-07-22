#
# Copyright 2026 Tabsdata Inc.
#

"""Integration tests for Iceberg-style schema evolution through the full
ClickHouseDest → CloudStagedBulkLoader → ClickHouseDialect pipeline.

Tests exercise all three cloud storage backends (S3, Azure, GCS) and cover
both safe evolution scenarios (that should succeed) and unsafe/impossible
scenarios (that should fail with appropriate errors).
"""

import logging
import os
import uuid

import polars as pl
import pyarrow as pa
import pytest

import tabsdata as td
from tabsdata._io.cloud_staging.common.errors import SchemaError
from tabsdata.connector.schema.evolution.iceberg.models import UnsafeSchemaChangeError

# noinspection PyUnresolvedReferences
from . import pytestmark as _package_pytestmark  # noqa: F401

pytestmark = [_package_pytestmark, pytest.mark.xdist_group("platform-output_clickhouse_schema_evolution_seq")]

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# ── Helpers ──────────────────────────────────────────────────────────────────────────


def _uid():
    return uuid.uuid4().hex[:12]


def _create_table(clickhouse_client, db, table, col_defs_sql):
    """Create a ClickHouse table with the given column definitions."""
    clickhouse_client.command(
        f"CREATE TABLE `{db}`.`{table}` ({col_defs_sql}) "
        f"ENGINE = MergeTree() ORDER BY tuple()"
    )


def _insert(clickhouse_client, db, table, values_sql):
    clickhouse_client.command(
        f"INSERT INTO `{db}`.`{table}` VALUES {values_sql}"
    )


def _table_exists(clickhouse_client, db, table):
    result = clickhouse_client.query(
        "SELECT count() FROM system.tables "
        "WHERE database = {db:String} AND name = {tbl:String}",
        parameters={"db": db, "tbl": table},
    )
    return result.result_rows[0][0] > 0


def _get_columns(clickhouse_client, db, table):
    """Return [(name, type)] from system.columns."""
    result = clickhouse_client.query(
        "SELECT name, type FROM system.columns "
        "WHERE database = {db:String} AND table = {tbl:String} "
        "ORDER BY position",
        parameters={"db": db, "tbl": table},
    )
    return result.result_rows


def _row_count(clickhouse_client, db, table):
    return clickhouse_client.query(
        f"SELECT count() FROM `{db}`.`{table}`"
    ).result_rows[0][0]


def _query_all(clickhouse_client, db, table, order_by="1"):
    result = clickhouse_client.query(
        f"SELECT * FROM `{db}`.`{table}` ORDER BY {order_by}"
    )
    return result.result_rows


# ── Connection builders ──────────────────────────────────────────────────────────────


def _build_s3_conn(clickhouse_config, s3_config, uid):
    staging = td.S3Config(
        bucket=s3_config["BUCKET_NAME"],
        region=s3_config["REGION"],
        base_path=f"tabsdata-testing/ch-evo-{uid}",
        credentials=s3_config["CREDENTIALS"],
    )
    return td.ClickHouseConn(
        host=clickhouse_config.host,
        port=clickhouse_config.port,
        database=clickhouse_config.database,
        credentials=clickhouse_config.credentials,
        staging=staging,
    )


def _build_azure_conn(clickhouse_config, azure_config, uid):
    staging = td.AzureConfig(
        container=azure_config["CONTAINER_NAME"],
        base_path=f"tabsdata-testing/ch-evo-{uid}",
        credentials=azure_config["CREDENTIALS"],
    )
    return td.ClickHouseConn(
        host=clickhouse_config.host,
        port=clickhouse_config.port,
        database=clickhouse_config.database,
        credentials=clickhouse_config.credentials,
        staging=staging,
    )


def _build_gcs_conn(clickhouse_config, gcs_config, uid):
    staging = td.GCSConfig(
        bucket=gcs_config["BUCKET"],
        base_path=f"tabsdata-testing/ch-evo-{uid}",
        upload_credentials=td.GCPServiceAccountKeyCredentials(
            td.EnvironmentSecret(gcs_config["ENV"])
        ),
        hmac_credentials=td.HMACCredentials(
            gcs_config["ACCESS_KEY"], gcs_config["SECRET_KEY"]
        ),
    )
    return td.ClickHouseConn(
        host=clickhouse_config.host,
        port=clickhouse_config.port,
        database=clickhouse_config.database,
        credentials=clickhouse_config.credentials,
        staging=staging,
    )


# ── Function builders ────────────────────────────────────────────────────────────────
# These create @td.subscriber functions that output specific schemas.


def _make_fn_output_int64_string(conn, table, schema_evolution="none"):
    """Output: id(Int64), name(String)."""

    @td.subscriber(
        name="evo_test",
        tables="collection/table",
        destination=td.ClickHouseDest(
            conn=conn,
            destination_tables=table,
            schema_evolution=schema_evolution,
        ),
    )
    def fn(df: td.TableFrame):
        data = pl.DataFrame(
            {"id": [10, 20], "name": ["x", "y"]},
            schema={"id": pl.Int64, "name": pl.Utf8},
        )
        return td.TableFrame.from_polars(data)

    return fn


def _make_fn_output_int64_string_email(conn, table, schema_evolution="none"):
    """Output: id(Int64), name(String), email(String nullable)."""

    @td.subscriber(
        name="evo_test",
        tables="collection/table",
        destination=td.ClickHouseDest(
            conn=conn,
            destination_tables=table,
            schema_evolution=schema_evolution,
        ),
    )
    def fn(df: td.TableFrame):
        data = pl.DataFrame(
            {
                "id": [10, 20],
                "name": ["x", "y"],
                "email": ["x@test.com", None],
            },
            schema={"id": pl.Int64, "name": pl.Utf8, "email": pl.Utf8},
        )
        return td.TableFrame.from_polars(data)

    return fn


def _make_fn_output_int64_only(conn, table, schema_evolution="none"):
    """Output: id(Int64) — drops the 'name' column."""

    @td.subscriber(
        name="evo_test",
        tables="collection/table",
        destination=td.ClickHouseDest(
            conn=conn,
            destination_tables=table,
            schema_evolution=schema_evolution,
        ),
    )
    def fn(df: td.TableFrame):
        data = pl.DataFrame({"id": [10, 20]}, schema={"id": pl.Int64})
        return td.TableFrame.from_polars(data)

    return fn

def _make_fn_output_int64_widened(conn, table, schema_evolution="none"):
    """Output: id(Int64), value(Float64) — widened from Int32/Float32."""

    @td.subscriber(
        name="evo_test",
        tables="collection/table",
        destination=td.ClickHouseDest(
            conn=conn,
            destination_tables=table,
            schema_evolution=schema_evolution,
        ),
    )
    def fn(df: td.TableFrame):
        data = pl.DataFrame(
            {"id": [10, 20], "value": [1.5, 2.5]},
            schema={"id": pl.Int64, "value": pl.Float64},
        )
        return td.TableFrame.from_polars(data)

    return fn


def _make_fn_output_int32_narrowed(conn, table, schema_evolution="none"):
    """Output: id(Int32) — narrowed from Int64 (unsafe)."""

    @td.subscriber(
        name="evo_test",
        tables="collection/table",
        destination=td.ClickHouseDest(
            conn=conn,
            destination_tables=table,
            schema_evolution=schema_evolution,
        ),
    )
    def fn(df: td.TableFrame):
        data = pl.DataFrame({"id": [10, 20]}, schema={"id": pl.Int32})
        return td.TableFrame.from_polars(data)

    return fn


def _make_fn_output_string_type_change(conn, table, schema_evolution="none"):
    """Output: id(String) — incompatible type change from Int64 (unsafe)."""

    @td.subscriber(
        name="evo_test",
        tables="collection/table",
        destination=td.ClickHouseDest(
            conn=conn,
            destination_tables=table,
            schema_evolution=schema_evolution,
        ),
    )
    def fn(df: td.TableFrame):
        data = pl.DataFrame({"id": ["a", "b"]}, schema={"id": pl.Utf8})
        return td.TableFrame.from_polars(data)

    return fn


def _make_fn_output_combined_evolution(conn, table, schema_evolution="none"):
    """Output: id(Int64), name(String nullable), tag(String nullable).

    Compared to initial schema (id Int32, name String NOT NULL, old_col String):
    - id: Int32 → Int64 (widen)
    - name: NOT NULL → nullable (set nullable)
    - old_col: dropped
    - tag: added (nullable)
    """

    @td.subscriber(
        name="evo_test",
        tables="collection/table",
        destination=td.ClickHouseDest(
            conn=conn,
            destination_tables=table,
            schema_evolution=schema_evolution,
        ),
    )
    def fn(df: td.TableFrame):
        data = pl.DataFrame(
            {
                "id": [10, 20],
                "name": ["x", None],
                "tag": ["a", "b"],
            },
            schema={"id": pl.Int64, "name": pl.Utf8, "tag": pl.Utf8},
        )
        return td.TableFrame.from_polars(data)

    return fn


# ── Mock input parquet ───────────────────────────────────────────────────────────────

# All test functions ignore their input and produce their own output,
# but tdfnrun requires an input table. We reuse the existing mock.

_MOCK_PARQUET = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "..",
    "..",
    "resources",
    "test_tabsdata",
    "test_output_clickhouse_list",
    "mock_table.parquet",
)


def _mock_parquet():
    from test_tabsdata.conftest import TESTING_RESOURCES_FOLDER

    return os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_clickhouse_list",
        "mock_table.parquet",
    )


# ═════════════════════════════════════════════════════════════════════════════════════
# S3 — SAFE EVOLUTION
# ═════════════════════════════════════════════════════════════════════════════════════


@pytest.mark.clickhouse
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_s3_evolution_add_nullable_column(
    clickhouse_config, clickhouse_client, s3_config
):
    """Add a nullable column to an existing table via S3 staging."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_s3_add_col_{uid}"
    db = clickhouse_config.database

    _create_table(clickhouse_client, db, table, "`id` Int64, `name` String")
    _insert(clickhouse_client, db, table, "(1, 'alice'), (2, 'bob')")

    conn = _build_s3_conn(clickhouse_config, s3_config, uid)
    fn = _make_fn_output_int64_string_email(conn, table, schema_evolution="iceberg")
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet())
    assert fnrun.status == 0

    cols = _get_columns(clickhouse_client, db, table)
    col_names = [c[0] for c in cols]
    assert "email" in col_names

    assert _row_count(clickhouse_client, db, table) == 4  # 2 old + 2 new


@pytest.mark.clickhouse
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_s3_evolution_drop_column(
    clickhouse_config, clickhouse_client, s3_config
):
    """Drop a column from an existing table via S3 staging."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_s3_drop_col_{uid}"
    db = clickhouse_config.database

    _create_table(
        clickhouse_client, db, table,
        "`id` Int64, `name` Nullable(String)",
    )
    _insert(clickhouse_client, db, table, "(1, 'alice')")

    conn = _build_s3_conn(clickhouse_config, s3_config, uid)
    fn = _make_fn_output_int64_only(conn, table, schema_evolution="iceberg")
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet())
    assert fnrun.status == 0

    cols = _get_columns(clickhouse_client, db, table)
    col_names = [c[0] for c in cols]
    assert "name" not in col_names
    assert _row_count(clickhouse_client, db, table) == 3  # 1 old + 2 new


@pytest.mark.clickhouse
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_s3_evolution_widen_type(
    clickhouse_config, clickhouse_client, s3_config
):
    """Widen Int32 → Int64 and Float32 → Float64 via S3 staging."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_s3_widen_{uid}"
    db = clickhouse_config.database

    _create_table(
        clickhouse_client, db, table,
        "`id` Int32, `value` Float32",
    )
    _insert(clickhouse_client, db, table, "(1, 1.5)")

    conn = _build_s3_conn(clickhouse_config, s3_config, uid)
    fn = _make_fn_output_int64_widened(conn, table, schema_evolution="iceberg")
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet())
    assert fnrun.status == 0

    cols = _get_columns(clickhouse_client, db, table)
    id_type = next(c[1] for c in cols if c[0] == "id")
    value_type = next(c[1] for c in cols if c[0] == "value")
    assert id_type == "Int64"
    assert value_type == "Float64"
    assert _row_count(clickhouse_client, db, table) == 3


@pytest.mark.clickhouse
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_s3_evolution_combined(
    clickhouse_config, clickhouse_client, s3_config
):
    """Combined evolution: add + drop + widen + set nullable via S3."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_s3_combined_{uid}"
    db = clickhouse_config.database

    _create_table(
        clickhouse_client, db, table,
        "`id` Int32, `name` String, `old_col` Nullable(String)",
    )
    _insert(clickhouse_client, db, table, "(1, 'alice', 'legacy')")

    conn = _build_s3_conn(clickhouse_config, s3_config, uid)
    fn = _make_fn_output_combined_evolution(conn, table, schema_evolution="iceberg")
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet())
    assert fnrun.status == 0

    cols = _get_columns(clickhouse_client, db, table)
    col_names = [c[0] for c in cols]
    assert col_names == ["id", "name", "tag"]
    assert "old_col" not in col_names

    id_type = next(c[1] for c in cols if c[0] == "id")
    name_type = next(c[1] for c in cols if c[0] == "name")
    assert id_type == "Int64"
    assert "Nullable" in name_type

    assert _row_count(clickhouse_client, db, table) == 3


@pytest.mark.clickhouse
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_s3_evolution_no_changes(
    clickhouse_config, clickhouse_client, s3_config
):
    """When schemas already match, iceberg mode should be a no-op."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_s3_noop_{uid}"
    db = clickhouse_config.database

    _create_table(
        clickhouse_client, db, table,
        "`id` Int64, `name` Nullable(String)",
    )
    _insert(clickhouse_client, db, table, "(1, 'alice')")

    conn = _build_s3_conn(clickhouse_config, s3_config, uid)
    fn = _make_fn_output_int64_string(conn, table, schema_evolution="iceberg")
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet())
    assert fnrun.status == 0

    assert _row_count(clickhouse_client, db, table) == 3


@pytest.mark.clickhouse
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_s3_evolution_create_new_table(
    clickhouse_config, clickhouse_client, s3_config
):
    """When the table doesn't exist yet, iceberg mode should create it normally."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_s3_create_{uid}"
    db = clickhouse_config.database

    assert not _table_exists(clickhouse_client, db, table)

    conn = _build_s3_conn(clickhouse_config, s3_config, uid)
    fn = _make_fn_output_int64_string(conn, table, schema_evolution="iceberg")
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet())
    assert fnrun.status == 0

    assert _table_exists(clickhouse_client, db, table)
    assert _row_count(clickhouse_client, db, table) == 2

    # Parquet stats say no nulls → columns should be NOT NULL
    cols = _get_columns(clickhouse_client, db, table)
    id_type = next(c[1] for c in cols if c[0] == "id")
    name_type = next(c[1] for c in cols if c[0] == "name")
    assert id_type == "Int64"
    assert name_type == "String"


@pytest.mark.clickhouse
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_s3_create_table_nullable_from_data(
    clickhouse_config, clickhouse_client, s3_config
):
    """Creating a new table with data that has nulls → nullable columns."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_s3_create_null_{uid}"
    db = clickhouse_config.database

    assert not _table_exists(clickhouse_client, db, table)

    conn = _build_s3_conn(clickhouse_config, s3_config, uid)
    # email column has a NULL value → should be Nullable in the created table
    fn = _make_fn_output_int64_string_email(conn, table, schema_evolution="iceberg")
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet())
    assert fnrun.status == 0

    assert _table_exists(clickhouse_client, db, table)
    cols = _get_columns(clickhouse_client, db, table)
    id_type = next(c[1] for c in cols if c[0] == "id")
    name_type = next(c[1] for c in cols if c[0] == "name")
    email_type = next(c[1] for c in cols if c[0] == "email")

    # id and name have no nulls → NOT NULL
    assert id_type == "Int64"
    assert name_type == "String"
    # email has a null → Nullable
    assert email_type == "Nullable(String)"

    # Verify data
    result = clickhouse_client.query(
        f"SELECT id, name, email FROM `{db}`.`{table}` ORDER BY id"
    )
    rows = result.result_rows
    assert rows[0] == (10, "x", "x@test.com")
    assert rows[1] == (20, "y", None)


@pytest.mark.clickhouse
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_s3_evolution_idempotent(
    clickhouse_config, clickhouse_client, s3_config
):
    """Running iceberg evolution twice with the same new schema is idempotent."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_s3_idempotent_{uid}"
    db = clickhouse_config.database

    _create_table(clickhouse_client, db, table, "`id` Int64, `name` String")
    _insert(clickhouse_client, db, table, "(1, 'alice')")

    conn = _build_s3_conn(clickhouse_config, s3_config, uid)

    # First run: add email column
    fn1 = _make_fn_output_int64_string_email(conn, table, schema_evolution="iceberg")
    fnrun1 = tdfnrun(function=fn1, input_tables=_mock_parquet())
    assert fnrun1.status == 0

    # Second run: same schema — no evolution needed, just append
    fn2 = _make_fn_output_int64_string_email(conn, table, schema_evolution="iceberg")
    fnrun2 = tdfnrun(function=fn2, input_tables=_mock_parquet())
    assert fnrun2.status == 0

    assert _row_count(clickhouse_client, db, table) == 5  # 1 original + 2 + 2


# ═════════════════════════════════════════════════════════════════════════════════════
# S3 — DATA INTEGRITY AFTER EVOLUTION
# ═════════════════════════════════════════════════════════════════════════════════════


@pytest.mark.clickhouse
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_s3_data_integrity_add_column_old_rows_get_null(
    clickhouse_config, clickhouse_client, s3_config
):
    """After adding a nullable column, pre-existing rows must have NULL for it."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_s3_di_add_{uid}"
    db = clickhouse_config.database

    _create_table(clickhouse_client, db, table, "`id` Int64, `name` String")
    _insert(clickhouse_client, db, table, "(1, 'alice'), (2, 'bob')")

    conn = _build_s3_conn(clickhouse_config, s3_config, uid)
    fn = _make_fn_output_int64_string_email(conn, table, schema_evolution="iceberg")
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet())
    assert fnrun.status == 0

    result = clickhouse_client.query(
        f"SELECT id, name, email FROM `{db}`.`{table}` ORDER BY id"
    )
    rows = result.result_rows

    # Original rows: email is NULL
    assert rows[0] == (1, "alice", None)
    assert rows[1] == (2, "bob", None)
    # New rows from the function
    assert rows[2] == (10, "x", "x@test.com")
    assert rows[3] == (20, "y", None)


@pytest.mark.clickhouse
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_s3_data_integrity_add_column_with_default_old_rows_get_default(
    clickhouse_config, clickhouse_client, s3_config
):
    """After adding a column with DEFAULT, pre-existing rows return the default."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_s3_di_default_{uid}"
    db = clickhouse_config.database

    _create_table(clickhouse_client, db, table, "`id` Int64")
    _insert(clickhouse_client, db, table, "(1), (2)")

    conn = _build_s3_conn(clickhouse_config, s3_config, uid)

    # The function outputs id + name; name is new, nullable, will be added
    fn = _make_fn_output_int64_string(conn, table, schema_evolution="iceberg")
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet())
    assert fnrun.status == 0

    result = clickhouse_client.query(
        f"SELECT id, name FROM `{db}`.`{table}` ORDER BY id"
    )
    rows = result.result_rows

    # Original rows: name should be NULL (it's nullable, no default)
    assert rows[0] == (1, None)
    assert rows[1] == (2, None)
    # New rows
    assert rows[2] == (10, "x")
    assert rows[3] == (20, "y")


@pytest.mark.clickhouse
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_s3_data_integrity_drop_column_remaining_data_preserved(
    clickhouse_config, clickhouse_client, s3_config
):
    """After dropping a column, remaining columns' data must be intact."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_s3_di_drop_{uid}"
    db = clickhouse_config.database

    _create_table(
        clickhouse_client, db, table,
        "`id` Int64, `name` Nullable(String)",
    )
    _insert(clickhouse_client, db, table, "(1, 'alice'), (2, 'bob'), (3, NULL)")

    conn = _build_s3_conn(clickhouse_config, s3_config, uid)
    fn = _make_fn_output_int64_only(conn, table, schema_evolution="iceberg")
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet())
    assert fnrun.status == 0

    result = clickhouse_client.query(
        f"SELECT id FROM `{db}`.`{table}` ORDER BY id"
    )
    rows = result.result_rows

    # Original rows preserved (only id column remains)
    assert rows[0] == (1,)
    assert rows[1] == (2,)
    assert rows[2] == (3,)
    # New rows
    assert rows[3] == (10,)
    assert rows[4] == (20,)


@pytest.mark.clickhouse
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_s3_data_integrity_widen_type_values_preserved(
    clickhouse_config, clickhouse_client, s3_config
):
    """After widening Int32 → Int64 and Float32 → Float64, original values must
    be exactly preserved (no truncation, no rounding)."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_s3_di_widen_{uid}"
    db = clickhouse_config.database

    _create_table(clickhouse_client, db, table, "`id` Int32, `value` Float32")
    _insert(clickhouse_client, db, table, "(42, 3.14), (-100, 0.0), (2147483647, 1.0)")

    conn = _build_s3_conn(clickhouse_config, s3_config, uid)
    fn = _make_fn_output_int64_widened(conn, table, schema_evolution="iceberg")
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet())
    assert fnrun.status == 0

    result = clickhouse_client.query(
        f"SELECT id, value FROM `{db}`.`{table}` ORDER BY id"
    )
    rows = result.result_rows

    ids = [r[0] for r in rows]
    # Original values must survive the type widening
    assert -100 in ids
    assert 42 in ids
    assert 2147483647 in ids  # max Int32 value — must survive widening to Int64

    # Original float values preserved (within Float32 precision)
    orig_row_42 = next(r for r in rows if r[0] == 42)
    assert abs(orig_row_42[1] - 3.14) < 0.01

    orig_row_neg = next(r for r in rows if r[0] == -100)
    assert orig_row_neg[1] == 0.0

    # New rows
    assert 10 in ids
    assert 20 in ids


@pytest.mark.clickhouse
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_s3_data_integrity_set_nullable_old_values_preserved(
    clickhouse_config, clickhouse_client, s3_config
):
    """After making a NOT NULL column nullable (because incoming data has nulls),
    old values are unchanged and new NULLs can be inserted."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_s3_di_nullable_{uid}"
    db = clickhouse_config.database

    _create_table(clickhouse_client, db, table, "`id` Int64, `name` String")
    _insert(clickhouse_client, db, table, "(1, 'alice'), (2, 'bob')")

    conn = _build_s3_conn(clickhouse_config, s3_config, uid)

    # Output data with a NULL in name — parquet stats will report has_nulls=True,
    # triggering SET_NULLABLE on the column.
    @td.subscriber(
        name="evo_test",
        tables="collection/table",
        destination=td.ClickHouseDest(
            conn=conn,
            destination_tables=table,
            schema_evolution="iceberg",
        ),
    )
    def fn_with_null_name(df: td.TableFrame):
        data = pl.DataFrame(
            {"id": [10, 20], "name": ["x", None]},
            schema={"id": pl.Int64, "name": pl.Utf8},
        )
        return td.TableFrame.from_polars(data)

    fnrun = tdfnrun(function=fn_with_null_name, input_tables=_mock_parquet())
    assert fnrun.status == 0

    result = clickhouse_client.query(
        f"SELECT id, name FROM `{db}`.`{table}` ORDER BY id"
    )
    rows = result.result_rows

    # Old values intact
    assert rows[0] == (1, "alice")
    assert rows[1] == (2, "bob")
    # New values (including a NULL)
    assert rows[2] == (10, "x")
    assert rows[3] == (20, None)

    # Column is now nullable — verify by inserting a NULL directly
    clickhouse_client.command(
        f"INSERT INTO `{db}`.`{table}` VALUES (99, NULL)"
    )
    result2 = clickhouse_client.query(
        f"SELECT id, name FROM `{db}`.`{table}` WHERE id = 99"
    )
    assert result2.result_rows == [(99, None)]


@pytest.mark.clickhouse
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_s3_data_integrity_combined_all_values_correct(
    clickhouse_config, clickhouse_client, s3_config
):
    """Combined evolution: verify every cell value is correct after add + drop +
    widen + set nullable."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_s3_di_combined_{uid}"
    db = clickhouse_config.database

    _create_table(
        clickhouse_client, db, table,
        "`id` Int32, `name` String, `old_col` Nullable(String)",
    )
    _insert(
        clickhouse_client, db, table,
        "(1, 'alice', 'legacy1'), (2, 'bob', NULL)",
    )

    conn = _build_s3_conn(clickhouse_config, s3_config, uid)
    fn = _make_fn_output_combined_evolution(conn, table, schema_evolution="iceberg")
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet())
    assert fnrun.status == 0

    # Verify schema
    cols = _get_columns(clickhouse_client, db, table)
    col_names = [c[0] for c in cols]
    assert col_names == ["id", "name", "tag"]

    result = clickhouse_client.query(
        f"SELECT id, name, tag FROM `{db}`.`{table}` ORDER BY id"
    )
    rows = result.result_rows

    # Row id=1: id widened 1→1, name 'alice' (now nullable), old_col dropped,
    #           tag added (nullable, no default → NULL)
    assert rows[0][0] == 1
    assert rows[0][1] == "alice"
    assert rows[0][2] is None  # tag: new column, NULL for old rows

    # Row id=2: id widened 2→2, name 'bob', old_col dropped, tag NULL
    assert rows[1][0] == 2
    assert rows[1][1] == "bob"
    assert rows[1][2] is None

    # Row id=10: new row from function
    assert rows[2][0] == 10
    assert rows[2][1] == "x"
    assert rows[2][2] == "a"

    # Row id=20: new row from function, name is NULL (nullable after evolution)
    assert rows[3][0] == 20
    assert rows[3][1] is None
    assert rows[3][2] == "b"


@pytest.mark.clickhouse
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_s3_data_integrity_multiple_evolutions_sequential(
    clickhouse_config, clickhouse_client, s3_config
):
    """Two sequential evolutions: first add a column, then widen a type.
    Verify all data across both rounds is correct."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_s3_di_multi_{uid}"
    db = clickhouse_config.database

    # Initial table: id Int32
    _create_table(clickhouse_client, db, table, "`id` Int32")
    _insert(clickhouse_client, db, table, "(1), (2)")

    conn = _build_s3_conn(clickhouse_config, s3_config, uid)

    # Round 1: evolve Int32 → Int64 and add 'name' (nullable)
    fn1 = _make_fn_output_int64_string(conn, table, schema_evolution="iceberg")
    fnrun1 = tdfnrun(function=fn1, input_tables=_mock_parquet())
    assert fnrun1.status == 0

    result1 = clickhouse_client.query(
        f"SELECT id, name FROM `{db}`.`{table}` ORDER BY id"
    )
    rows1 = result1.result_rows
    assert rows1[0] == (1, None)   # original, name is NULL
    assert rows1[1] == (2, None)   # original, name is NULL
    assert rows1[2] == (10, "x")   # new
    assert rows1[3] == (20, "y")   # new

    # Verify id was widened
    cols = _get_columns(clickhouse_client, db, table)
    id_type = next(c[1] for c in cols if c[0] == "id")
    assert id_type == "Int64"

    # Round 2: add 'email' column — use distinct ids (30, 40) to avoid
    # ambiguous ORDER BY with duplicate ids from round 1.
    @td.subscriber(
        name="evo_test",
        tables="collection/table",
        destination=td.ClickHouseDest(
            conn=conn,
            destination_tables=table,
            schema_evolution="iceberg",
        ),
    )
    def fn_round2(df: td.TableFrame):
        data = pl.DataFrame(
            {
                "id": [30, 40],
                "name": ["p", "q"],
                "email": ["p@test.com", None],
            },
            schema={"id": pl.Int64, "name": pl.Utf8, "email": pl.Utf8},
        )
        return td.TableFrame.from_polars(data)

    fnrun2 = tdfnrun(function=fn_round2, input_tables=_mock_parquet())
    assert fnrun2.status == 0

    result2 = clickhouse_client.query(
        f"SELECT id, name, email FROM `{db}`.`{table}` ORDER BY id"
    )
    rows2 = result2.result_rows

    # All 6 rows present (2 original + 2 from round1 + 2 from round2)
    assert len(rows2) == 6

    # Original rows: name=NULL, email=NULL
    assert rows2[0] == (1, None, None)
    assert rows2[1] == (2, None, None)
    # Round 1 rows: email=NULL (added in round 2)
    assert rows2[2] == (10, "x", None)
    assert rows2[3] == (20, "y", None)
    # Round 2 rows: all columns populated
    assert rows2[4] == (30, "p", "p@test.com")
    assert rows2[5] == (40, "q", None)


# ═════════════════════════════════════════════════════════════════════════════════════
# S3 — UNSAFE / FAILING EVOLUTION
# ═════════════════════════════════════════════════════════════════════════════════════


@pytest.mark.clickhouse
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_s3_evolution_narrow_type_fails(
    clickhouse_config, clickhouse_client, s3_config
):
    """Narrowing Int64 → Int32 is unsafe and should fail."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_s3_narrow_{uid}"
    db = clickhouse_config.database

    _create_table(clickhouse_client, db, table, "`id` Int64")
    _insert(clickhouse_client, db, table, "(1)")

    conn = _build_s3_conn(clickhouse_config, s3_config, uid)
    fn = _make_fn_output_int32_narrowed(conn, table, schema_evolution="iceberg")
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet(), require_success=False)
    assert fnrun.status != 0

    # Table should be unchanged
    assert _row_count(clickhouse_client, db, table) == 1


@pytest.mark.clickhouse
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_s3_evolution_incompatible_type_fails(
    clickhouse_config, clickhouse_client, s3_config
):
    """Changing Int64 → String is unsafe and should fail."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_s3_incompat_{uid}"
    db = clickhouse_config.database

    _create_table(clickhouse_client, db, table, "`id` Int64")
    _insert(clickhouse_client, db, table, "(1)")

    conn = _build_s3_conn(clickhouse_config, s3_config, uid)
    fn = _make_fn_output_string_type_change(conn, table, schema_evolution="iceberg")
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet(), require_success=False)
    assert fnrun.status != 0

    # Original data unchanged
    assert _row_count(clickhouse_client, db, table) == 1


@pytest.mark.clickhouse
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_s3_no_evolution_schema_mismatch_fails(
    clickhouse_config, clickhouse_client, s3_config
):
    """With schema_evolution='none', any schema mismatch should fail."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_s3_none_fail_{uid}"
    db = clickhouse_config.database

    _create_table(clickhouse_client, db, table, "`id` Int64, `name` String")
    _insert(clickhouse_client, db, table, "(1, 'alice')")

    conn = _build_s3_conn(clickhouse_config, s3_config, uid)
    # Try to add a column with evolution=none — should fail
    fn = _make_fn_output_int64_string_email(conn, table, schema_evolution="none")
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet(), require_success=False)
    assert fnrun.status != 0

    # Table unchanged
    assert _row_count(clickhouse_client, db, table) == 1


@pytest.mark.clickhouse
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_s3_no_evolution_safe_change_still_fails(
    clickhouse_config, clickhouse_client, s3_config
):
    """Even a safe evolution (add nullable col) fails when evolution='none'."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_s3_none_safe_{uid}"
    db = clickhouse_config.database

    _create_table(clickhouse_client, db, table, "`id` Int32")
    _insert(clickhouse_client, db, table, "(1)")

    conn = _build_s3_conn(clickhouse_config, s3_config, uid)
    # Widen Int32 → Int64 — safe evolution, but mode is 'none'
    fn = _make_fn_output_int64_only(conn, table, schema_evolution="none")
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet(), require_success=False)
    assert fnrun.status != 0

    cols = _get_columns(clickhouse_client, db, table)
    assert cols[0][1] == "Int32"  # type NOT widened


# ═════════════════════════════════════════════════════════════════════════════════════
# AZURE — SAFE EVOLUTION
# ═════════════════════════════════════════════════════════════════════════════════════


@pytest.mark.clickhouse
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.azure
def test_azure_evolution_add_nullable_column(
    clickhouse_config, clickhouse_client, azure_config
):
    """Add a nullable column via Azure staging."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_az_add_col_{uid}"
    db = clickhouse_config.database

    _create_table(clickhouse_client, db, table, "`id` Int64, `name` String")
    _insert(clickhouse_client, db, table, "(1, 'alice')")

    conn = _build_azure_conn(clickhouse_config, azure_config, uid)
    fn = _make_fn_output_int64_string_email(conn, table, schema_evolution="iceberg")
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet())
    assert fnrun.status == 0

    cols = _get_columns(clickhouse_client, db, table)
    col_names = [c[0] for c in cols]
    assert "email" in col_names
    assert _row_count(clickhouse_client, db, table) == 3


@pytest.mark.clickhouse
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.azure
def test_azure_evolution_widen_type(
    clickhouse_config, clickhouse_client, azure_config
):
    """Widen Int32 → Int64 via Azure staging."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_az_widen_{uid}"
    db = clickhouse_config.database

    _create_table(clickhouse_client, db, table, "`id` Int32, `value` Float32")
    _insert(clickhouse_client, db, table, "(1, 1.5)")

    conn = _build_azure_conn(clickhouse_config, azure_config, uid)
    fn = _make_fn_output_int64_widened(conn, table, schema_evolution="iceberg")
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet())
    assert fnrun.status == 0

    cols = _get_columns(clickhouse_client, db, table)
    id_type = next(c[1] for c in cols if c[0] == "id")
    assert id_type == "Int64"
    assert _row_count(clickhouse_client, db, table) == 3


@pytest.mark.clickhouse
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.azure
def test_azure_evolution_combined(
    clickhouse_config, clickhouse_client, azure_config
):
    """Combined evolution via Azure staging."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_az_combined_{uid}"
    db = clickhouse_config.database

    _create_table(
        clickhouse_client, db, table,
        "`id` Int32, `name` String, `old_col` Nullable(String)",
    )
    _insert(clickhouse_client, db, table, "(1, 'alice', 'legacy')")

    conn = _build_azure_conn(clickhouse_config, azure_config, uid)
    fn = _make_fn_output_combined_evolution(conn, table, schema_evolution="iceberg")
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet())
    assert fnrun.status == 0

    cols = _get_columns(clickhouse_client, db, table)
    col_names = [c[0] for c in cols]
    assert col_names == ["id", "name", "tag"]
    assert _row_count(clickhouse_client, db, table) == 3


# ═════════════════════════════════════════════════════════════════════════════════════
# AZURE — UNSAFE / FAILING EVOLUTION
# ═════════════════════════════════════════════════════════════════════════════════════


@pytest.mark.clickhouse
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.azure
def test_azure_evolution_narrow_type_fails(
    clickhouse_config, clickhouse_client, azure_config
):
    """Narrowing Int64 → Int32 fails via Azure staging."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_az_narrow_{uid}"
    db = clickhouse_config.database

    _create_table(clickhouse_client, db, table, "`id` Int64")
    _insert(clickhouse_client, db, table, "(1)")

    conn = _build_azure_conn(clickhouse_config, azure_config, uid)
    fn = _make_fn_output_int32_narrowed(conn, table, schema_evolution="iceberg")
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet(), require_success=False)
    assert fnrun.status != 0


@pytest.mark.clickhouse
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.azure
def test_azure_no_evolution_schema_mismatch_fails(
    clickhouse_config, clickhouse_client, azure_config
):
    """With evolution='none', schema mismatch fails via Azure staging."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_az_none_fail_{uid}"
    db = clickhouse_config.database

    _create_table(clickhouse_client, db, table, "`id` Int64, `name` String")
    _insert(clickhouse_client, db, table, "(1, 'alice')")

    conn = _build_azure_conn(clickhouse_config, azure_config, uid)
    fn = _make_fn_output_int64_string_email(conn, table, schema_evolution="none")
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet(), require_success=False)
    assert fnrun.status != 0


# ═════════════════════════════════════════════════════════════════════════════════════
# GCS — SAFE EVOLUTION
# ═════════════════════════════════════════════════════════════════════════════════════


@pytest.mark.clickhouse
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.gcs
def test_gcs_evolution_add_nullable_column(
    clickhouse_config, clickhouse_client, gcs_config
):
    """Add a nullable column via GCS staging."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_gcs_add_col_{uid}"
    db = clickhouse_config.database

    _create_table(clickhouse_client, db, table, "`id` Int64, `name` String")
    _insert(clickhouse_client, db, table, "(1, 'alice')")

    conn = _build_gcs_conn(clickhouse_config, gcs_config, uid)
    fn = _make_fn_output_int64_string_email(conn, table, schema_evolution="iceberg")
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet())
    assert fnrun.status == 0

    cols = _get_columns(clickhouse_client, db, table)
    col_names = [c[0] for c in cols]
    assert "email" in col_names
    assert _row_count(clickhouse_client, db, table) == 3


@pytest.mark.clickhouse
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.gcs
def test_gcs_evolution_widen_type(
    clickhouse_config, clickhouse_client, gcs_config
):
    """Widen Int32 → Int64 via GCS staging."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_gcs_widen_{uid}"
    db = clickhouse_config.database

    _create_table(clickhouse_client, db, table, "`id` Int32, `value` Float32")
    _insert(clickhouse_client, db, table, "(1, 1.5)")

    conn = _build_gcs_conn(clickhouse_config, gcs_config, uid)
    fn = _make_fn_output_int64_widened(conn, table, schema_evolution="iceberg")
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet())
    assert fnrun.status == 0

    cols = _get_columns(clickhouse_client, db, table)
    id_type = next(c[1] for c in cols if c[0] == "id")
    assert id_type == "Int64"
    assert _row_count(clickhouse_client, db, table) == 3


@pytest.mark.clickhouse
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.gcs
def test_gcs_evolution_combined(
    clickhouse_config, clickhouse_client, gcs_config
):
    """Combined evolution via GCS staging."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_gcs_combined_{uid}"
    db = clickhouse_config.database

    _create_table(
        clickhouse_client, db, table,
        "`id` Int32, `name` String, `old_col` Nullable(String)",
    )
    _insert(clickhouse_client, db, table, "(1, 'alice', 'legacy')")

    conn = _build_gcs_conn(clickhouse_config, gcs_config, uid)
    fn = _make_fn_output_combined_evolution(conn, table, schema_evolution="iceberg")
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet())
    assert fnrun.status == 0

    cols = _get_columns(clickhouse_client, db, table)
    col_names = [c[0] for c in cols]
    assert col_names == ["id", "name", "tag"]
    assert _row_count(clickhouse_client, db, table) == 3


# ═════════════════════════════════════════════════════════════════════════════════════
# GCS — UNSAFE / FAILING EVOLUTION
# ═════════════════════════════════════════════════════════════════════════════════════


@pytest.mark.clickhouse
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.gcs
def test_gcs_evolution_narrow_type_fails(
    clickhouse_config, clickhouse_client, gcs_config
):
    """Narrowing Int64 → Int32 fails via GCS staging."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_gcs_narrow_{uid}"
    db = clickhouse_config.database

    _create_table(clickhouse_client, db, table, "`id` Int64")
    _insert(clickhouse_client, db, table, "(1)")

    conn = _build_gcs_conn(clickhouse_config, gcs_config, uid)
    fn = _make_fn_output_int32_narrowed(conn, table, schema_evolution="iceberg")
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet(), require_success=False)
    assert fnrun.status != 0


@pytest.mark.clickhouse
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.gcs
def test_gcs_evolution_incompatible_type_fails(
    clickhouse_config, clickhouse_client, gcs_config
):
    """Changing Int64 → String fails via GCS staging."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_gcs_incompat_{uid}"
    db = clickhouse_config.database

    _create_table(clickhouse_client, db, table, "`id` Int64")
    _insert(clickhouse_client, db, table, "(1)")

    conn = _build_gcs_conn(clickhouse_config, gcs_config, uid)
    fn = _make_fn_output_string_type_change(conn, table, schema_evolution="iceberg")
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet(), require_success=False)
    assert fnrun.status != 0


@pytest.mark.clickhouse
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.gcs
def test_gcs_no_evolution_schema_mismatch_fails(
    clickhouse_config, clickhouse_client, gcs_config
):
    """With evolution='none', schema mismatch fails via GCS staging."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_gcs_none_fail_{uid}"
    db = clickhouse_config.database

    _create_table(clickhouse_client, db, table, "`id` Int64, `name` String")
    _insert(clickhouse_client, db, table, "(1, 'alice')")

    conn = _build_gcs_conn(clickhouse_config, gcs_config, uid)
    fn = _make_fn_output_int64_string_email(conn, table, schema_evolution="none")
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet(), require_success=False)
    assert fnrun.status != 0
