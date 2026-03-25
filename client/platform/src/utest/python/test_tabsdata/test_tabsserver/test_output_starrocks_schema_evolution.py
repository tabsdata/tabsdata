#
# Copyright 2026 Tabs Data Inc.
#

"""Integration tests for Iceberg-style schema evolution through the full
StarRocksDest -> CloudStagedBulkLoader -> StarRocksDialect pipeline.

Tests exercise all three cloud storage backends (S3, Azure, GCS) and cover
both safe evolution scenarios (that should succeed) and unsafe/impossible
scenarios (that should fail with appropriate errors).
"""

import logging
import os
import re
import uuid

import polars as pl
import pytest

import tabsdata as td

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# -- Helpers --------------------------------------------------------------------------


def _uid():
    return uuid.uuid4().hex[:12]


def _normalize_type(col_type):
    """Normalize StarRocks information_schema types for comparison.

    StarRocks stores types like ``bigint(20)``, ``int(11)``, ``varchar(65533)``
    in *information_schema.COLUMNS*.  This helper strips the width suffix and
    maps common aliases so that assertions can use simple canonical names such
    as ``BIGINT``, ``INT``, ``STRING``, ``DOUBLE``, ``FLOAT``.
    """
    t = col_type.upper()
    # bigint(20) -> BIGINT, int(11) -> INT, etc.
    t = re.sub(r"INT\(\d+\)", "INT", t)
    if t == "VARCHAR(65533)":
        t = "STRING"
    return t


def _sr_create_table(starrocks_client, db, table, col_defs_sql):
    """Create a StarRocks table with DUPLICATE KEY and fast_schema_evolution."""
    first_col = col_defs_sql.strip().split("`")[1]
    with starrocks_client.cursor() as cur:
        cur.execute(
            f"CREATE TABLE `{db}`.`{table}` ({col_defs_sql}) "
            f"DUPLICATE KEY(`{first_col}`) "
            f"DISTRIBUTED BY HASH(`{first_col}`) "
            f'PROPERTIES ("fast_schema_evolution" = "true")'
        )


def _sr_insert(starrocks_client, db, table, values_sql):
    """INSERT rows into a StarRocks table."""
    with starrocks_client.cursor() as cur:
        cur.execute(f"INSERT INTO `{db}`.`{table}` VALUES {values_sql}")


def _table_exists(starrocks_client, db, table):
    """Check whether a table exists in StarRocks."""
    with starrocks_client.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
            (db, table),
        )
        return cur.fetchone()[0] > 0


def _get_columns(starrocks_client, db, table):
    """Return list of (COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE) tuples."""
    with starrocks_client.cursor() as cur:
        cur.execute(
            "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE "
            "FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s "
            "ORDER BY ORDINAL_POSITION",
            (db, table),
        )
        return cur.fetchall()


def _row_count(starrocks_client, db, table):
    """Return the number of rows in the table."""
    with starrocks_client.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM `{db}`.`{table}`")
        return cur.fetchone()[0]


def _query_all(starrocks_client, db, table, order_by="1"):
    """SELECT * from the table, ordered by the given expression."""
    with starrocks_client.cursor() as cur:
        cur.execute(f"SELECT * FROM `{db}`.`{table}` ORDER BY {order_by}")
        return cur.fetchall()


# -- Connection builders --------------------------------------------------------------


def _build_s3_conn(starrocks_config, s3_config, uid):
    staging = td.S3Config(
        bucket=s3_config["BUCKET_NAME"],
        region=s3_config["REGION"],
        base_path=f"tabsdata-testing/sr-evo-{uid}",
        credentials=s3_config["CREDENTIALS"],
    )
    return td.StarRocksConn(
        host=starrocks_config.host,
        port=starrocks_config.port,
        database=starrocks_config.database,
        credentials=starrocks_config.credentials,
        staging=staging,
    )


def _build_azure_conn(starrocks_config, azure_config, uid):
    staging = td.AzureConfig(
        container=azure_config["CONTAINER_NAME"],
        base_path=f"tabsdata-testing/sr-evo-{uid}",
        credentials=azure_config["CREDENTIALS"],
    )
    return td.StarRocksConn(
        host=starrocks_config.host,
        port=starrocks_config.port,
        database=starrocks_config.database,
        credentials=starrocks_config.credentials,
        staging=staging,
    )


def _build_gcs_conn(starrocks_config, gcs_config, uid):
    staging = td.GCSConfig(
        bucket=gcs_config["BUCKET"],
        base_path=f"tabsdata-testing/sr-evo-{uid}",
        upload_credentials=td.GCPServiceAccountKeyCredentials(
            td.EnvironmentSecret(gcs_config["ENV"])
        ),
        hmac_credentials=td.HMACCredentials(
            gcs_config["ACCESS_KEY"], gcs_config["SECRET_KEY"]
        ),
    )
    return td.StarRocksConn(
        host=starrocks_config.host,
        port=starrocks_config.port,
        database=starrocks_config.database,
        credentials=starrocks_config.credentials,
        staging=staging,
    )


# -- Function builders ----------------------------------------------------------------
# These create @td.subscriber functions that output specific schemas.


def _make_fn_output_bigint_string(conn, table, schema_evolution="none"):
    """Output: id(Int64) -> BIGINT, name(Utf8) -> STRING."""

    @td.subscriber(
        name="evo_test",
        tables="collection/table",
        destination=td.StarRocksDest(
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


def _make_fn_output_bigint_string_email(conn, table, schema_evolution="none"):
    """Output: id(Int64) -> BIGINT, name(Utf8) -> STRING, email(Utf8 nullable) -> STRING."""

    @td.subscriber(
        name="evo_test",
        tables="collection/table",
        destination=td.StarRocksDest(
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


def _make_fn_output_bigint_only(conn, table, schema_evolution="none"):
    """Output: id(Int64) -> BIGINT -- drops the 'name' column."""

    @td.subscriber(
        name="evo_test",
        tables="collection/table",
        destination=td.StarRocksDest(
            conn=conn,
            destination_tables=table,
            schema_evolution=schema_evolution,
        ),
    )
    def fn(df: td.TableFrame):
        data = pl.DataFrame({"id": [10, 20]}, schema={"id": pl.Int64})
        return td.TableFrame.from_polars(data)

    return fn


def _make_fn_output_bigint_widened(conn, table, schema_evolution="none"):
    """Output: key_col(Int64), id(Int64) -> BIGINT, value(Float64) -> DOUBLE -- widened from INT/FLOAT."""

    @td.subscriber(
        name="evo_test",
        tables="collection/table",
        destination=td.StarRocksDest(
            conn=conn,
            destination_tables=table,
            schema_evolution=schema_evolution,
        ),
    )
    def fn(df: td.TableFrame):
        data = pl.DataFrame(
            {"key_col": [100, 200], "id": [10, 20], "value": [1.5, 2.5]},
            schema={"key_col": pl.Int64, "id": pl.Int64, "value": pl.Float64},
        )
        return td.TableFrame.from_polars(data)

    return fn


def _make_fn_output_int_narrowed(conn, table, schema_evolution="none"):
    """Output: id(Int32) -> INT -- narrowed from BIGINT (unsafe)."""

    @td.subscriber(
        name="evo_test",
        tables="collection/table",
        destination=td.StarRocksDest(
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
    """Output: id(Utf8) -> STRING -- incompatible type change from BIGINT (unsafe)."""

    @td.subscriber(
        name="evo_test",
        tables="collection/table",
        destination=td.StarRocksDest(
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
    """Output: key_col(Int64), id(Int64) -> BIGINT, name(Utf8 with None) -> STRING NULL, tag(Utf8) -> STRING.

    Compared to initial schema (key_col BIGINT, id INT, name STRING NOT NULL, old_col STRING):
    - key_col: BIGINT (stable key, no change)
    - id: INT -> BIGINT (widen)
    - name: NOT NULL -> nullable (set nullable)
    - old_col: dropped
    - tag: added (nullable)
    """

    @td.subscriber(
        name="evo_test",
        tables="collection/table",
        destination=td.StarRocksDest(
            conn=conn,
            destination_tables=table,
            schema_evolution=schema_evolution,
        ),
    )
    def fn(df: td.TableFrame):
        data = pl.DataFrame(
            {
                "key_col": [100, 200],
                "id": [10, 20],
                "name": ["x", None],
                "tag": ["a", "b"],
            },
            schema={"key_col": pl.Int64, "id": pl.Int64, "name": pl.Utf8, "tag": pl.Utf8},
        )
        return td.TableFrame.from_polars(data)

    return fn


# -- Mock input parquet ---------------------------------------------------------------

# All test functions ignore their input and produce their own output,
# but tdfnrun requires an input table.  We reuse the existing mock.


def _mock_parquet():
    from test_tabsdata.conftest import TESTING_RESOURCES_FOLDER

    return os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_starrocks_list",
        "mock_table.parquet",
    )


# =====================================================================================
# S3 -- SAFE EVOLUTION
# =====================================================================================


@pytest.mark.starrocks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_s3_evolution_add_nullable_column(
    starrocks_config, starrocks_client, s3_config
):
    """Add a nullable column to an existing table via S3 staging."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_s3_add_col_{uid}"
    db = starrocks_config.database

    _sr_create_table(
        starrocks_client, db, table, "`id` BIGINT, `name` STRING"
    )
    _sr_insert(starrocks_client, db, table, "(1, 'alice'), (2, 'bob')")

    conn = _build_s3_conn(starrocks_config, s3_config, uid)
    fn = _make_fn_output_bigint_string_email(
        conn, table, schema_evolution="iceberg"
    )
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet())
    assert fnrun.status == 0

    cols = _get_columns(starrocks_client, db, table)
    col_names = [c[0] for c in cols]
    assert "email" in col_names

    assert _row_count(starrocks_client, db, table) == 4  # 2 old + 2 new


@pytest.mark.starrocks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_s3_evolution_drop_column(
    starrocks_config, starrocks_client, s3_config
):
    """Drop a column from an existing table via S3 staging."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_s3_drop_col_{uid}"
    db = starrocks_config.database

    _sr_create_table(
        starrocks_client, db, table, "`id` BIGINT, `name` STRING NULL"
    )
    _sr_insert(starrocks_client, db, table, "(1, 'alice')")

    conn = _build_s3_conn(starrocks_config, s3_config, uid)
    fn = _make_fn_output_bigint_only(conn, table, schema_evolution="iceberg")
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet())
    assert fnrun.status == 0

    cols = _get_columns(starrocks_client, db, table)
    col_names = [c[0] for c in cols]
    assert "name" not in col_names
    assert _row_count(starrocks_client, db, table) == 3  # 1 old + 2 new


@pytest.mark.starrocks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_s3_evolution_widen_type(
    starrocks_config, starrocks_client, s3_config
):
    """Widen INT -> BIGINT and FLOAT -> DOUBLE via S3 staging."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_s3_widen_{uid}"
    db = starrocks_config.database

    _sr_create_table(
        starrocks_client, db, table, "`key_col` BIGINT, `id` INT, `value` FLOAT"
    )
    _sr_insert(starrocks_client, db, table, "(1, 1, 1.5)")

    conn = _build_s3_conn(starrocks_config, s3_config, uid)
    fn = _make_fn_output_bigint_widened(
        conn, table, schema_evolution="iceberg"
    )
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet())
    assert fnrun.status == 0

    cols = _get_columns(starrocks_client, db, table)
    id_type = _normalize_type(next(c[1] for c in cols if c[0] == "id"))
    value_type = _normalize_type(next(c[1] for c in cols if c[0] == "value"))
    assert "BIGINT" in id_type
    assert "DOUBLE" in value_type
    assert _row_count(starrocks_client, db, table) == 3


@pytest.mark.starrocks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_s3_evolution_combined(
    starrocks_config, starrocks_client, s3_config
):
    """Combined evolution: add + drop + widen + set nullable via S3."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_s3_combined_{uid}"
    db = starrocks_config.database

    _sr_create_table(
        starrocks_client, db, table,
        "`key_col` BIGINT, `id` INT, `name` STRING NOT NULL, `old_col` STRING NULL",
    )
    _sr_insert(starrocks_client, db, table, "(1, 1, 'alice', 'legacy')")

    conn = _build_s3_conn(starrocks_config, s3_config, uid)
    fn = _make_fn_output_combined_evolution(
        conn, table, schema_evolution="iceberg"
    )
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet())
    assert fnrun.status == 0

    cols = _get_columns(starrocks_client, db, table)
    col_names = [c[0] for c in cols]
    assert col_names == ["key_col", "id", "name", "tag"]
    assert "old_col" not in col_names

    id_type = _normalize_type(next(c[1] for c in cols if c[0] == "id"))
    name_nullable = next(c[2] for c in cols if c[0] == "name")
    assert "BIGINT" in id_type
    assert name_nullable == "YES"

    assert _row_count(starrocks_client, db, table) == 3


@pytest.mark.starrocks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_s3_evolution_no_changes(
    starrocks_config, starrocks_client, s3_config
):
    """When schemas already match, iceberg mode should be a no-op."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_s3_noop_{uid}"
    db = starrocks_config.database

    _sr_create_table(
        starrocks_client, db, table, "`id` BIGINT, `name` STRING NULL"
    )
    _sr_insert(starrocks_client, db, table, "(1, 'alice')")

    conn = _build_s3_conn(starrocks_config, s3_config, uid)
    fn = _make_fn_output_bigint_string(
        conn, table, schema_evolution="iceberg"
    )
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet())
    assert fnrun.status == 0

    assert _row_count(starrocks_client, db, table) == 3


@pytest.mark.starrocks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_s3_evolution_create_new_table(
    starrocks_config, starrocks_client, s3_config
):
    """When the table doesn't exist yet, iceberg mode should create it normally."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_s3_create_{uid}"
    db = starrocks_config.database

    assert not _table_exists(starrocks_client, db, table)

    conn = _build_s3_conn(starrocks_config, s3_config, uid)
    fn = _make_fn_output_bigint_string(
        conn, table, schema_evolution="iceberg"
    )
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet())
    assert fnrun.status == 0

    assert _table_exists(starrocks_client, db, table)
    assert _row_count(starrocks_client, db, table) == 2

    # Parquet stats say no nulls -> columns should be NOT NULL
    cols = _get_columns(starrocks_client, db, table)
    id_nullable = next(c[2] for c in cols if c[0] == "id")
    name_nullable = next(c[2] for c in cols if c[0] == "name")
    assert id_nullable == "NO"
    assert name_nullable == "NO"


@pytest.mark.starrocks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_s3_create_table_nullable_from_data(
    starrocks_config, starrocks_client, s3_config
):
    """Creating a new table with data that has nulls -> nullable columns."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_s3_create_null_{uid}"
    db = starrocks_config.database

    assert not _table_exists(starrocks_client, db, table)

    conn = _build_s3_conn(starrocks_config, s3_config, uid)
    # email column has a NULL value -> should be nullable in the created table
    fn = _make_fn_output_bigint_string_email(
        conn, table, schema_evolution="iceberg"
    )
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet())
    assert fnrun.status == 0

    assert _table_exists(starrocks_client, db, table)
    cols = _get_columns(starrocks_client, db, table)
    id_nullable = next(c[2] for c in cols if c[0] == "id")
    name_nullable = next(c[2] for c in cols if c[0] == "name")
    email_nullable = next(c[2] for c in cols if c[0] == "email")

    # id and name have no nulls -> NOT NULL
    assert id_nullable == "NO"
    assert name_nullable == "NO"
    # email has a null -> Nullable
    assert email_nullable == "YES"

    # Verify data
    rows = _query_all(starrocks_client, db, table, order_by="`id`")
    assert rows[0][0] == 10
    assert rows[0][1] == "x"
    assert rows[0][2] == "x@test.com"
    assert rows[1][0] == 20
    assert rows[1][1] == "y"
    assert rows[1][2] is None


@pytest.mark.starrocks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_s3_evolution_idempotent(
    starrocks_config, starrocks_client, s3_config
):
    """Running iceberg evolution twice with the same new schema is idempotent."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_s3_idempotent_{uid}"
    db = starrocks_config.database

    _sr_create_table(
        starrocks_client, db, table, "`id` BIGINT, `name` STRING"
    )
    _sr_insert(starrocks_client, db, table, "(1, 'alice')")

    conn = _build_s3_conn(starrocks_config, s3_config, uid)

    # First run: add email column
    fn1 = _make_fn_output_bigint_string_email(
        conn, table, schema_evolution="iceberg"
    )
    fnrun1 = tdfnrun(function=fn1, input_tables=_mock_parquet())
    assert fnrun1.status == 0

    # Second run: same schema -- no evolution needed, just append
    fn2 = _make_fn_output_bigint_string_email(
        conn, table, schema_evolution="iceberg"
    )
    fnrun2 = tdfnrun(function=fn2, input_tables=_mock_parquet())
    assert fnrun2.status == 0

    assert _row_count(starrocks_client, db, table) == 5  # 1 original + 2 + 2


# =====================================================================================
# S3 -- DATA INTEGRITY AFTER EVOLUTION
# =====================================================================================


@pytest.mark.starrocks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_s3_data_integrity_add_column_old_rows_get_null(
    starrocks_config, starrocks_client, s3_config
):
    """After adding a nullable column, pre-existing rows must have NULL for it."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_s3_di_add_{uid}"
    db = starrocks_config.database

    _sr_create_table(
        starrocks_client, db, table, "`id` BIGINT, `name` STRING"
    )
    _sr_insert(starrocks_client, db, table, "(1, 'alice'), (2, 'bob')")

    conn = _build_s3_conn(starrocks_config, s3_config, uid)
    fn = _make_fn_output_bigint_string_email(
        conn, table, schema_evolution="iceberg"
    )
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet())
    assert fnrun.status == 0

    rows = _query_all(starrocks_client, db, table, order_by="`id`")

    # Original rows: email is NULL
    assert rows[0] == (1, "alice", None)
    assert rows[1] == (2, "bob", None)
    # New rows from the function
    assert rows[2] == (10, "x", "x@test.com")
    assert rows[3] == (20, "y", None)


@pytest.mark.starrocks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_s3_data_integrity_drop_column_remaining_data_preserved(
    starrocks_config, starrocks_client, s3_config
):
    """After dropping a column, remaining columns' data must be intact."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_s3_di_drop_{uid}"
    db = starrocks_config.database

    _sr_create_table(
        starrocks_client, db, table, "`id` BIGINT, `name` STRING NULL"
    )
    _sr_insert(
        starrocks_client, db, table, "(1, 'alice'), (2, 'bob'), (3, NULL)"
    )

    conn = _build_s3_conn(starrocks_config, s3_config, uid)
    fn = _make_fn_output_bigint_only(conn, table, schema_evolution="iceberg")
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet())
    assert fnrun.status == 0

    rows = _query_all(starrocks_client, db, table, order_by="`id`")

    # Original rows preserved (only id column remains)
    assert rows[0] == (1,)
    assert rows[1] == (2,)
    assert rows[2] == (3,)
    # New rows
    assert rows[3] == (10,)
    assert rows[4] == (20,)


@pytest.mark.starrocks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_s3_data_integrity_widen_type_values_preserved(
    starrocks_config, starrocks_client, s3_config
):
    """After widening INT -> BIGINT and FLOAT -> DOUBLE, original values must
    be exactly preserved (no truncation, no rounding)."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_s3_di_widen_{uid}"
    db = starrocks_config.database

    _sr_create_table(
        starrocks_client, db, table, "`key_col` BIGINT, `id` INT, `value` FLOAT"
    )
    _sr_insert(
        starrocks_client, db, table,
        "(1, 42, 3.14), (2, -100, 0.0), (3, 2147483647, 1.0)",
    )

    conn = _build_s3_conn(starrocks_config, s3_config, uid)
    fn = _make_fn_output_bigint_widened(
        conn, table, schema_evolution="iceberg"
    )
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet())
    assert fnrun.status == 0

    rows = _query_all(starrocks_client, db, table, order_by="`id`")
    ids = [r[1] for r in rows]

    # Original values must survive the type widening
    assert -100 in ids
    assert 42 in ids
    assert 2147483647 in ids  # max INT value -- must survive widening to BIGINT

    # Original float values preserved (within FLOAT precision)
    orig_row_42 = next(r for r in rows if r[1] == 42)
    assert abs(orig_row_42[2] - 3.14) < 0.01

    orig_row_neg = next(r for r in rows if r[1] == -100)
    assert orig_row_neg[2] == 0.0

    # New rows
    assert 10 in ids
    assert 20 in ids


@pytest.mark.starrocks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_s3_data_integrity_combined_all_values_correct(
    starrocks_config, starrocks_client, s3_config
):
    """Combined evolution: verify every cell value is correct after add + drop +
    widen + set nullable."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_s3_di_combined_{uid}"
    db = starrocks_config.database

    _sr_create_table(
        starrocks_client, db, table,
        "`key_col` BIGINT, `id` INT, `name` STRING NOT NULL, `old_col` STRING NULL",
    )
    _sr_insert(
        starrocks_client, db, table,
        "(1, 1, 'alice', 'legacy1'), (2, 2, 'bob', NULL)",
    )

    conn = _build_s3_conn(starrocks_config, s3_config, uid)
    fn = _make_fn_output_combined_evolution(
        conn, table, schema_evolution="iceberg"
    )
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet())
    assert fnrun.status == 0

    # Verify schema
    cols = _get_columns(starrocks_client, db, table)
    col_names = [c[0] for c in cols]
    assert col_names == ["key_col", "id", "name", "tag"]

    rows = _query_all(starrocks_client, db, table, order_by="`id`")

    # Row id=1: id widened 1->1, name 'alice' (now nullable), old_col dropped,
    #           tag added (nullable -> NULL for old rows)
    assert rows[0][1] == 1
    assert rows[0][2] == "alice"
    assert rows[0][3] is None  # tag: new column, NULL for old rows

    # Row id=2: id widened 2->2, name 'bob', old_col dropped, tag NULL
    assert rows[1][1] == 2
    assert rows[1][2] == "bob"
    assert rows[1][3] is None

    # Row id=10: new row from function
    assert rows[2][1] == 10
    assert rows[2][2] == "x"
    assert rows[2][3] == "a"

    # Row id=20: new row from function, name is NULL (nullable after evolution)
    assert rows[3][1] == 20
    assert rows[3][2] is None
    assert rows[3][3] == "b"


# =====================================================================================
# S3 -- UNSAFE / FAILING EVOLUTION
# =====================================================================================


@pytest.mark.starrocks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_s3_evolution_narrow_type_fails(
    starrocks_config, starrocks_client, s3_config
):
    """Narrowing BIGINT -> INT is unsafe and should fail."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_s3_narrow_{uid}"
    db = starrocks_config.database

    _sr_create_table(starrocks_client, db, table, "`id` BIGINT")
    _sr_insert(starrocks_client, db, table, "(1)")

    conn = _build_s3_conn(starrocks_config, s3_config, uid)
    fn = _make_fn_output_int_narrowed(
        conn, table, schema_evolution="iceberg"
    )
    fnrun = tdfnrun(
        function=fn, input_tables=_mock_parquet(), require_success=False
    )
    assert fnrun.status != 0

    # Table should be unchanged
    assert _row_count(starrocks_client, db, table) == 1


@pytest.mark.starrocks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_s3_evolution_incompatible_type_fails(
    starrocks_config, starrocks_client, s3_config
):
    """Changing BIGINT -> STRING is unsafe and should fail."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_s3_incompat_{uid}"
    db = starrocks_config.database

    _sr_create_table(starrocks_client, db, table, "`id` BIGINT")
    _sr_insert(starrocks_client, db, table, "(1)")

    conn = _build_s3_conn(starrocks_config, s3_config, uid)
    fn = _make_fn_output_string_type_change(
        conn, table, schema_evolution="iceberg"
    )
    fnrun = tdfnrun(
        function=fn, input_tables=_mock_parquet(), require_success=False
    )
    assert fnrun.status != 0

    # Original data unchanged
    assert _row_count(starrocks_client, db, table) == 1


@pytest.mark.starrocks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_s3_no_evolution_schema_mismatch_fails(
    starrocks_config, starrocks_client, s3_config
):
    """With schema_evolution='none', any schema mismatch should fail."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_s3_none_fail_{uid}"
    db = starrocks_config.database

    _sr_create_table(
        starrocks_client, db, table, "`id` BIGINT, `name` STRING"
    )
    _sr_insert(starrocks_client, db, table, "(1, 'alice')")

    conn = _build_s3_conn(starrocks_config, s3_config, uid)
    # Try to add a column with evolution=none -- should fail
    fn = _make_fn_output_bigint_string_email(
        conn, table, schema_evolution="none"
    )
    fnrun = tdfnrun(
        function=fn, input_tables=_mock_parquet(), require_success=False
    )
    assert fnrun.status != 0

    # Table unchanged
    assert _row_count(starrocks_client, db, table) == 1


@pytest.mark.starrocks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_s3_no_evolution_safe_change_still_fails(
    starrocks_config, starrocks_client, s3_config
):
    """Even a safe evolution (widen type) fails when evolution='none'."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_s3_none_safe_{uid}"
    db = starrocks_config.database

    _sr_create_table(starrocks_client, db, table, "`id` INT")
    _sr_insert(starrocks_client, db, table, "(1)")

    conn = _build_s3_conn(starrocks_config, s3_config, uid)
    # Widen INT -> BIGINT -- safe evolution, but mode is 'none'
    fn = _make_fn_output_bigint_only(conn, table, schema_evolution="none")
    fnrun = tdfnrun(
        function=fn, input_tables=_mock_parquet(), require_success=False
    )
    assert fnrun.status != 0

    cols = _get_columns(starrocks_client, db, table)
    id_type = _normalize_type(cols[0][1])
    assert "BIGINT" not in id_type  # type NOT widened


# =====================================================================================
# AZURE -- SAFE EVOLUTION
# =====================================================================================


@pytest.mark.starrocks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.azure
def test_azure_evolution_add_nullable_column(
    starrocks_config, starrocks_client, azure_config
):
    """Add a nullable column via Azure staging."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_az_add_col_{uid}"
    db = starrocks_config.database

    _sr_create_table(
        starrocks_client, db, table, "`id` BIGINT, `name` STRING"
    )
    _sr_insert(starrocks_client, db, table, "(1, 'alice')")

    conn = _build_azure_conn(starrocks_config, azure_config, uid)
    fn = _make_fn_output_bigint_string_email(
        conn, table, schema_evolution="iceberg"
    )
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet())
    assert fnrun.status == 0

    cols = _get_columns(starrocks_client, db, table)
    col_names = [c[0] for c in cols]
    assert "email" in col_names
    assert _row_count(starrocks_client, db, table) == 3


@pytest.mark.starrocks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.azure
def test_azure_evolution_widen_type(
    starrocks_config, starrocks_client, azure_config
):
    """Widen INT -> BIGINT via Azure staging."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_az_widen_{uid}"
    db = starrocks_config.database

    _sr_create_table(
        starrocks_client, db, table, "`key_col` BIGINT, `id` INT, `value` FLOAT"
    )
    _sr_insert(starrocks_client, db, table, "(1, 1, 1.5)")

    conn = _build_azure_conn(starrocks_config, azure_config, uid)
    fn = _make_fn_output_bigint_widened(
        conn, table, schema_evolution="iceberg"
    )
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet())
    assert fnrun.status == 0

    cols = _get_columns(starrocks_client, db, table)
    id_type = _normalize_type(next(c[1] for c in cols if c[0] == "id"))
    assert "BIGINT" in id_type
    assert _row_count(starrocks_client, db, table) == 3


@pytest.mark.starrocks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.azure
def test_azure_evolution_combined(
    starrocks_config, starrocks_client, azure_config
):
    """Combined evolution via Azure staging."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_az_combined_{uid}"
    db = starrocks_config.database

    _sr_create_table(
        starrocks_client, db, table,
        "`key_col` BIGINT, `id` INT, `name` STRING NOT NULL, `old_col` STRING NULL",
    )
    _sr_insert(starrocks_client, db, table, "(1, 1, 'alice', 'legacy')")

    conn = _build_azure_conn(starrocks_config, azure_config, uid)
    fn = _make_fn_output_combined_evolution(
        conn, table, schema_evolution="iceberg"
    )
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet())
    assert fnrun.status == 0

    cols = _get_columns(starrocks_client, db, table)
    col_names = [c[0] for c in cols]
    assert col_names == ["key_col", "id", "name", "tag"]
    assert _row_count(starrocks_client, db, table) == 3


# =====================================================================================
# AZURE -- UNSAFE / FAILING EVOLUTION
# =====================================================================================


@pytest.mark.starrocks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.azure
def test_azure_evolution_narrow_type_fails(
    starrocks_config, starrocks_client, azure_config
):
    """Narrowing BIGINT -> INT fails via Azure staging."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_az_narrow_{uid}"
    db = starrocks_config.database

    _sr_create_table(starrocks_client, db, table, "`id` BIGINT")
    _sr_insert(starrocks_client, db, table, "(1)")

    conn = _build_azure_conn(starrocks_config, azure_config, uid)
    fn = _make_fn_output_int_narrowed(
        conn, table, schema_evolution="iceberg"
    )
    fnrun = tdfnrun(
        function=fn, input_tables=_mock_parquet(), require_success=False
    )
    assert fnrun.status != 0


@pytest.mark.starrocks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.azure
def test_azure_no_evolution_schema_mismatch_fails(
    starrocks_config, starrocks_client, azure_config
):
    """With evolution='none', schema mismatch fails via Azure staging."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_az_none_fail_{uid}"
    db = starrocks_config.database

    _sr_create_table(
        starrocks_client, db, table, "`id` BIGINT, `name` STRING"
    )
    _sr_insert(starrocks_client, db, table, "(1, 'alice')")

    conn = _build_azure_conn(starrocks_config, azure_config, uid)
    fn = _make_fn_output_bigint_string_email(
        conn, table, schema_evolution="none"
    )
    fnrun = tdfnrun(
        function=fn, input_tables=_mock_parquet(), require_success=False
    )
    assert fnrun.status != 0


# =====================================================================================
# GCS -- SAFE EVOLUTION
# =====================================================================================


@pytest.mark.starrocks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.gcs
def test_gcs_evolution_add_nullable_column(
    starrocks_config, starrocks_client, gcs_config
):
    """Add a nullable column via GCS staging."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_gcs_add_col_{uid}"
    db = starrocks_config.database

    _sr_create_table(
        starrocks_client, db, table, "`id` BIGINT, `name` STRING"
    )
    _sr_insert(starrocks_client, db, table, "(1, 'alice')")

    conn = _build_gcs_conn(starrocks_config, gcs_config, uid)
    fn = _make_fn_output_bigint_string_email(
        conn, table, schema_evolution="iceberg"
    )
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet())
    assert fnrun.status == 0

    cols = _get_columns(starrocks_client, db, table)
    col_names = [c[0] for c in cols]
    assert "email" in col_names
    assert _row_count(starrocks_client, db, table) == 3


@pytest.mark.starrocks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.gcs
def test_gcs_evolution_widen_type(
    starrocks_config, starrocks_client, gcs_config
):
    """Widen INT -> BIGINT via GCS staging."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_gcs_widen_{uid}"
    db = starrocks_config.database

    _sr_create_table(
        starrocks_client, db, table, "`key_col` BIGINT, `id` INT, `value` FLOAT"
    )
    _sr_insert(starrocks_client, db, table, "(1, 1, 1.5)")

    conn = _build_gcs_conn(starrocks_config, gcs_config, uid)
    fn = _make_fn_output_bigint_widened(
        conn, table, schema_evolution="iceberg"
    )
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet())
    assert fnrun.status == 0

    cols = _get_columns(starrocks_client, db, table)
    id_type = _normalize_type(next(c[1] for c in cols if c[0] == "id"))
    assert "BIGINT" in id_type
    assert _row_count(starrocks_client, db, table) == 3


@pytest.mark.starrocks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.gcs
def test_gcs_evolution_combined(
    starrocks_config, starrocks_client, gcs_config
):
    """Combined evolution via GCS staging."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_gcs_combined_{uid}"
    db = starrocks_config.database

    _sr_create_table(
        starrocks_client, db, table,
        "`key_col` BIGINT, `id` INT, `name` STRING NOT NULL, `old_col` STRING NULL",
    )
    _sr_insert(starrocks_client, db, table, "(1, 1, 'alice', 'legacy')")

    conn = _build_gcs_conn(starrocks_config, gcs_config, uid)
    fn = _make_fn_output_combined_evolution(
        conn, table, schema_evolution="iceberg"
    )
    fnrun = tdfnrun(function=fn, input_tables=_mock_parquet())
    assert fnrun.status == 0

    cols = _get_columns(starrocks_client, db, table)
    col_names = [c[0] for c in cols]
    assert col_names == ["key_col", "id", "name", "tag"]
    assert _row_count(starrocks_client, db, table) == 3


# =====================================================================================
# GCS -- UNSAFE / FAILING EVOLUTION
# =====================================================================================


@pytest.mark.starrocks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.gcs
def test_gcs_evolution_narrow_type_fails(
    starrocks_config, starrocks_client, gcs_config
):
    """Narrowing BIGINT -> INT fails via GCS staging."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_gcs_narrow_{uid}"
    db = starrocks_config.database

    _sr_create_table(starrocks_client, db, table, "`id` BIGINT")
    _sr_insert(starrocks_client, db, table, "(1)")

    conn = _build_gcs_conn(starrocks_config, gcs_config, uid)
    fn = _make_fn_output_int_narrowed(
        conn, table, schema_evolution="iceberg"
    )
    fnrun = tdfnrun(
        function=fn, input_tables=_mock_parquet(), require_success=False
    )
    assert fnrun.status != 0


@pytest.mark.starrocks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.gcs
def test_gcs_evolution_incompatible_type_fails(
    starrocks_config, starrocks_client, gcs_config
):
    """Changing BIGINT -> STRING fails via GCS staging."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_gcs_incompat_{uid}"
    db = starrocks_config.database

    _sr_create_table(starrocks_client, db, table, "`id` BIGINT")
    _sr_insert(starrocks_client, db, table, "(1)")

    conn = _build_gcs_conn(starrocks_config, gcs_config, uid)
    fn = _make_fn_output_string_type_change(
        conn, table, schema_evolution="iceberg"
    )
    fnrun = tdfnrun(
        function=fn, input_tables=_mock_parquet(), require_success=False
    )
    assert fnrun.status != 0


@pytest.mark.starrocks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.gcs
def test_gcs_no_evolution_schema_mismatch_fails(
    starrocks_config, starrocks_client, gcs_config
):
    """With evolution='none', schema mismatch fails via GCS staging."""
    from test_tabsdata.fntest import tdfnrun

    uid = _uid()
    table = f"evo_gcs_none_fail_{uid}"
    db = starrocks_config.database

    _sr_create_table(
        starrocks_client, db, table, "`id` BIGINT, `name` STRING"
    )
    _sr_insert(starrocks_client, db, table, "(1, 'alice')")

    conn = _build_gcs_conn(starrocks_config, gcs_config, uid)
    fn = _make_fn_output_bigint_string_email(
        conn, table, schema_evolution="none"
    )
    fnrun = tdfnrun(
        function=fn, input_tables=_mock_parquet(), require_success=False
    )
    assert fnrun.status != 0
