#
# Copyright 2026 Tabsdata Inc.
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
from . import pytestmark as _package_pytestmark  # noqa: F401

pytestmark = [_package_pytestmark, pytest.mark.xdist_group("starrocks_seq")]

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

