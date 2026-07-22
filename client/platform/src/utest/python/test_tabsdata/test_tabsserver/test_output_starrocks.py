#
# Copyright 2026 Tabsdata Inc.
#

import logging
import os
import uuid

import polars as pl
import pytest

import tabsdata as td
from test_tabsdata.conftest import (
    TESTING_RESOURCES_FOLDER,
    clean_polars_df,
    read_json_and_clean,
)
from test_tabsdata.fntest import tdfnrun
from test_tabsdata.test_output_starrocks_list.example import (
    testing_tabsdata_function as output_starrocks_list_generator,
)
from test_tabsdata.test_output_starrocks_list_none.example import (
    testing_tabsdata_function as output_starrocks_list_none_generator,
)
from test_tabsdata.test_output_starrocks_none.example import (
    testing_tabsdata_function as output_starrocks_none_generator,
)

# noinspection PyUnresolvedReferences
from . import pytestmark as _package_pytestmark  # noqa: F401

pytestmark = [_package_pytestmark, pytest.mark.xdist_group("starrocks_seq")]

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# ── Helpers ──────────────────────────────────────────────────────────────────────────


def _build_conn(starrocks_config, s3_config, uid: str) -> td.StarRocksConn:
    """Build a StarRocksConn using the S3 staging area."""
    staging = td.S3Config(
        bucket=s3_config["BUCKET_NAME"],
        region=s3_config["REGION"],
        base_path=f"tabsdata-testing/starrocks-output-{uid}",
        credentials=s3_config["CREDENTIALS"],
    )
    return td.StarRocksConn(
        host=starrocks_config.host,
        port=starrocks_config.port,
        database=starrocks_config.database,
        credentials=starrocks_config.credentials,
        staging=staging,
    )


def _query_table(starrocks_client, table_name: str) -> pl.DataFrame:
    """Query a StarRocks table and return a polars DataFrame."""
    with starrocks_client.cursor() as cursor:
        cursor.execute(f"SELECT * FROM `{table_name}`")
        rows = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]
    if not rows:
        return pl.DataFrame()
    # mysql-connector-python may return mixed int/float for DOUBLE columns
    # (whole numbers as int, fractional as float). Convert all values to
    # float where any value in the column is a float, to avoid Polars
    # mixed-type errors.
    columns: dict[str, list] = {name: [] for name in col_names}
    for row in rows:
        for name, val in zip(col_names, row):
            columns[name].append(val)
    for name, vals in columns.items():
        has_float = any(isinstance(v, float) for v in vals if v is not None)
        if has_float:
            columns[name] = [float(v) if v is not None else None for v in vals]
    return pl.DataFrame(columns)


def _table_exists(starrocks_client, database: str, table_name: str) -> bool:
    """Check whether a table exists in StarRocks."""
    with starrocks_client.cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
            (database, table_name),
        )
        return cursor.fetchone()[0] > 0


def _expected_output() -> pl.DataFrame:
    """Load and clean the expected result for the list test."""
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_starrocks_list",
        "expected_result.json",
    )
    return read_json_and_clean(expected_output_file)


def _assert_data_matches(starrocks_client, table_name: str, expected: pl.DataFrame):
    """Query the table and assert the cleaned data matches expected."""
    output = _query_table(starrocks_client, table_name)
    output = clean_polars_df(output)
    assert output.shape[0] > 0, f"Table '{table_name}' has no rows"
    assert output.equals(expected), (
        f"Data mismatch in table '{table_name}':\n"
        f"  expected shape: {expected.shape}\n"
        f"  actual shape:   {output.shape}\n"
        f"  expected:\n{expected}\n"
        f"  actual:\n{output}"
    )


# ── Tests ────────────────────────────────────────────────────────────────────────────


@pytest.mark.starrocks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_output_starrocks_list(starrocks_config, starrocks_client, s3_config):
    uid = uuid.uuid4().hex[:12]
    table1 = f"sr_output_list_{uid}"
    table2 = f"second_sr_output_list_{uid}"

    conn = _build_conn(starrocks_config, s3_config, uid)
    testing_function = output_starrocks_list_generator(
        conn=conn,
        destination_table=[table1, table2],
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_starrocks_list",
        "mock_table.parquet",
    )
    fnrun = tdfnrun(function=testing_function, input_tables=mock_parquet_table)
    assert fnrun.status == 0

    expected = _expected_output()
    for table_name in [table1, table2]:
        _assert_data_matches(starrocks_client, table_name, expected)


@pytest.mark.starrocks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_output_starrocks_list_none(starrocks_config, starrocks_client, s3_config):
    uid = uuid.uuid4().hex[:12]
    table1 = f"sr_output_list_none_{uid}"
    table2 = f"second_sr_output_list_none_{uid}"

    conn = _build_conn(starrocks_config, s3_config, uid)
    testing_function = output_starrocks_list_none_generator(
        conn=conn,
        destination_table=[table1, table2],
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_starrocks_list_none",
        "mock_table.parquet",
    )
    fnrun = tdfnrun(function=testing_function, input_tables=mock_parquet_table)
    assert fnrun.status == 0

    # Neither table should have been created (all results were None)
    for table_name in [table1, table2]:
        assert not _table_exists(
            starrocks_client, starrocks_config.database, table_name
        ), f"Table '{table_name}' should not exist after a None result"


@pytest.mark.starrocks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_output_starrocks_none(starrocks_config, starrocks_client, s3_config):
    uid = uuid.uuid4().hex[:12]
    table_name = f"sr_output_none_{uid}"

    conn = _build_conn(starrocks_config, s3_config, uid)
    testing_function = output_starrocks_none_generator(
        conn=conn,
        destination_table=table_name,
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_starrocks_none",
        "mock_table.parquet",
    )
    fnrun = tdfnrun(function=testing_function, input_tables=mock_parquet_table)
    assert fnrun.status == 0

    # Table should not have been created (result was None)
    assert not _table_exists(
        starrocks_client, starrocks_config.database, table_name
    ), f"Table '{table_name}' should not exist after a None result"


@pytest.mark.starrocks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_output_starrocks_append(starrocks_config, starrocks_client, s3_config):
    """Writing twice with if_table_exists='append' doubles the data."""
    uid = uuid.uuid4().hex[:12]
    table1 = f"sr_output_append_{uid}"
    table2 = f"second_sr_output_append_{uid}"

    conn = _build_conn(starrocks_config, s3_config, uid)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_starrocks_list",
        "mock_table.parquet",
    )

    expected_single = _expected_output()

    # First write (append mode — creates the tables)
    testing_function_1 = output_starrocks_list_generator(
        conn=conn,
        destination_table=[table1, table2],
    )
    fnrun = tdfnrun(function=testing_function_1, input_tables=mock_parquet_table)
    assert fnrun.status == 0

    # Verify data after first write
    for table_name in [table1, table2]:
        _assert_data_matches(starrocks_client, table_name, expected_single)

    # Second write (append mode — adds rows)
    testing_function_2 = output_starrocks_list_generator(
        conn=conn,
        destination_table=[table1, table2],
    )
    fnrun2 = tdfnrun(function=testing_function_2, input_tables=mock_parquet_table)
    assert fnrun2.status == 0

    # After appending, the table should have exactly double the rows
    expected_doubled = pl.concat([expected_single, expected_single])
    expected_doubled = clean_polars_df(expected_doubled)

    for table_name in [table1, table2]:
        output = _query_table(starrocks_client, table_name)
        output = clean_polars_df(output)
        assert output.shape[0] == expected_single.shape[0] * 2, (
            f"Expected {expected_single.shape[0] * 2} rows after append, "
            f"got {output.shape[0]} in table '{table_name}'"
        )
        assert output.equals(expected_doubled), (
            f"Data mismatch after append in table '{table_name}':\n"
            f"  expected:\n{expected_doubled}\n"
            f"  actual:\n{output}"
        )


@pytest.mark.starrocks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_output_starrocks_replace(starrocks_config, starrocks_client, s3_config):
    """Writing with if_table_exists='replace' truncates the table before loading."""
    uid = uuid.uuid4().hex[:12]
    table1 = f"sr_output_replace_{uid}"
    table2 = f"second_sr_output_replace_{uid}"

    conn = _build_conn(starrocks_config, s3_config, uid)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_starrocks_list",
        "mock_table.parquet",
    )

    expected = _expected_output()

    # First write (append mode — creates the tables)
    testing_function_append = output_starrocks_list_generator(
        conn=conn,
        destination_table=[table1, table2],
    )
    fnrun = tdfnrun(function=testing_function_append, input_tables=mock_parquet_table)
    assert fnrun.status == 0

    # Verify data after first write
    for table_name in [table1, table2]:
        _assert_data_matches(starrocks_client, table_name, expected)

    # Second write in replace mode — data should be identical (truncate + refill)
    testing_function_replace = output_starrocks_list_generator(
        conn=conn,
        destination_table=[table1, table2],
        if_table_exists="replace",
    )
    fnrun2 = tdfnrun(function=testing_function_replace, input_tables=mock_parquet_table)
    assert fnrun2.status == 0

    # After replace, the data should be exactly the same as a single write
    for table_name in [table1, table2]:
        _assert_data_matches(starrocks_client, table_name, expected)
