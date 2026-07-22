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
from test_tabsdata.test_output_clickhouse_list.example import (
    testing_tabsdata_function as output_clickhouse_list_generator,
)
from test_tabsdata.test_output_clickhouse_list_none.example import (
    testing_tabsdata_function as output_clickhouse_list_none_generator,
)
from test_tabsdata.test_output_clickhouse_none.example import (
    testing_tabsdata_function as output_clickhouse_none_generator,
)

# noinspection PyUnresolvedReferences
from . import pytestmark as _package_pytestmark  # noqa: F401

pytestmark = [_package_pytestmark, pytest.mark.xdist_group("platform-output_clickhouse_seq")]

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# ── Helpers ───────────────────────────────────────────────────────────────────────────


def _build_conn(clickhouse_config, s3_config, uid: str) -> td.ClickHouseConn:
    """Build a ClickHouseConn using the S3 staging area."""
    staging = td.S3Config(
        bucket=s3_config["BUCKET_NAME"],
        region=s3_config["REGION"],
        base_path=f"tabsdata-testing/clickhouse-output-{uid}",
        credentials=s3_config["CREDENTIALS"],
    )
    return td.ClickHouseConn(
        host=clickhouse_config.host,
        port=clickhouse_config.port,
        database=clickhouse_config.database,
        credentials=clickhouse_config.credentials,
        staging=staging,
    )


def _query_table(clickhouse_client, table_name: str) -> pl.DataFrame:
    """Query a ClickHouse table and return a polars DataFrame."""
    result = clickhouse_client.query(f"SELECT * FROM `{table_name}`")
    return pl.from_dicts(list(result.named_results()))


# ── Tests ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.clickhouse
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_output_clickhouse_list(clickhouse_config, clickhouse_client, s3_config):
    uid = uuid.uuid4().hex[:12]
    table1 = f"ch_output_list_{uid}"
    table2 = f"second_ch_output_list_{uid}"

    conn = _build_conn(clickhouse_config, s3_config, uid)
    testing_function = output_clickhouse_list_generator(
        conn=conn,
        destination_table=[table1, table2],
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_clickhouse_list",
        "mock_table.parquet",
    )
    fnrun = tdfnrun(function=testing_function, input_tables=mock_parquet_table)
    assert fnrun.status == 0

    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_clickhouse_list",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)

    for table_name in [table1, table2]:
        output = _query_table(clickhouse_client, table_name)
        output = clean_polars_df(output)
        assert output.equals(expected_output)


@pytest.mark.clickhouse
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_output_clickhouse_list_none(
    clickhouse_config, clickhouse_client, s3_config
):
    uid = uuid.uuid4().hex[:12]
    table1 = f"ch_output_list_none_{uid}"
    table2 = f"second_ch_output_list_none_{uid}"

    conn = _build_conn(clickhouse_config, s3_config, uid)
    testing_function = output_clickhouse_list_none_generator(
        conn=conn,
        destination_table=[table1, table2],
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_clickhouse_list_none",
        "mock_table.parquet",
    )
    fnrun = tdfnrun(function=testing_function, input_tables=mock_parquet_table)
    assert fnrun.status == 0

    # Neither table should have been created (all results were None)
    for table_name in [table1, table2]:
        result = clickhouse_client.query(
            "SELECT count() FROM system.tables "
            "WHERE database = {db:String} AND name = {tbl:String}",
            parameters={"db": clickhouse_config.database, "tbl": table_name},
        )
        assert result.result_rows[0][0] == 0, (
            f"Table '{table_name}' should not exist after a None result"
        )


@pytest.mark.clickhouse
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_output_clickhouse_none(clickhouse_config, clickhouse_client, s3_config):
    uid = uuid.uuid4().hex[:12]
    table_name = f"ch_output_none_{uid}"

    conn = _build_conn(clickhouse_config, s3_config, uid)
    testing_function = output_clickhouse_none_generator(
        conn=conn,
        destination_table=table_name,
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_clickhouse_none",
        "mock_table.parquet",
    )
    fnrun = tdfnrun(function=testing_function, input_tables=mock_parquet_table)
    assert fnrun.status == 0

    # Table should not have been created (result was None)
    result = clickhouse_client.query(
        "SELECT count() FROM system.tables "
        "WHERE database = {db:String} AND name = {tbl:String}",
        parameters={"db": clickhouse_config.database, "tbl": table_name},
    )
    assert result.result_rows[0][0] == 0, (
        f"Table '{table_name}' should not exist after a None result"
    )


@pytest.mark.clickhouse
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_output_clickhouse_replace(clickhouse_config, clickhouse_client, s3_config):
    """Writing with if_table_exists='replace' truncates the table before loading."""
    uid = uuid.uuid4().hex[:12]
    table1 = f"ch_output_replace_{uid}"
    table2 = f"second_ch_output_replace_{uid}"

    conn = _build_conn(clickhouse_config, s3_config, uid)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_clickhouse_list",
        "mock_table.parquet",
    )

    # First write (append mode — creates the tables)
    testing_function_append = output_clickhouse_list_generator(
        conn=conn,
        destination_table=[table1, table2],
    )
    fnrun = tdfnrun(function=testing_function_append, input_tables=mock_parquet_table)
    assert fnrun.status == 0

    count_after_first = clickhouse_client.query(
        f"SELECT count() FROM `{table1}`"
    ).result_rows[0][0]

    # Second write in replace mode — row count should stay the same (truncate + refill)
    testing_function_replace = output_clickhouse_list_generator(
        conn=conn,
        destination_table=[table1, table2],
        if_table_exists="replace",
    )
    fnrun2 = tdfnrun(function=testing_function_replace, input_tables=mock_parquet_table)
    assert fnrun2.status == 0

    count_after_replace = clickhouse_client.query(
        f"SELECT count() FROM `{table1}`"
    ).result_rows[0][0]

    assert count_after_replace == count_after_first
