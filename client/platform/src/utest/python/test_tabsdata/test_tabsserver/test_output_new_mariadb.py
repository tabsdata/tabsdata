#
# Copyright 2026 Tabs Data Inc.
#

import logging
import os
from io import StringIO
from unittest import mock

import polars as pl
import pytest
from mysql.connector import InterfaceError, ProgrammingError

from test_tabsdata.conftest import (
    TESTING_RESOURCES_FOLDER,
    clean_polars_df,
    read_json_and_clean,
)
from test_tabsdata.fntest import FnError, tdfnrun
from test_tabsdata.test_output_new_mariadb_driver_provided.example import (
    testing_tabsdata_function as output_new_mariadb_driver_provided_generator,
)
from test_tabsdata.test_output_new_mariadb_list.example import (
    testing_tabsdata_function as output_new_mariadb_list_generator,
)
from test_tabsdata.test_output_new_mariadb_list_types_check.example import (
    testing_tabsdata_function as output_new_mariadb_list_types_check_generator,
)
from test_tabsdata.test_output_new_mariadb_transaction.example import (
    testing_tabsdata_function as output_new_mariadb_transaction_generator,
)
from test_tabsdata.test_output_new_mariadb_with_charset.example import (
    testing_tabsdata_function as output_new_mariadb_with_charset_generator,
)
from test_tabsdata.test_output_new_mariadb_with_collation.example import (
    testing_tabsdata_function as output_new_mariadb_with_collation_generator,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.mariadb
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_mariadb_list(mariadb_config):
    destination_tables = ["output_new_mariadb_list", "second_output_new_mariadb_list"]
    testing_function = output_new_mariadb_list_generator(
        mariadb_config.uri,
        credentials=mariadb_config.credentials,
        destination_tables=destination_tables,
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_mariadb_list", "mock_table.parquet"
    )
    fnrun = tdfnrun(function=testing_function, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    output = pl.read_database_uri(
        uri=mariadb_config.uri_with_credentials,
        query="SELECT * FROM output_new_mariadb_list",
    )
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_mariadb_list",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    output = pl.read_database_uri(
        uri=mariadb_config.uri_with_credentials,
        query="SELECT * FROM second_output_new_mariadb_list",
    )
    output = clean_polars_df(output)
    assert output.equals(expected_output)


@pytest.mark.mariadb
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_mariadb_list_chunk_size(mariadb_config):
    destination_tables = [
        "output_new_mariadb_list_chunk_size",
        "second_output_new_mariadb_list_chunk_size",
    ]
    testing_function = output_new_mariadb_list_generator(
        mariadb_config.uri,
        credentials=mariadb_config.credentials,
        destination_tables=destination_tables,
        chunk_size=10,
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_mariadb_list", "mock_table.parquet"
    )
    fnrun = tdfnrun(function=testing_function, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    output = pl.read_database_uri(
        uri=mariadb_config.uri_with_credentials,
        query="SELECT * FROM output_new_mariadb_list_chunk_size",
    )
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_mariadb_list",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    output = pl.read_database_uri(
        uri=mariadb_config.uri_with_credentials,
        query="SELECT * FROM second_output_new_mariadb_list_chunk_size",
    )
    output = clean_polars_df(output)
    assert output.equals(expected_output)


@pytest.mark.mariadb
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_mariadb_list_types_check(mariadb_config):
    destination_table = "output_new_mariadb_list_types_check"
    testing_function = output_new_mariadb_list_types_check_generator(
        mariadb_config.uri,
        credentials=mariadb_config.credentials,
        destination_table=destination_table,
        transactional=True,
        chunk_size=10,
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_mariadb_list_types_check",
        "mock_table.parquet",
    )
    fnrun = tdfnrun(function=testing_function, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    output = pl.read_database_uri(
        uri=mariadb_config.uri_with_credentials,
        query="SELECT * FROM output_new_mariadb_list_types_check",
    )
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_mariadb_list_types_check",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    expected_output = expected_output.with_columns(
        pl.col("c_date").str.strptime(pl.Date, "%Y-%m-%d"),
        pl.col("c_datetime").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"),
        pl.col("c_timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"),
    )
    assert output.equals(expected_output)


@pytest.mark.mariadb
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_mariadb_list_w_transaction(mariadb_config):
    destination_tables = [
        "output_new_mariadb_transaction",
        "second_output_new_mariadb_transaction",
    ]
    testing_function = output_new_mariadb_transaction_generator(
        mariadb_config.uri,
        credentials=mariadb_config.credentials,
        destination_tables=destination_tables,
        transactional=True,
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_mariadb_transaction",
        "mock_table.parquet",
    )
    with pytest.raises(FnError) as error:
        tdfnrun(function=testing_function, input_tables=mock_parquet_table)

    fnrun = error.value.fnrun
    exception = fnrun.exception.as_object()
    assert exception.exception in (InterfaceError.__name__, ProgrammingError.__name__)

    output = pl.read_database_uri(
        uri=mariadb_config.uri_with_credentials,
        query="SELECT * FROM output_new_mariadb_transaction",
    )
    output = clean_polars_df(output)
    assert output.is_empty()

    output = pl.read_database_uri(
        uri=mariadb_config.uri_with_credentials,
        query="SELECT * FROM second_output_new_mariadb_transaction",
    )
    output = clean_polars_df(output)
    assert output.is_empty()


@pytest.mark.mariadb
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_mariadb_list_wo_transaction(mariadb_config):
    destination_tables = [
        "output_new_mariadb_wo_transaction",
        "second_output_new_mariadb_wo_transaction",
    ]
    testing_function = output_new_mariadb_transaction_generator(
        mariadb_config.uri,
        credentials=mariadb_config.credentials,
        destination_tables=destination_tables,
        transactional=False,
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_mariadb_transaction",
        "mock_table.parquet",
    )
    with pytest.raises(FnError) as error:
        tdfnrun(function=testing_function, input_tables=mock_parquet_table)

    fnrun = error.value.fnrun
    exception = fnrun.exception.as_object()
    assert exception.exception in (InterfaceError.__name__, ProgrammingError.__name__)

    output = pl.read_database_uri(
        uri=mariadb_config.uri_with_credentials,
        query="SELECT * FROM output_new_mariadb_wo_transaction",
    )
    output = clean_polars_df(output)
    assert not output.is_empty()

    output = pl.read_database_uri(
        uri=mariadb_config.uri_with_credentials,
        query="SELECT * FROM second_output_new_mariadb_wo_transaction",
    )
    output = clean_polars_df(output)
    assert output.is_empty()


@pytest.mark.mariadb
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_mariadb_driver_provided(mariadb_config):
    destination_tables = "output_new_mariadb_driver_provided"
    testing_function = output_new_mariadb_driver_provided_generator(
        mariadb_config.uri,
        credentials=mariadb_config.credentials,
        destination_tables=destination_tables,
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_mariadb_driver_provided",
        "mock_table.parquet",
    )
    fnrun = tdfnrun(function=testing_function, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    output = pl.read_database_uri(
        uri=mariadb_config.uri_with_credentials,
        query="SELECT * FROM output_new_mariadb_driver_provided",
    )
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_mariadb_driver_provided",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.mariadb
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_mariadb_with_charset(mariadb_config):
    destination_tables = "output_new_mariadb_with_charset"
    testing_function = output_new_mariadb_with_charset_generator(
        mariadb_config.uri,
        credentials=mariadb_config.credentials,
        destination_tables=destination_tables,
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_mariadb_with_charset",
        "mock_table.parquet",
    )
    fnrun = tdfnrun(function=testing_function, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    output = pl.read_database_uri(
        uri=mariadb_config.uri_with_credentials,
        query="SELECT * FROM output_new_mariadb_with_charset",
    )
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_mariadb_with_charset",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.mariadb
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_mariadb_with_collation(mariadb_config):
    destination_tables = "output_new_mariadb_with_collation"
    testing_function = output_new_mariadb_with_collation_generator(
        mariadb_config.uri,
        credentials=mariadb_config.credentials,
        destination_tables=destination_tables,
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_mariadb_with_collation",
        "mock_table.parquet",
    )
    fnrun = tdfnrun(function=testing_function, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    output = pl.read_database_uri(
        uri=mariadb_config.uri_with_credentials,
        query="SELECT * FROM output_new_mariadb_with_collation",
    )
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_mariadb_with_collation",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)
