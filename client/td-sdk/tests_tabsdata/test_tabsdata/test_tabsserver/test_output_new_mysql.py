#
# Copyright 2026 Tabs Data Inc.
#

import logging
import os
from io import StringIO
from unittest import mock

import polars as pl
import pytest
from fntest import FnError, tdfnrun
from mysql.connector import InterfaceError
from sqlalchemy.exc import NoSuchModuleError

from tests_tabsdata.conftest import (
    TESTING_RESOURCES_FOLDER,
    clean_polars_df,
    read_json_and_clean,
)
from tests_tabsdata.testing_resources.test_output_new_sql_driver_provided.example import (
    testing_tabsdata_function as output_new_sql_driver_provided_generator,
)
from tests_tabsdata.testing_resources.test_output_new_sql_list.example import (
    testing_tabsdata_function as output_sql_list_generator,
)
from tests_tabsdata.testing_resources.test_output_new_sql_list_none.example import (
    testing_tabsdata_function as output_new_sql_list_none_generator,
)
from tests_tabsdata.testing_resources.test_output_new_sql_list_types_check.example import (
    testing_tabsdata_function as output_new_sql_list_types_check_generator,
)
from tests_tabsdata.testing_resources.test_output_new_sql_none.example import (
    testing_tabsdata_function as output_new_sql_none_generator,
)
from tests_tabsdata.testing_resources.test_output_new_sql_transaction.example import (
    testing_tabsdata_function as output_new_sql_transaction_generator,
)
from tests_tabsdata.testing_resources.test_output_new_sql_wrong_driver_fails.example import (
    testing_tabsdata_function as output_new_sql_wrong_driver_fails_generator,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.mysql
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_sql_list(mysql_config):
    destination_tables = ["output_new_sql_list", "second_output_new_sql_list"]
    testing_function = output_sql_list_generator(
        mysql_config.uri,
        credentials=mysql_config.credentials,
        destination_table=destination_tables,
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_sql_list", "mock_table.parquet"
    )
    fnrun = tdfnrun(function=testing_function, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    output = pl.read_database_uri(
        uri=mysql_config.uri_with_credentials,
        query="SELECT * FROM output_new_sql_list",
    )
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_sql_list",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    output = pl.read_database_uri(
        uri=mysql_config.uri_with_credentials,
        query="SELECT * FROM second_output_new_sql_list",
    )
    output = clean_polars_df(output)
    assert output.equals(expected_output)


@pytest.mark.mysql
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_sql_list_types_check(mysql_config):
    destination_tables = ["output_new_sql_list_types_check"]
    testing_function = output_new_sql_list_types_check_generator(
        mysql_config.uri,
        credentials=mysql_config.credentials,
        destination_table=destination_tables,
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_sql_list_types_check",
        "mock_table.parquet",
    )
    fnrun = tdfnrun(function=testing_function, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    output = pl.read_database_uri(
        uri=mysql_config.uri_with_credentials,
        query="SELECT * FROM output_new_sql_list_types_check",
    )
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_sql_list_types_check",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    expected_output = expected_output.with_columns(
        pl.col("c_date").str.strptime(pl.Date, "%Y-%m-%d"),
        pl.col("c_datetime").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"),
        pl.col("c_timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"),
    )
    assert output.equals(expected_output)


@pytest.mark.mysql
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_sql_list_chunk_size(mysql_config):
    destination_tables = [
        "output_new_sql_list_chunk_size",
        "second_output_new_sql_list_chunk_size",
    ]
    testing_function = output_sql_list_generator(
        mysql_config.uri,
        credentials=mysql_config.credentials,
        destination_table=destination_tables,
        chunk_size=10,
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_sql_list", "mock_table.parquet"
    )
    fnrun = tdfnrun(function=testing_function, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    output = pl.read_database_uri(
        uri=mysql_config.uri_with_credentials,
        query="SELECT * FROM output_new_sql_list_chunk_size",
    )
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_sql_list",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    output = pl.read_database_uri(
        uri=mysql_config.uri_with_credentials,
        query="SELECT * FROM second_output_new_sql_list_chunk_size",
    )
    output = clean_polars_df(output)
    assert output.equals(expected_output)


@pytest.mark.mysql
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_sql_list_w_transaction(mysql_config):
    destination_tables = [
        "output_new_sql_transaction",
        "second_output_new_sql_transaction",
    ]
    testing_function = output_new_sql_transaction_generator(
        mysql_config.uri,
        credentials=mysql_config.credentials,
        destination_table=destination_tables,
        transactional=True,
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_sql_transaction",
        "mock_table.parquet",
    )
    with pytest.raises(FnError) as error:
        tdfnrun(function=testing_function, input_tables=mock_parquet_table)

    fnrun = error.value.fnrun
    exception = fnrun.exception.as_object()
    assert exception.exception == InterfaceError.__name__

    output = pl.read_database_uri(
        uri=mysql_config.uri_with_credentials,
        query="SELECT * FROM output_new_sql_transaction",
    )
    output = clean_polars_df(output)
    assert output.is_empty()

    output = pl.read_database_uri(
        uri=mysql_config.uri_with_credentials,
        query="SELECT * FROM second_output_new_sql_transaction",
    )
    output = clean_polars_df(output)
    assert output.is_empty()


@pytest.mark.mysql
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_sql_list_wo_transaction(mysql_config):
    destination_tables = [
        "output_new_sql_wo_transaction",
        "second_output_new_sql_wo_transaction",
    ]
    testing_function = output_new_sql_transaction_generator(
        mysql_config.uri,
        credentials=mysql_config.credentials,
        destination_table=destination_tables,
        transactional=False,
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_sql_transaction",
        "mock_table.parquet",
    )
    with pytest.raises(FnError) as error:
        tdfnrun(function=testing_function, input_tables=mock_parquet_table)

    fnrun = error.value.fnrun
    exception = fnrun.exception.as_object()
    assert exception.exception == InterfaceError.__name__

    output = pl.read_database_uri(
        uri=mysql_config.uri_with_credentials,
        query="SELECT * FROM output_new_sql_wo_transaction",
    )
    output = clean_polars_df(output)
    assert not output.is_empty()

    output = pl.read_database_uri(
        uri=mysql_config.uri_with_credentials,
        query="SELECT * FROM second_output_new_sql_wo_transaction",
    )
    output = clean_polars_df(output)
    assert output.is_empty()


@pytest.mark.mysql
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_sql_wrong_driver_fails(mysql_config):
    destination_table = [
        "output_new_sql_wrong_driver_fails",
        "second_output_new_sql_wrong_driver_fails",
    ]
    testing_function = output_new_sql_wrong_driver_fails_generator(
        mysql_config.uri,
        credentials=mysql_config.credentials,
        destination_table=destination_table,
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_sql_wrong_driver_fails",
        "mock_table.parquet",
    )
    with pytest.raises(FnError) as error:
        tdfnrun(function=testing_function, input_tables=mock_parquet_table)

    fnrun = error.value.fnrun
    exception = fnrun.exception.as_object()
    assert exception.exception == NoSuchModuleError.__name__


@pytest.mark.mysql
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_sql_driver_provided(mysql_config):
    destination_table = "output_new_sql_driver_provided"
    testing_function = output_new_sql_driver_provided_generator(
        mysql_config.uri,
        credentials=mysql_config.credentials,
        destination_table=destination_table,
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_sql_driver_provided",
        "mock_table.parquet",
    )
    fnrun = tdfnrun(function=testing_function, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    output = pl.read_database_uri(
        uri=mysql_config.uri_with_credentials,
        query="SELECT * FROM output_new_sql_driver_provided",
    )
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_sql_driver_provided",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.mysql
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_sql_list_none(mysql_config):
    destination_tables = ["output_new_sql_list_none", "second_output_new_sql_list_none"]
    testing_function = output_new_sql_list_none_generator(
        mysql_config.uri,
        credentials=mysql_config.credentials,
        destination_table=destination_tables,
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_sql_list_none", "mock_table.parquet"
    )
    fnrun = tdfnrun(function=testing_function, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    with pytest.raises(Exception):
        pl.read_database_uri(
            uri=mysql_config.uri_with_credentials,
            query="SELECT * FROM output_new_sql_list_none",
        )
    with pytest.raises(Exception):
        pl.read_database_uri(
            uri=mysql_config.uri_with_credentials,
            query="SELECT * FROM second_output_new_sql_list_none",
        )


@pytest.mark.mysql
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_sql_none(mysql_config):
    destination_table = "output_new_sql_none"
    testing_function = output_new_sql_none_generator(
        mysql_config.uri,
        credentials=mysql_config.credentials,
        destination_table=destination_table,
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_sql_none",
        "mock_table.parquet",
    )
    fnrun = tdfnrun(function=testing_function, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    with pytest.raises(Exception):
        pl.read_database_uri(
            uri=mysql_config.uri_with_credentials,
            query="SELECT * FROM output_new_sql_none",
        )
