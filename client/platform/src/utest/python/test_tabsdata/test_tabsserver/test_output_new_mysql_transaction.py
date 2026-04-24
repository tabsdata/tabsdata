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
from sqlalchemy.exc import NoSuchModuleError

from test_tabsdata.conftest import (
    TESTING_RESOURCES_FOLDER,
    clean_polars_df,
)
from test_tabsdata.fntest import FnError, tdfnrun
from test_tabsdata.test_output_new_sql_transaction.example import (
    testing_tabsdata_function as output_new_sql_transaction_generator,
)
from test_tabsdata.test_output_new_sql_wrong_driver_fails.example import (
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
    assert exception.exception in (InterfaceError.__name__, ProgrammingError.__name__)

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
    assert exception.exception in (InterfaceError.__name__, ProgrammingError.__name__)

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
