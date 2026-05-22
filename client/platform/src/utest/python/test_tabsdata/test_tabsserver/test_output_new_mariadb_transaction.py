#
# Copyright 2026 Tabs Data Inc.
#

import logging
import os

import polars as pl
import pytest
from mysql.connector import InterfaceError, ProgrammingError

from test_tabsdata.conftest import (
    TESTING_RESOURCES_FOLDER,
    clean_polars_df,
)
from test_tabsdata.fntest import FnError, tdfnrun
from test_tabsdata.test_output_new_mariadb_transaction.example import (
    testing_tabsdata_function as output_new_mariadb_transaction_generator,
)

# noinspection PyUnresolvedReferences
from . import pytestmark as _package_pytestmark  # noqa: F401

pytestmark = [_package_pytestmark, pytest.mark.xdist_group("platform-output_new_mariadb_transaction_seq")]

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


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
