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
from psycopg2 import ProgrammingError

from tests_tabsdata.bootest import TDLOCAL_FOLDER
from tests_tabsdata.conftest import (
    ABSOLUTE_TEST_FOLDER_LOCATION,
    TESTING_RESOURCES_FOLDER,
    clean_polars_df,
    read_json_and_clean,
)
from tests_tabsdata.testing_resources.test_output_new_postgres_driver_provided.example import (
    testing_tabsdata_function as output_new_postgres_driver_provided,
)
from tests_tabsdata.testing_resources.test_output_new_postgres_list.example import (
    testing_tabsdata_function as output_new_postgres_list,
)
from tests_tabsdata.testing_resources.test_output_new_postgres_schema.example import (
    testing_tabsdata_function as output_new_postgres_schema,
)
from tests_tabsdata.testing_resources.test_output_new_postgres_table_replace.example import (
    testing_tabsdata_function as output_new_postgres_table_replace,
)
from tests_tabsdata.testing_resources.test_output_new_postgres_transaction.example import (
    testing_tabsdata_function as output_new_postgres_transaction_generator,
)
from tests_tabsdata.testing_resources.test_output_new_postgres_types_check.example import (
    testing_tabsdata_function as output_new_postgres_types_check_generator,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ROOT_PROJECT_DIR = os.path.dirname(
    os.path.dirname(os.path.dirname(ABSOLUTE_TEST_FOLDER_LOCATION))
)
RESPONSE_FOLDER = "response_folder"

LOCAL_DEV_FOLDER = TDLOCAL_FOLDER


@pytest.mark.postgres
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_postgres_list(postgres_config):
    destination_tables = ["output_new_postgres_list", "second_output_new_postgres_list"]
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_postgres_list", "mock_table.parquet"
    )
    fnrun = tdfnrun(
        output_new_postgres_list(
            postgres_config.uri, postgres_config.credentials, destination_tables
        ),
        input_tables=mock_parquet_table,
    )
    assert fnrun.status == 0
    output = pl.read_database_uri(
        uri=postgres_config.uri_with_credentials,
        query="SELECT * FROM output_new_postgres_list",
    )
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_postgres_list",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    output = pl.read_database_uri(
        uri=postgres_config.uri_with_credentials,
        query="SELECT * FROM second_output_new_postgres_list",
    )
    output = clean_polars_df(output)
    assert output.equals(expected_output)


@pytest.mark.postgres
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_postgres_list_chunk_size(postgres_config):
    destination_tables = [
        "output_new_postgres_list_chunk_size",
        "second_output_new_postgres_list_chunk_size",
    ]
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_postgres_list", "mock_table.parquet"
    )
    fnrun = tdfnrun(
        output_new_postgres_list(
            postgres_config.uri,
            postgres_config.credentials,
            destination_tables,
            chunk_size=2,
        ),
        input_tables=mock_parquet_table,
    )
    assert fnrun.status == 0
    output = pl.read_database_uri(
        uri=postgres_config.uri_with_credentials,
        query="SELECT * FROM output_new_postgres_list_chunk_size",
    )
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_postgres_list",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    output = pl.read_database_uri(
        uri=postgres_config.uri_with_credentials,
        query="SELECT * FROM second_output_new_postgres_list_chunk_size",
    )
    output = clean_polars_df(output)
    assert output.equals(expected_output)


@pytest.mark.postgres
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_postgres_types_check(postgres_config):
    destination_tables = ["all_postgres_dest_types"]
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_postgres_types_check",
        "mock_table.parquet",
    )
    fnrun = tdfnrun(
        output_new_postgres_types_check_generator(
            postgres_config.uri, postgres_config.credentials, destination_tables
        ),
        input_tables=mock_parquet_table,
    )
    assert fnrun.status == 0
    output = pl.read_database_uri(
        uri=postgres_config.uri_with_credentials,
        query="SELECT * FROM all_postgres_dest_types",
    )
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_postgres_types_check",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.postgres
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_postgres_driver_provided(postgres_config):
    destination_tables = "output_new_postgres_driver_provided"
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_postgres_driver_provided",
        "mock_table.parquet",
    )
    fnrun = tdfnrun(
        output_new_postgres_driver_provided(
            postgres_config.uri, postgres_config.credentials, destination_tables
        ),
        input_tables=mock_parquet_table,
    )
    assert fnrun.status == 0
    output = pl.read_database_uri(
        uri=postgres_config.uri_with_credentials,
        query="SELECT * FROM output_new_postgres_driver_provided",
    )
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_postgres_driver_provided",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.postgres
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_postgres_w_transaction(postgres_config):
    destination_tables = [
        "output_new_postgres_transaction",
        "second_output_new_postgres_transaction",
    ]
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_postgres_transaction",
        "mock_table.parquet",
    )

    with pytest.raises(FnError) as error:
        tdfnrun(
            output_new_postgres_transaction_generator(
                postgres_config.uri,
                postgres_config.credentials,
                destination_tables,
                transactional=True,
            ),
            input_tables=mock_parquet_table,
        )

    fnrun = error.value.fnrun
    exception = fnrun.exception.as_object()
    assert exception.exception == ProgrammingError.__name__

    output = pl.read_database_uri(
        uri=postgres_config.uri_with_credentials,
        query="SELECT * FROM output_new_postgres_transaction",
    )
    output = clean_polars_df(output)
    assert output.is_empty()

    output = pl.read_database_uri(
        uri=postgres_config.uri_with_credentials,
        query="SELECT * FROM second_output_new_postgres_transaction",
    )
    output = clean_polars_df(output)
    assert output.is_empty()


@pytest.mark.postgres
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_postgres_wo_transaction(postgres_config):
    destination_tables = [
        "output_new_postgres_wo_transaction",
        "second_output_new_postgres_wo_transaction",
    ]
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_postgres_transaction",
        "mock_table.parquet",
    )

    with pytest.raises(FnError) as error:
        tdfnrun(
            output_new_postgres_transaction_generator(
                postgres_config.uri,
                postgres_config.credentials,
                destination_tables,
                transactional=False,
            ),
            input_tables=mock_parquet_table,
        )

    fnrun = error.value.fnrun
    exception = fnrun.exception.as_object()
    assert exception.exception == ProgrammingError.__name__

    output = pl.read_database_uri(
        uri=postgres_config.uri_with_credentials,
        query="SELECT * FROM output_new_postgres_wo_transaction",
    )
    output = clean_polars_df(output)
    assert not output.is_empty()

    output = pl.read_database_uri(
        uri=postgres_config.uri_with_credentials,
        query="SELECT * FROM second_output_new_postgres_wo_transaction",
    )
    output = clean_polars_df(output)
    assert output.is_empty()


@pytest.mark.postgres
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_postgres_table_replace(postgres_config):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_postgres_table_replace",
        "mock_table.parquet",
    )
    for _ in range(2):
        fnrun = tdfnrun(
            output_new_postgres_table_replace(
                postgres_config.uri,
                postgres_config.credentials,
                "output_new_postgres_table_replace",
            ),
            input_tables=mock_parquet_table,
        )
        assert fnrun.status == 0
        output = pl.read_database_uri(
            uri=postgres_config.uri_with_credentials,
            query="SELECT * FROM output_new_postgres_table_replace",
        )
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_new_postgres_table_replace",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)


@pytest.mark.postgres
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_postgres_schema(postgres_config):
    destination_tables = ["testing_schema.output_new_postgres_schema"]
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_postgres_schema",
        "mock_table.parquet",
    )
    for _ in range(2):
        fnrun = tdfnrun(
            output_new_postgres_schema(
                postgres_config.uri,
                postgres_config.credentials,
                destination_tables,
            ),
            input_tables=mock_parquet_table,
        )
        assert fnrun.status == 0
        output = pl.read_database_uri(
            uri=postgres_config.uri_with_credentials,
            query="SELECT * FROM testing_schema.output_new_postgres_schema",
        )
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_new_postgres_schema",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
