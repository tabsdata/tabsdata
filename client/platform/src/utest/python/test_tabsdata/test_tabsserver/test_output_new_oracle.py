#
# Copyright 2026 Tabs Data Inc.
#

import logging
import os

import polars as pl
import pytest
from fntest import tdfnrun

from test_tabsdata.conftest import (
    TESTING_RESOURCES_FOLDER,
    clean_polars_df,
    read_json_and_clean,
)
from test_tabsdata.test_output_new_oracle_driver_provided.example import (
    testing_tabsdata_function as output_new_oracle_driver_provided,
)
from test_tabsdata.test_output_new_oracle_list.example import (
    testing_tabsdata_function as output_new_oracle_list,
)

# noinspection PyUnresolvedReferences
from . import pytestmark as _package_pytestmark  # noqa: F401

pytestmark = [_package_pytestmark, pytest.mark.xdist_group("platform-output_new_oracle_seq")]

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.oracle
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_oracle_list(oracle_config):
    destination_tables = ["output_new_oracle_list", "second_output_new_oracle_list"]
    fn = output_new_oracle_list(
        uri=oracle_config.uri,
        credentials=oracle_config.credentials,
        destination_tables=destination_tables,
    )

    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_oracle_list", "mock_table.parquet"
    )
    fnrun = tdfnrun(function=fn, input_tables=mock_parquet_table)
    assert fnrun.status == 0

    output = pl.read_database_uri(
        uri=oracle_config.uri_with_credentials,
        query="SELECT * FROM output_new_oracle_list",
    )
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_oracle_list",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    output = pl.read_database_uri(
        uri=oracle_config.uri_with_credentials,
        query="SELECT * FROM second_output_new_oracle_list",
    )
    output = clean_polars_df(output)
    assert output.equals(expected_output)


@pytest.mark.oracle
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_oracle_list_chunk_size(oracle_config):
    destination_tables = [
        "output_new_oracle_list_chunk_size",
        "second_output_new_oracle_list_chunk_size",
    ]
    fn = output_new_oracle_list(
        uri=oracle_config.uri,
        credentials=oracle_config.credentials,
        destination_tables=destination_tables,
        chunk_size=10,
    )

    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_oracle_list", "mock_table.parquet"
    )
    fnrun = tdfnrun(function=fn, input_tables=mock_parquet_table)
    assert fnrun.status == 0

    output = pl.read_database_uri(
        uri=oracle_config.uri_with_credentials,
        query="SELECT * FROM output_new_oracle_list_chunk_size",
    )
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_oracle_list",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    output = pl.read_database_uri(
        uri=oracle_config.uri_with_credentials,
        query="SELECT * FROM second_output_new_oracle_list_chunk_size",
    )
    output = clean_polars_df(output)
    assert output.equals(expected_output)


@pytest.mark.oracle
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_oracle_driver_provided(oracle_config):
    destination_tables = ["output_new_oracle_driver_provided"]
    fn = output_new_oracle_driver_provided(
        uri=oracle_config.uri,
        credentials=oracle_config.credentials,
        destination_tables=destination_tables,
    )

    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_oracle_driver_provided",
        "mock_table.parquet",
    )
    fnrun = tdfnrun(function=fn, input_tables=mock_parquet_table)
    assert fnrun.status == 0

    output = pl.read_database_uri(
        uri=oracle_config.uri_with_credentials,
        query="SELECT * FROM output_new_oracle_driver_provided",
    )
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_oracle_driver_provided",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


