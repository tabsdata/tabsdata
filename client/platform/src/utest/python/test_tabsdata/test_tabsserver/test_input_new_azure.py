#
# Copyright 2026 Tabs Data Inc.
#

import logging
import os

import polars as pl
import pytest

from test_tabsdata.fntest import tdfnrun
from test_tabsdata.conftest import (
    TESTING_RESOURCES_FOLDER,
    clean_polars_df,
    read_json_and_clean,
)
from test_tabsdata.test_input_new_azure.example import (
    testing_tabsdata_function as input_new_azure,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.azure
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_input_new_azure_csv(azure_config):
    fnrun = tdfnrun(
        input_new_azure(
            bucket_uri=azure_config["URI"],
            azure_credentials=azure_config["CREDENTIALS"],
            path="testing_resources/data.csv",
        )
    )
    assert fnrun.status == 0

    output_file = fnrun.output[0].path
    assert os.path.exists(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_azure",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.azure
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_input_new_azure_csv_infer_schema_length(azure_config):
    fnrun = tdfnrun(
        input_new_azure(
            bucket_uri=azure_config["URI"],
            azure_credentials=azure_config["CREDENTIALS"],
            path="testing_resources/data.csv",
            infer_schema_length=10,
        )
    )
    assert fnrun.status == 0

    output_file = fnrun.output[0].path
    assert os.path.exists(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_azure",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.azure
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_input_new_azure_log(azure_config):
    fnrun = tdfnrun(
        input_new_azure(
            bucket_uri=azure_config["URI"],
            azure_credentials=azure_config["CREDENTIALS"],
            path="testing_resources/data.log",
        )
    )
    assert fnrun.status == 0

    output_file = fnrun.output[0].path
    assert os.path.exists(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_azure",
        "expected_result_log.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.azure
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_input_new_azure_parquet(azure_config):
    fnrun = tdfnrun(
        input_new_azure(
            bucket_uri=azure_config["URI"],
            azure_credentials=azure_config["CREDENTIALS"],
            path="testing_resources/data.parquet",
        )
    )
    assert fnrun.status == 0

    output_file = fnrun.output[0].path
    assert os.path.exists(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_azure",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.azure
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_input_new_azure_ndjson(azure_config):
    fnrun = tdfnrun(
        input_new_azure(
            bucket_uri=azure_config["URI"],
            azure_credentials=azure_config["CREDENTIALS"],
            path="testing_resources/data.ndjson",
        )
    )
    assert fnrun.status == 0

    output_file = fnrun.output[0].path
    assert os.path.exists(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_azure",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)
