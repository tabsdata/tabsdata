#
# Copyright 2026 Tabs Data Inc.
#

import logging
import os

import polars as pl
import pytest

import tabsdata as td
from test_tabsdata.fntest import tdfnrun
from test_tabsdata.conftest import (
    TESTING_RESOURCES_FOLDER,
    clean_polars_df,
    read_json_and_clean,
)
from test_tabsdata.test_input_new_gcs.example import (
    testing_tabsdata_function as input_new_gcs,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.gcs
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_input_new_gcs_csv(gcs_config):
    fnrun = tdfnrun(
        input_new_gcs(
            bucket_uri=f"gs://{gcs_config['BUCKET']}",
            gcs_credentials=td.GCPServiceAccountKeyCredentials(
                td.EnvironmentSecret(gcs_config["ENV"])
            ),
            path="testing_resources/data.csv",
        ),
    )
    assert fnrun.status == 0

    output_file = fnrun.output[0].path
    assert os.path.exists(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_gcs",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.gcs
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_input_new_gcs_csv_infer_schema_length(gcs_config):
    fnrun = tdfnrun(
        input_new_gcs(
            bucket_uri=f"gs://{gcs_config['BUCKET']}",
            gcs_credentials=td.GCPServiceAccountKeyCredentials(
                td.EnvironmentSecret(gcs_config["ENV"])
            ),
            path="testing_resources/data.csv",
            infer_schema_length=10,
        ),
    )
    assert fnrun.status == 0

    output_file = fnrun.output[0].path
    assert os.path.exists(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_gcs",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.gcs
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_input_new_gcs_csv_file_info_name(gcs_config):
    fnrun = tdfnrun(
        input_new_gcs(
            bucket_uri=f"gs://{gcs_config['BUCKET']}",
            gcs_credentials=td.GCPServiceAccountKeyCredentials(
                td.EnvironmentSecret(gcs_config["ENV"])
            ),
            path="testing_resources/data.csv",
            file_info="name",
        ),
    )
    assert fnrun.status == 0

    output_file = fnrun.output[0].path
    assert os.path.exists(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_gcs",
        "expected_result_file_info_name.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.gcs
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_input_new_gcs_csv_file_info_uri(gcs_config):
    fnrun = tdfnrun(
        input_new_gcs(
            bucket_uri=f"gs://{gcs_config['BUCKET']}",
            gcs_credentials=td.GCPServiceAccountKeyCredentials(
                td.EnvironmentSecret(gcs_config["ENV"])
            ),
            path="testing_resources/data.csv",
            file_info="uri",
        ),
    )
    assert fnrun.status == 0

    output_file = fnrun.output[0].path
    assert os.path.exists(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_gcs",
        "expected_result_file_info_uri.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.gcs
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_input_new_gcs_parquet(gcs_config):
    fnrun = tdfnrun(
        input_new_gcs(
            bucket_uri=f"gs://{gcs_config['BUCKET']}",
            gcs_credentials=td.GCPServiceAccountKeyCredentials(
                td.EnvironmentSecret(gcs_config["ENV"])
            ),
            path="testing_resources/data.parquet",
        ),
    )
    assert fnrun.status == 0

    output_file = fnrun.output[0].path
    assert os.path.exists(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_gcs",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.gcs
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_input_new_gcs_ndjson(gcs_config):
    fnrun = tdfnrun(
        input_new_gcs(
            bucket_uri=f"gs://{gcs_config['BUCKET']}",
            gcs_credentials=td.GCPServiceAccountKeyCredentials(
                td.EnvironmentSecret(gcs_config["ENV"])
            ),
            path="testing_resources/data.ndjson",
        ),
    )
    assert fnrun.status == 0

    output_file = fnrun.output[0].path
    assert os.path.exists(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_gcs",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)
