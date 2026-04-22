#
# Copyright 2026 Tabs Data Inc.
#

import inspect
import logging
import os
from io import StringIO
from unittest import mock

import polars as pl
import pytest

import tabsdata as td
from test_tabsdata.conftest import (
    TESTING_RESOURCES_FOLDER,
    clean_polars_df,
    read_json_and_clean,
)
from test_tabsdata.fntest import tdfnrun

from test_tabsdata.test_input_new_s3.example import (
    testing_tabsdata_function as input_new_s3,
)
from test_tabsdata.test_input_new_s3_compressed.example import (
    testing_tabsdata_function as input_new_s3_compressed,
)
from test_tabsdata.test_input_new_s3_select_datetime.example import (
    testing_tabsdata_function as input_new_s3_select_datetime,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.s3
@pytest.mark.slow
@pytest.mark.requires_internet
@pytest.mark.tdfnrun
def test_input_new_s3(tmp_path, s3_config):
    fnrun = tdfnrun(
        input_new_s3(
            bucket_uri=s3_config["URI"],
            credentials=s3_config["CREDENTIALS"],
            region=s3_config["REGION"],
            path="testing_nested_import/data.csv",
        ),
    )
    assert fnrun.status == 0

    output_file = fnrun.output[0].path
    assert os.path.exists(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_s3",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.s3
@pytest.mark.slow
@pytest.mark.requires_internet
@pytest.mark.tdfnrun
def test_input_new_s3_infer_schema_length(tmp_path, s3_config):
    fnrun = tdfnrun(
        input_new_s3(
            bucket_uri=s3_config["URI"],
            credentials=s3_config["CREDENTIALS"],
            region=s3_config["REGION"],
            path="testing_nested_import/data.csv",
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
        "test_input_new_s3",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.s3
@pytest.mark.slow
@pytest.mark.requires_internet
@pytest.mark.tdfnrun
def test_input_new_s3_compressed(tmp_path, s3_config):
    fnrun = tdfnrun(
        input_new_s3_compressed(
            bucket_uri=s3_config["URI"],
            credentials=s3_config["CREDENTIALS"],
            region=s3_config["REGION"],
            path="testing_compressed/*",
            format=td.CSVFormat(),
        ),
    )
    assert fnrun.status == 0

    output_file = fnrun.output[0].path
    assert os.path.exists(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_s3_compressed",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.s3
@pytest.mark.slow
@pytest.mark.requires_internet
@pytest.mark.tdfnrun
def test_input_new_s3_parquet(tmp_path, s3_config):
    fnrun = tdfnrun(
        input_new_s3(
            bucket_uri=s3_config["URI"],
            credentials=s3_config["CREDENTIALS"],
            region=s3_config["REGION"],
            path="testing_formats/data.parquet",
        ),
    )
    assert fnrun.status == 0

    output_file = fnrun.output[0].path
    assert os.path.exists(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_s3",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.s3
@pytest.mark.slow
@pytest.mark.requires_internet
@pytest.mark.tdfnrun
def test_input_new_s3_ndjson(tmp_path, s3_config):
    fnrun = tdfnrun(
        input_new_s3(
            bucket_uri=s3_config["URI"],
            credentials=s3_config["CREDENTIALS"],
            region=s3_config["REGION"],
            path="testing_formats/data.ndjson",
        ),
    )
    assert fnrun.status == 0

    output_file = fnrun.output[0].path
    assert os.path.exists(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_s3",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.s3
@pytest.mark.slow
@pytest.mark.requires_internet
@pytest.mark.tdfnrun
def test_input_new_s3_avro(tmp_path, s3_config):
    fnrun = tdfnrun(
        input_new_s3(
            bucket_uri=s3_config["URI"],
            credentials=s3_config["CREDENTIALS"],
            region=s3_config["REGION"],
            path="testing_formats/data.avro",
        ),
    )
    assert fnrun.status == 0

    output_file = fnrun.output[0].path
    assert os.path.exists(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_s3",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.s3
@pytest.mark.slow
@pytest.mark.requires_internet
@pytest.mark.tdfnrun
def test_input_new_s3_csv(tmp_path, s3_config):
    fnrun = tdfnrun(
        input_new_s3(
            bucket_uri=s3_config["URI"],
            credentials=s3_config["CREDENTIALS"],
            region=s3_config["REGION"],
            path="data_no_extension",
            format="csv",
        ),
    )
    assert fnrun.status == 0

    output_file = fnrun.output[0].path
    assert os.path.exists(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_s3",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.s3
@pytest.mark.slow
@pytest.mark.requires_internet
@pytest.mark.tdfnrun
def test_input_new_s3_csv_object(tmp_path, s3_config):
    fnrun = tdfnrun(
        input_new_s3(
            bucket_uri=s3_config["URI"],
            credentials=s3_config["CREDENTIALS"],
            region=s3_config["REGION"],
            path="data_no_extension",
            format=td.CSVFormat(),
        ),
    )
    assert fnrun.status == 0

    output_file = fnrun.output[0].path
    assert os.path.exists(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_s3",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.s3
@pytest.mark.slow
@pytest.mark.requires_internet
@pytest.mark.tdfnrun
def test_input_new_s3_datetime(s3_config):
    fnrun = tdfnrun(
        input_new_s3_select_datetime(
            bucket_uri=s3_config["URI"],
            credentials=s3_config["CREDENTIALS"],
            region=s3_config["REGION"],
            path="testing_nested_import/*.csv",
            initial_last_modified="2024-09-05T01:01:00.01Z",
        ),
        output_initial_values=True,
    )
    assert fnrun.status == 0

    output_file = fnrun.output[0].path
    assert os.path.exists(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_s3_select_datetime",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    initial_values_file = fnrun.offset
    assert os.path.exists(initial_values_file)
    output_initial_values = pl.read_parquet(initial_values_file)
    assert "last_modified" in output_initial_values.columns
    assert output_initial_values["last_modified"].len() == 1

    fnrun = tdfnrun(
        input_new_s3_select_datetime(
            bucket_uri=s3_config["URI"],
            credentials=s3_config["CREDENTIALS"],
            region=s3_config["REGION"],
            path="testing_nested_import/*.csv",
            initial_last_modified="2024-09-05T01:01:00.01Z",
        ),
        input_initial_values=initial_values_file,
        output_initial_values=True,
    )
    assert fnrun.status == 0
    output_file = fnrun.output[0].path
    assert os.path.exists(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_s3_select_datetime",
        "expected_result_second_run.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.s3
@pytest.mark.slow
@pytest.mark.requires_internet
@pytest.mark.tdfnrun
def test_input_new_s3_file_info_name(tmp_path, s3_config):
    fnrun = tdfnrun(
        input_new_s3(
            bucket_uri=s3_config["URI"],
            credentials=s3_config["CREDENTIALS"],
            region=s3_config["REGION"],
            path="testing_nested_import/data.csv",
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
        "test_input_new_s3",
        "expected_result_file_info_name.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    expected_output = expected_output.cast({"@td.origin.file.line": pl.UInt32})
    assert output.equals(expected_output)


@pytest.mark.s3
@pytest.mark.slow
@pytest.mark.requires_internet
@pytest.mark.tdfnrun
def test_input_new_s3_file_info_uri(tmp_path, s3_config):
    fnrun = tdfnrun(
        input_new_s3(
            bucket_uri=s3_config["URI"],
            credentials=s3_config["CREDENTIALS"],
            region=s3_config["REGION"],
            path="testing_nested_import/data.csv",
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
        "test_input_new_s3",
        "expected_result_file_info_uri.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    expected_output = expected_output.cast({"@td.origin.file.line": pl.UInt32})
    assert output.equals(expected_output)
