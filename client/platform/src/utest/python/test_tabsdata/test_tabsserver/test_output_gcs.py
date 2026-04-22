#
# Copyright 2025 Tabs Data Inc.
#

import copy
import datetime
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
from test_tabsdata.test_output_gcs.example import (
    output_gcs as output_gcs_format_testing,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.gcs
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_gcs_parquet(tmp_path, gcs_config, gcs_client):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_gcs", "mock_table.parquet"
    )
    output_gcs_parquet = copy.deepcopy(output_gcs_format_testing)
    container_name = gcs_config["BUCKET"]
    blob_name = (
        "test_output/test_output_gcs_parquet_"
        f"{int(datetime.datetime.now().timestamp())}.parquet"
    )
    output_file = f"gs://{container_name}/{blob_name}"
    output_gcs_parquet.output.uri = output_file
    output_gcs_parquet.output.credentials = td.GCPServiceAccountKeyCredentials(
        td.EnvironmentSecret(gcs_config["ENV"])
    )
    fnrun = tdfnrun(output_gcs_parquet, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    try:
        bucket = gcs_client.bucket(container_name)
        blob = bucket.blob(blob_name)
        local_file_path = os.path.join(tmp_path, "output.parquet")
        with open(local_file_path, "wb") as download_file:
            download_file.write(blob.download_as_bytes())
        output = pl.read_parquet(local_file_path)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_gcs",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        try:
            bucket = gcs_client.bucket(container_name)
            blob = bucket.blob(blob_name)
            blob.delete()
        except Exception:
            pass


@pytest.mark.gcs
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_gcs_csv(tmp_path, gcs_config, gcs_client):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_gcs", "mock_table.parquet"
    )
    output_gcs_csv = copy.deepcopy(output_gcs_format_testing)
    container_name = gcs_config["BUCKET"]
    blob_name = (
        "test_output/test_output_gcs_csv_"
        f"{int(datetime.datetime.now().timestamp())}.csv"
    )
    output_file = f"gs://{container_name}/{blob_name}"
    output_gcs_csv.output.uri = output_file
    output_gcs_csv.output.credentials = td.GCPServiceAccountKeyCredentials(
        td.EnvironmentSecret(gcs_config["ENV"])
    )
    output_gcs_csv.output.format = td.CSVFormat(output_float_precision=4)
    fnrun = tdfnrun(output_gcs_csv, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    try:
        bucket = gcs_client.bucket(container_name)
        blob = bucket.blob(blob_name)
        local_file_path = os.path.join(tmp_path, "output.csv")
        with open(local_file_path, "wb") as download_file:
            download_file.write(blob.download_as_bytes())
        output = pl.read_csv(local_file_path)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_gcs",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        try:
            bucket = gcs_client.bucket(container_name)
            blob = bucket.blob(blob_name)
            blob.delete()
        except Exception:
            pass


@pytest.mark.gcs
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_gcs_ndjson(tmp_path, gcs_config, gcs_client):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_gcs", "mock_table.parquet"
    )
    output_gcs_ndjson = copy.deepcopy(output_gcs_format_testing)
    container_name = gcs_config["BUCKET"]
    blob_name = (
        "test_output/test_output_gcs_ndjson_"
        f"{int(datetime.datetime.now().timestamp())}.ndjson"
    )
    output_file = f"gs://{container_name}/{blob_name}"
    output_gcs_ndjson.output.uri = output_file
    output_gcs_ndjson.output.credentials = td.GCPServiceAccountKeyCredentials(
        td.EnvironmentSecret(gcs_config["ENV"])
    )
    fnrun = tdfnrun(output_gcs_ndjson, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    try:
        bucket = gcs_client.bucket(container_name)
        blob = bucket.blob(blob_name)
        local_file_path = os.path.join(tmp_path, "output.ndjson")
        with open(local_file_path, "wb") as download_file:
            download_file.write(blob.download_as_bytes())
        output = pl.read_ndjson(local_file_path)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_gcs",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        try:
            bucket = gcs_client.bucket(container_name)
            blob = bucket.blob(blob_name)
            blob.delete()
        except Exception:
            pass
