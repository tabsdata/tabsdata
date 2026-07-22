#
# Copyright 2025 Tabsdata Inc.
#

import copy
import datetime
import logging
import os

import polars as pl
import pytest
from azure.core.exceptions import ResourceNotFoundError

import tabsdata as td
from test_tabsdata.fntest import tdfnrun
from test_tabsdata.conftest import (
    TESTING_RESOURCES_FOLDER,
    clean_polars_df,
    read_json_and_clean,
)
from test_tabsdata.test_output_azure.example import (
    output_azure as output_azure_format_testing,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.azure
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_azure_parquet(tmp_path, azure_client, azure_config):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_azure", "mock_table.parquet"
    )
    output_azure_parquet = copy.deepcopy(output_azure_format_testing)
    container_name = azure_config["CONTAINER_NAME"]
    blob_name = (
        "test_output/test_output_azure_parquet_"
        f"{int(datetime.datetime.now().timestamp())}.parquet"
    )
    output_file = f"{azure_config['URI']}/{blob_name}"
    output_azure_parquet.output.uri = output_file
    output_azure_parquet.output.credentials = azure_config["CREDENTIALS"]
    blob_client = None
    fnrun = tdfnrun(output_azure_parquet, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    try:
        blob_client = azure_client.get_blob_client(
            container=container_name, blob=blob_name
        )
        local_file_path = os.path.join(tmp_path, "output.parquet")
        with open(local_file_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
        output = pl.read_parquet(local_file_path)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_azure",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        if blob_client is not None:
            try:
                blob_client.delete_blob()
            except ResourceNotFoundError as e:
                logger.warning(
                    f"The blob to delete wasn't created or was already deleted: {e}"
                )


@pytest.mark.azure
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_azure_csv(tmp_path, azure_client, azure_config):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_azure", "mock_table.parquet"
    )
    output_azure_csv = copy.deepcopy(output_azure_format_testing)
    container_name = azure_config["CONTAINER_NAME"]
    blob_name = (
        "test_output/test_output_azure_csv_"
        f"{int(datetime.datetime.now().timestamp())}.csv"
    )
    output_file = f"{azure_config['URI']}/{blob_name}"
    output_azure_csv.output.uri = output_file
    output_azure_csv.output.credentials = azure_config["CREDENTIALS"]
    output_azure_csv.output.format = td.CSVFormat(output_float_precision=4)
    blob_client = None
    fnrun = tdfnrun(output_azure_csv, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    try:
        blob_client = azure_client.get_blob_client(
            container=container_name, blob=blob_name
        )
        local_file_path = os.path.join(tmp_path, "output.csv")
        with open(local_file_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
        output = pl.read_csv(local_file_path)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_azure",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        if blob_client is not None:
            try:
                blob_client.delete_blob()
            except ResourceNotFoundError as e:
                logger.warning(
                    f"The blob to delete wasn't created or was already deleted: {e}"
                )


@pytest.mark.azure
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_azure_ndjson(tmp_path, azure_client, azure_config):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_azure", "mock_table.parquet"
    )
    output_azure_ndjson = copy.deepcopy(output_azure_format_testing)
    container_name = azure_config["CONTAINER_NAME"]
    blob_name = (
        "test_output/test_output_azure_ndjson_"
        f"{int(datetime.datetime.now().timestamp())}.ndjson"
    )
    output_file = f"{azure_config['URI']}/{blob_name}"
    output_azure_ndjson.output.credentials = azure_config["CREDENTIALS"]
    output_azure_ndjson.output.uri = output_file
    blob_client = None
    fnrun = tdfnrun(output_azure_ndjson, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    try:
        blob_client = azure_client.get_blob_client(
            container=container_name, blob=blob_name
        )
        local_file_path = os.path.join(tmp_path, "output.ndjson")
        with open(local_file_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
        output = pl.read_ndjson(local_file_path)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_azure",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        if blob_client is not None:
            try:
                blob_client.delete_blob()
            except ResourceNotFoundError as e:
                logger.warning(
                    f"The blob to delete wasn't created or was already deleted: {e}"
                )
