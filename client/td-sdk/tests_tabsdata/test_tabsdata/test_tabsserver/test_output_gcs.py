#
# Copyright 2025 Tabs Data Inc.
#

import copy
import datetime
import inspect
import logging
import os
from io import StringIO
from unittest import mock

import polars as pl
import pytest

import tabsdata as td
from tabsdata._tabsserver.function.response_utils import RESPONSE_FILE_NAME
from tabsdata._tabsserver.invoker import REQUEST_FILE_NAME
from tabsdata._tabsserver.invoker import invoke as tabsserver_main
from tabsdata._utils.bundle_utils import create_bundle_archive
from tests_tabsdata.bootest import TDLOCAL_FOLDER
from tests_tabsdata.conftest import (
    ABSOLUTE_TEST_FOLDER_LOCATION,
    FUNCTION_DATA_FOLDER,
    LOCAL_PACKAGES_LIST,
    PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
    TESTING_RESOURCES_FOLDER,
    clean_polars_df,
    read_json_and_clean,
    write_v2_yaml_file,
)
from tests_tabsdata.testing_resources.test_output_gcs.example import (
    output_gcs as output_gcs_format_testing,
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


@pytest.mark.gcs
@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_gcs_parquet(tmp_path, gcs_config, gcs_client):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
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
    context_archive = create_bundle_archive(
        output_gcs_parquet,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_gcs", "mock_table.parquet"
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
        function_data_path=function_data_folder,
    )
    tabsserver_output_folder = os.path.join(tmp_path, "tabsserver_output")
    os.makedirs(tabsserver_output_folder, exist_ok=True)
    try:
        environment_name, result = tabsserver_main(
            tmp_path,
            response_folder,
            tabsserver_output_folder,
            environment_prefix=PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
            logs_folder=logs_folder,
            temp_cwd=True,
        )
        assert result == 0
        assert os.path.exists(os.path.join(response_folder, RESPONSE_FILE_NAME))

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
@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_gcs_csv(tmp_path, gcs_config, gcs_client):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
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
    # ToDo: Undo when https://github.com/pola-rs/polars/issues/21802 fix is available
    output_gcs_csv.output.format = td.CSVFormat(
        eol_char="\n", separator=",", output_float_precision=4
    )
    context_archive = create_bundle_archive(
        output_gcs_csv,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_gcs", "mock_table.parquet"
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
        function_data_path=function_data_folder,
    )

    try:
        tabsserver_output_folder = os.path.join(tmp_path, "tabsserver_output")
        os.makedirs(tabsserver_output_folder, exist_ok=True)
        environment_name, result = tabsserver_main(
            tmp_path,
            response_folder,
            tabsserver_output_folder,
            environment_prefix=PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
            logs_folder=logs_folder,
            temp_cwd=True,
        )
        assert result == 0
        assert os.path.exists(os.path.join(response_folder, RESPONSE_FILE_NAME))

        bucket = gcs_client.bucket(container_name)
        blob = bucket.blob(blob_name)
        local_file_path = os.path.join(tmp_path, "output.parquet")
        with open(local_file_path, "wb") as download_file:
            download_file.write(blob.download_as_bytes())
        # ToDo: Undo when https://github.com/pola-rs/polars/issues/21802 fix is available
        output = pl.read_csv(local_file_path, separator=",", eol_char="\n")
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
@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_gcs_ndjson(tmp_path, gcs_config, gcs_client):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
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
    context_archive = create_bundle_archive(
        output_gcs_ndjson,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_gcs", "mock_table.parquet"
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
        function_data_path=function_data_folder,
    )
    tabsserver_output_folder = os.path.join(tmp_path, "tabsserver_output")
    os.makedirs(tabsserver_output_folder, exist_ok=True)
    try:
        environment_name, result = tabsserver_main(
            tmp_path,
            response_folder,
            tabsserver_output_folder,
            environment_prefix=PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
            logs_folder=logs_folder,
            temp_cwd=True,
        )
        assert result == 0
        assert os.path.exists(os.path.join(response_folder, RESPONSE_FILE_NAME))

        bucket = gcs_client.bucket(container_name)
        blob = bucket.blob(blob_name)
        local_file_path = os.path.join(tmp_path, "output.parquet")
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
