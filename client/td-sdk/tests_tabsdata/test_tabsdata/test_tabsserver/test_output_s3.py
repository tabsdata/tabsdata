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
    FAKE_EXECUTION_ID,
    FAKE_FUNCTION_RUN_ID,
    FAKE_SCHEDULED_TIME,
    FAKE_TRANSACTION_ID,
    FAKE_TRIGGERED_TIME,
    FUNCTION_DATA_FOLDER,
    LOCAL_PACKAGES_LIST,
    PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
    TESTING_RESOURCES_FOLDER,
    clean_polars_df,
    read_json_and_clean,
    write_v2_yaml_file,
)
from tests_tabsdata.testing_resources.test_output_s3.example import (
    output_s3 as output_s3_format_testing,
)
from tests_tabsdata.testing_resources.test_output_s3_frame_list.example import (
    output_s3_frame_list,
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


@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_s3_parquet(tmp_path, s3_client):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    output_s3_parquet = copy.deepcopy(output_s3_format_testing)
    output_file = (
        "s3://tabsdata-testing-bucket/testing_output/test_output_s3_parquet_"
        f"{int(datetime.datetime.now().timestamp())}.parquet"
    )
    output_s3_parquet.output.uri = output_file
    context_archive = create_bundle_archive(
        output_s3_parquet,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_s3", "mock_table.parquet"
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
        function_data_path=function_data_folder,
    )
    tabsserver_output_folder = os.path.join(tmp_path, "tabsserver_output")
    bucket_name = output_file.split("/")[2]
    file_name = "/".join(output_file.split("/")[3:])
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

        copy_destination = os.path.join(tmp_path, "output.parquet")
        s3_client.download_file(bucket_name, file_name, copy_destination)
        output = pl.read_parquet(copy_destination)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_s3",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        # Clean up the S3 bucket
        s3_client.delete_object(Bucket=bucket_name, Key=file_name)


@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_s3_parquet_with_transaction_id(tmp_path, s3_client):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    output_s3_parquet_with_data_version = copy.deepcopy(output_s3_format_testing)
    output_file = (
        "s3://tabsdata-testing-bucket/testing_output/test_output_s3_parquet_with_data_"
        f"version_{int(datetime.datetime.now().timestamp())}_$TRANSACTION_ID.parquet"
    )
    output_s3_parquet_with_data_version.output.uri = output_file
    context_archive = create_bundle_archive(
        output_s3_parquet_with_data_version,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )
    output_file = output_file.replace("$TRANSACTION_ID", str(FAKE_TRANSACTION_ID))

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_s3", "mock_table.parquet"
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
        function_data_path=function_data_folder,
    )
    tabsserver_output_folder = os.path.join(tmp_path, "tabsserver_output")
    bucket_name = output_file.split("/")[2]
    file_name = "/".join(output_file.split("/")[3:])
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

        copy_destination = os.path.join(tmp_path, "output.parquet")
        s3_client.download_file(bucket_name, file_name, copy_destination)
        output = pl.read_parquet(copy_destination)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_s3",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        # Clean up the S3 bucket
        s3_client.delete_object(Bucket=bucket_name, Key=file_name)


@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_s3_parquet_with_function_run_id(tmp_path, s3_client):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    output_s3_parquet_with_data_version = copy.deepcopy(output_s3_format_testing)
    output_file = (
        "s3://tabsdata-testing-bucket/testing_output/test_output_s3_parquet_with_data_"
        f"version_{int(datetime.datetime.now().timestamp())}_$FUNCTION_RUN_ID.parquet"
    )
    output_s3_parquet_with_data_version.output.uri = output_file
    context_archive = create_bundle_archive(
        output_s3_parquet_with_data_version,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )
    output_file = output_file.replace("$FUNCTION_RUN_ID", str(FAKE_FUNCTION_RUN_ID))

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_s3", "mock_table.parquet"
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
        function_data_path=function_data_folder,
    )
    tabsserver_output_folder = os.path.join(tmp_path, "tabsserver_output")
    bucket_name = output_file.split("/")[2]
    file_name = "/".join(output_file.split("/")[3:])
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

        copy_destination = os.path.join(tmp_path, "output.parquet")
        s3_client.download_file(bucket_name, file_name, copy_destination)
        output = pl.read_parquet(copy_destination)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_s3",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        # Clean up the S3 bucket
        s3_client.delete_object(Bucket=bucket_name, Key=file_name)


@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_s3_parquet_with_execution_id(tmp_path, s3_client):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    output_s3_parquet_with_data_version = copy.deepcopy(output_s3_format_testing)
    output_file = (
        "s3://tabsdata-testing-bucket/testing_output/test_output_s3_parquet_with_data_"
        f"version_{int(datetime.datetime.now().timestamp())}_$EXECUTION_ID.parquet"
    )
    output_s3_parquet_with_data_version.output.uri = output_file
    context_archive = create_bundle_archive(
        output_s3_parquet_with_data_version,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )
    output_file = output_file.replace("$EXECUTION_ID", str(FAKE_EXECUTION_ID))

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_s3", "mock_table.parquet"
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
        function_data_path=function_data_folder,
    )
    tabsserver_output_folder = os.path.join(tmp_path, "tabsserver_output")
    bucket_name = output_file.split("/")[2]
    file_name = "/".join(output_file.split("/")[3:])
    try:
        environment_name, result = tabsserver_main(
            tmp_path,
            response_folder,
            tabsserver_output_folder,
            environment_prefix=PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
            logs_folder=logs_folder,
        )
        assert result == 0
        assert os.path.exists(os.path.join(response_folder, RESPONSE_FILE_NAME))

        copy_destination = os.path.join(tmp_path, "output.parquet")
        s3_client.download_file(bucket_name, file_name, copy_destination)
        output = pl.read_parquet(copy_destination)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_s3",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        # Clean up the S3 bucket
        s3_client.delete_object(Bucket=bucket_name, Key=file_name)


@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_s3_parquet_with_export_timestamp(tmp_path, s3_client):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    output_s3_parquet_with_export_timestamp = copy.deepcopy(output_s3_format_testing)
    output_file = (
        "s3://tabsdata-testing-bucket/testing_output"
        "/test_output_s3_parquet_with_export_timestamp_"
        f"{int(datetime.datetime.now().timestamp())}_$EXPORT_TIMESTAMP.parquet"
    )
    output_s3_parquet_with_export_timestamp.output.uri = output_file
    context_archive = create_bundle_archive(
        output_s3_parquet_with_export_timestamp,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_s3", "mock_table.parquet"
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
        function_data_path=function_data_folder,
    )
    tabsserver_output_folder = os.path.join(tmp_path, "tabsserver_output")
    bucket_name = output_file.split("/")[2]
    file_name = None
    try:
        environment_name, result = tabsserver_main(
            tmp_path,
            response_folder,
            tabsserver_output_folder,
            environment_prefix=PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
            logs_folder=logs_folder,
        )
        assert result == 0
        assert os.path.exists(os.path.join(response_folder, RESPONSE_FILE_NAME))

        response = s3_client.list_objects_v2(Bucket=bucket_name)
        # Check if the bucket contains any files
        if "Contents" in response:
            for obj in response["Contents"]:
                if obj["Key"].startswith(
                    "testing_output/test_output_s3_parquet_with_export_timestamp_"
                ):
                    file_name = obj["Key"]
                    break
        assert file_name is not None
        copy_destination = os.path.join(tmp_path, "output.parquet")
        s3_client.download_file(bucket_name, file_name, copy_destination)
        output = pl.read_parquet(copy_destination)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_s3",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        # Clean up the S3 bucket
        if file_name is not None:
            s3_client.delete_object(Bucket=bucket_name, Key=file_name)


@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_s3_parquet_with_trigger_timestamp(tmp_path, s3_client):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    output_s3_parquet_with_trigger_timestamp = copy.deepcopy(output_s3_format_testing)
    output_file = (
        "s3://tabsdata-testing-bucket/testing_output"
        "/test_output_s3_parquet_with_trigger_timestamp_"
        f"{int(datetime.datetime.now().timestamp())}_$TRIGGER_TIMESTAMP.parquet"
    )
    output_s3_parquet_with_trigger_timestamp.output.uri = output_file
    context_archive = create_bundle_archive(
        output_s3_parquet_with_trigger_timestamp,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_s3", "mock_table.parquet"
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
        function_data_path=function_data_folder,
    )
    tabsserver_output_folder = os.path.join(tmp_path, "tabsserver_output")
    bucket_name = output_file.split("/")[2]
    file_name = None
    try:
        environment_name, result = tabsserver_main(
            tmp_path,
            response_folder,
            tabsserver_output_folder,
            environment_prefix=PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
            logs_folder=logs_folder,
        )
        assert result == 0
        assert os.path.exists(os.path.join(response_folder, RESPONSE_FILE_NAME))

        response = s3_client.list_objects_v2(Bucket=bucket_name)
        # Check if the bucket contains any files
        if "Contents" in response:
            for obj in response["Contents"]:
                if obj["Key"].startswith(
                    "testing_output/test_output_s3_parquet_with_trigger_timestamp_"
                ):
                    file_name = obj["Key"]
                    break
        assert file_name is not None
        assert str(FAKE_TRIGGERED_TIME) in file_name
        copy_destination = os.path.join(tmp_path, "output.parquet")
        s3_client.download_file(bucket_name, file_name, copy_destination)
        output = pl.read_parquet(copy_destination)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_s3",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        # Clean up the S3 bucket
        if file_name is not None:
            s3_client.delete_object(Bucket=bucket_name, Key=file_name)


@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_s3_parquet_with_scheduler_timestamp(tmp_path, s3_client):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    output_s3_parquet_with_scheduler_timestamp = copy.deepcopy(output_s3_format_testing)
    output_file = (
        "s3://tabsdata-testing-bucket/testing_output"
        "/test_output_s3_parquet_with_scheduler_timestamp_"
        f"{int(datetime.datetime.now().timestamp())}_$SCHEDULER_TIMESTAMP.parquet"
    )
    output_s3_parquet_with_scheduler_timestamp.output.uri = output_file
    context_archive = create_bundle_archive(
        output_s3_parquet_with_scheduler_timestamp,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_s3", "mock_table.parquet"
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
        function_data_path=function_data_folder,
    )
    tabsserver_output_folder = os.path.join(tmp_path, "tabsserver_output")
    bucket_name = output_file.split("/")[2]
    file_name = None
    try:
        environment_name, result = tabsserver_main(
            tmp_path,
            response_folder,
            tabsserver_output_folder,
            environment_prefix=PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
            logs_folder=logs_folder,
        )
        assert result == 0
        assert os.path.exists(os.path.join(response_folder, RESPONSE_FILE_NAME))

        response = s3_client.list_objects_v2(Bucket=bucket_name)
        # Check if the bucket contains any files
        if "Contents" in response:
            for obj in response["Contents"]:
                if obj["Key"].startswith(
                    "testing_output/test_output_s3_parquet_with_scheduler_timestamp_"
                ):
                    file_name = obj["Key"]
                    break
        assert file_name is not None
        assert str(FAKE_SCHEDULED_TIME) in file_name
        copy_destination = os.path.join(tmp_path, "output.parquet")
        s3_client.download_file(bucket_name, file_name, copy_destination)
        output = pl.read_parquet(copy_destination)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_s3",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        # Clean up the S3 bucket
        if file_name is not None:
            s3_client.delete_object(Bucket=bucket_name, Key=file_name)


@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_s3_csv(tmp_path, s3_client):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    output_s3_csv = copy.deepcopy(output_s3_format_testing)
    output_file = (
        "s3://tabsdata-testing-bucket/testing_output/test_output_s3_csv_"
        f"{int(datetime.datetime.now().timestamp())}.csv"
    )
    output_s3_csv.output.uri = output_file
    output_s3_csv.output.format = td.CSVFormat(
        # ToDo: Undo when https://github.com/pola-rs/polars/issues/21802 fix is available
        eol_char="\n",
        separator=",",
        output_float_precision=4,
    )
    context_archive = create_bundle_archive(
        output_s3_csv,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_s3", "mock_table.parquet"
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
        function_data_path=function_data_folder,
    )
    bucket_name = output_file.split("/")[2]
    file_name = "/".join(output_file.split("/")[3:])
    try:
        tabsserver_output_folder = os.path.join(tmp_path, "tabsserver_output")
        environment_name, result = tabsserver_main(
            tmp_path,
            response_folder,
            tabsserver_output_folder,
            environment_prefix=PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
            logs_folder=logs_folder,
        )
        assert result == 0
        assert os.path.exists(os.path.join(response_folder, RESPONSE_FILE_NAME))

        copy_destination = os.path.join(tmp_path, "output.csv")
        s3_client.download_file(bucket_name, file_name, copy_destination)
        # ToDo: Undo when https://github.com/pola-rs/polars/issues/21802 fix is available
        output = pl.read_csv(copy_destination, separator=",", eol_char="\n")
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_s3",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        # Clean up the S3 bucket
        s3_client.delete_object(Bucket=bucket_name, Key=file_name)


@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_s3_ndjson(tmp_path, s3_client):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    output_s3_ndjson = copy.deepcopy(output_s3_format_testing)
    output_file = (
        "s3://tabsdata-testing-bucket/testing_output/test_output_s3_ndjson_"
        f"{int(datetime.datetime.now().timestamp())}.ndjson"
    )
    output_s3_ndjson.output.uri = output_file
    context_archive = create_bundle_archive(
        output_s3_ndjson,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_s3", "mock_table.parquet"
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
        function_data_path=function_data_folder,
    )
    tabsserver_output_folder = os.path.join(tmp_path, "tabsserver_output")
    bucket_name = output_file.split("/")[2]
    file_name = "/".join(output_file.split("/")[3:])
    try:
        environment_name, result = tabsserver_main(
            tmp_path,
            response_folder,
            tabsserver_output_folder,
            environment_prefix=PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
            logs_folder=logs_folder,
        )
        assert result == 0
        assert os.path.exists(os.path.join(response_folder, RESPONSE_FILE_NAME))

        copy_destination = os.path.join(tmp_path, "output.ndjson")
        s3_client.download_file(bucket_name, file_name, copy_destination)
        output = pl.read_ndjson(copy_destination)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_s3",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        # Clean up the S3 bucket
        s3_client.delete_object(Bucket=bucket_name, Key=file_name)


@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_s3_frame_list(tmp_path, s3_client):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    output_file = (
        "s3://tabsdata-testing-bucket/testing_output/test_output_s3_frame_list_"
        f"{int(datetime.datetime.now().timestamp())}_$FRAGMENT_IDX.parquet"
    )
    output_s3_frame_list.output.uri = output_file
    context_archive = create_bundle_archive(
        output_s3_frame_list,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_s3_frame_list", "mock_table.parquet"
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
        function_data_path=function_data_folder,
    )
    tabsserver_output_folder = os.path.join(tmp_path, "tabsserver_output")
    bucket_name = output_file.split("/")[2]
    file_name = "/".join(output_file.split("/")[3:])
    file_name_0 = file_name.replace("$FRAGMENT_IDX", "0")
    file_name_1 = file_name.replace("$FRAGMENT_IDX", "1")
    try:
        environment_name, result = tabsserver_main(
            tmp_path,
            response_folder,
            tabsserver_output_folder,
            environment_prefix=PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
            logs_folder=logs_folder,
        )
        assert result == 0
        assert os.path.exists(os.path.join(response_folder, RESPONSE_FILE_NAME))

        copy_destination = os.path.join(tmp_path, "output.parquet")
        s3_client.download_file(bucket_name, file_name_0, copy_destination)
        output = pl.read_parquet(copy_destination)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_s3_frame_list",
            "expected_result_0.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)

        s3_client.download_file(bucket_name, file_name_1, copy_destination)
        output = pl.read_parquet(copy_destination)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_s3_frame_list",
            "expected_result_1.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        # Clean up the S3 bucket
        s3_client.delete_object(Bucket=bucket_name, Key=file_name_0)
        s3_client.delete_object(Bucket=bucket_name, Key=file_name_1)
