#
# Copyright 2025 Tabs Data Inc.
#

import copy
import datetime
import inspect
import logging
import os

import polars as pl
import pytest
from tests_tabsdata.conftest import (
    ABSOLUTE_TEST_FOLDER_LOCATION,
    FAKE_SCHEDULED_TIME,
    FAKE_TRIGGERED_TIME,
    LOCAL_PACKAGES_LIST,
    PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
    TESTING_RESOURCES_FOLDER,
    clean_polars_df,
    read_json_and_clean,
    write_v1_yaml_file,
)
from tests_tabsdata.testing_resources.test_output_s3.example import (
    output_s3 as output_s3_format_testing,
)

import tabsdata as td
from tabsdata.utils.bundle_utils import create_bundle_archive
from tabsserver.function_execution.response_utils import RESPONSE_FILE_NAME
from tabsserver.main import EXECUTION_CONTEXT_FILE_NAME
from tabsserver.main import do as tabsserver_main

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ROOT_PROJECT_DIR = os.path.dirname(
    os.path.dirname(os.path.dirname(ABSOLUTE_TEST_FOLDER_LOCATION))
)
RESPONSE_FOLDER = "response_folder"

LOCAL_DEV_FOLDER = os.path.join(
    os.path.dirname(ABSOLUTE_TEST_FOLDER_LOCATION), "local_dev"
)


@pytest.mark.requires_internet
@pytest.mark.slow
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

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_s3", "mock_table.parquet"
    )
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
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

        temporary_output_file = os.path.join(tabsserver_output_folder, "0.parquet")
        assert os.path.isfile(temporary_output_file)
        output = pl.read_parquet(temporary_output_file)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_s3",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)

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
def test_output_s3_parquet_with_data_version(tmp_path, s3_client):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    output_s3_parquet_with_data_version = copy.deepcopy(output_s3_format_testing)
    output_file = (
        "s3://tabsdata-testing-bucket/testing_output/test_output_s3_parquet_with_data_"
        f"version_{int(datetime.datetime.now().timestamp())}_$DATA_VERSION.parquet"
    )
    output_s3_parquet_with_data_version.output.uri = output_file
    context_archive = create_bundle_archive(
        output_s3_parquet_with_data_version,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )
    output_file = output_file.replace("$DATA_VERSION", "fake_dataset_version")

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_s3", "mock_table.parquet"
    )
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
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

        temporary_output_file = os.path.join(tabsserver_output_folder, "0.parquet")
        assert os.path.isfile(temporary_output_file)
        output = pl.read_parquet(temporary_output_file)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_s3",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)

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

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_s3", "mock_table.parquet"
    )
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
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

        temporary_output_file = os.path.join(tabsserver_output_folder, "0.parquet")
        assert os.path.isfile(temporary_output_file)
        output = pl.read_parquet(temporary_output_file)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_s3",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)

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

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_s3", "mock_table.parquet"
    )
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
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

        temporary_output_file = os.path.join(tabsserver_output_folder, "0.parquet")
        assert os.path.isfile(temporary_output_file)
        output = pl.read_parquet(temporary_output_file)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_s3",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)

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

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_s3", "mock_table.parquet"
    )
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
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

        temporary_output_file = os.path.join(tabsserver_output_folder, "0.parquet")
        assert os.path.isfile(temporary_output_file)
        output = pl.read_parquet(temporary_output_file)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_s3",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)

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
def test_output_s3_csv(tmp_path, s3_client):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    output_s3_csv = copy.deepcopy(output_s3_format_testing)
    output_file = (
        "s3://tabsdata-testing-bucket/testing_output/test_output_s3_csv_"
        f"{int(datetime.datetime.now().timestamp())}.csv"
    )
    output_s3_csv.output.uri = output_file
    output_s3_csv.output.format = td.CSVFormat(
        eol_char="\t", separator="|", output_float_precision=4
    )
    context_archive = create_bundle_archive(
        output_s3_csv,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_s3", "mock_table.parquet"
    )
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
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

        temporary_output_file = os.path.join(tabsserver_output_folder, "0.csv")
        assert os.path.isfile(temporary_output_file)
        output = pl.read_csv(temporary_output_file, separator="|", eol_char="\t")
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_s3",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)

        copy_destination = os.path.join(tmp_path, "output.csv")
        s3_client.download_file(bucket_name, file_name, copy_destination)
        output = pl.read_csv(copy_destination, separator="|", eol_char="\t")
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

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_s3", "mock_table.parquet"
    )
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
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

        temporary_output_file = os.path.join(tabsserver_output_folder, "0.ndjson")
        assert os.path.isfile(temporary_output_file)
        output = pl.read_ndjson(temporary_output_file)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_s3",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)

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
