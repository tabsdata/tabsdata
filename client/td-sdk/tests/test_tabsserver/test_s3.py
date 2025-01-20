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

import tabsdata as td
from tabsdata.utils.bundle_utils import create_bundle_archive
from tabsserver.function_execution.response_utils import RESPONSE_FILE_NAME
from tabsserver.main import EXECUTION_CONTEXT_FILE_NAME
from tabsserver.main import do as tabsserver_main
from tests.conftest import (
    ABSOLUTE_TEST_FOLDER_LOCATION,
    FAKE_SCHEDULED_TIME,
    FAKE_TRIGGERED_TIME,
    PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
    TESTING_RESOURCES_FOLDER,
    clean_polars_df,
    read_json_and_clean,
    write_v1_yaml_file,
)
from tests.testing_resources.test_input_s3.example import input_s3
from tests.testing_resources.test_input_s3_environment_secret.example import (
    input_s3_environment_secret,
)
from tests.testing_resources.test_input_s3_eu_north_region.example import (
    input_s3_eu_north_region,
)
from tests.testing_resources.test_input_s3_explicit_format.example import (
    input_s3_explicit_format,
)
from tests.testing_resources.test_input_s3_explicit_format_object.example import (
    input_s3_explicit_format_object,
)
from tests.testing_resources.test_input_s3_modified_uri.example import (
    input_s3_modified_uri,
)
from tests.testing_resources.test_input_s3_select_datetime.example import (
    input_s3_select_datetime,
)
from tests.testing_resources.test_input_s3_uri_list.example import input_s3_uri_list
from tests.testing_resources.test_input_s3_wildcard.example import input_s3_wildcard
from tests.testing_resources.test_output_s3.example import (
    output_s3 as output_s3_format_testing,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ROOT_PROJECT_DIR = os.path.dirname(
    os.path.dirname(os.path.dirname(ABSOLUTE_TEST_FOLDER_LOCATION))
)
RESPONSE_FOLDER = "response_folder"

LOCAL_DEV_FOLDER = os.path.join(
    os.path.dirname(ABSOLUTE_TEST_FOLDER_LOCATION), "local_dev"
)


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_input_s3(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_s3, local_packages=ROOT_PROJECT_DIR, save_location=tmp_path
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file = os.path.join(tmp_path, "output.parquet")
    path_to_output_initial_values = os.path.join(tmp_path, "initial_values.parquet")
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output_file],
        output_initial_values_path=path_to_output_initial_values,
    )
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
    assert os.path.isfile(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_s3",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)
    assert not os.path.isfile(path_to_output_initial_values)


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_input_s3_eu_north_region(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_s3_eu_north_region,
        local_packages=ROOT_PROJECT_DIR,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file = os.path.join(tmp_path, "output.parquet")
    path_to_output_initial_values = os.path.join(tmp_path, "initial_values.parquet")
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output_file],
        output_initial_values_path=path_to_output_initial_values,
    )
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
    assert os.path.isfile(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_s3_eu_north_region",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)
    assert not os.path.isfile(path_to_output_initial_values)


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_input_s3_environment_secret(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_s3_environment_secret,
        local_packages=ROOT_PROJECT_DIR,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file = os.path.join(tmp_path, "output.parquet")
    write_v1_yaml_file(
        input_yaml_file, context_archive, mock_table_location=[output_file]
    )
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
    assert os.path.isfile(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_s3_environment_secret",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_input_s3_modified_uri(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_s3_modified_uri, local_packages=ROOT_PROJECT_DIR, save_location=tmp_path
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file = os.path.join(tmp_path, "output.parquet")
    write_v1_yaml_file(
        input_yaml_file, context_archive, mock_table_location=[output_file]
    )
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
    assert os.path.isfile(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_s3_modified_uri",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_input_s3_explicit_format(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_s3_explicit_format,
        local_packages=ROOT_PROJECT_DIR,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file = os.path.join(tmp_path, "output.parquet")
    write_v1_yaml_file(
        input_yaml_file, context_archive, mock_table_location=[output_file]
    )
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
    assert os.path.isfile(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_s3_explicit_format",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_input_s3_wildcard(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_s3_wildcard, local_packages=ROOT_PROJECT_DIR, save_location=tmp_path
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file = os.path.join(tmp_path, "output.parquet")
    write_v1_yaml_file(
        input_yaml_file, context_archive, mock_table_location=[output_file]
    )
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
    assert os.path.isfile(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_s3_wildcard",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_input_s3_select_datetime(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_s3_select_datetime,
        local_packages=ROOT_PROJECT_DIR,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file = os.path.join(tmp_path, "output.parquet")
    path_to_output_initial_values = os.path.join(tmp_path, "initial_values.parquet")
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output_file],
        output_initial_values_path=path_to_output_initial_values,
    )
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
    assert os.path.isfile(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_s3_select_datetime",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)
    assert os.path.isfile(path_to_output_initial_values)
    output_initial_values = pl.read_parquet(path_to_output_initial_values)
    assert (
        output_initial_values.filter(pl.col("variable") == "last_modified")
        .select("value")
        .item()
    )


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_input_s3_select_datetime_stored_valid_last_modified(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_s3_select_datetime,
        local_packages=ROOT_PROJECT_DIR,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file = os.path.join(tmp_path, "output.parquet")
    path_to_initial_values = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_s3_select_datetime",
        "mock_valid_date.parquet",
    )
    path_to_output_initial_values = os.path.join(tmp_path, "initial_values.parquet")
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output_file],
        input_initial_values_path=path_to_initial_values,
        output_initial_values_path=path_to_output_initial_values,
    )
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
    assert os.path.isfile(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_s3_select_datetime",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)
    assert os.path.isfile(path_to_output_initial_values)
    output_initial_values = pl.read_parquet(path_to_output_initial_values)
    assert (
        output_initial_values.filter(pl.col("variable") == "last_modified")
        .select("value")
        .item()
    )


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_input_s3_select_datetime_stored_late_last_modified(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_s3_select_datetime,
        local_packages=ROOT_PROJECT_DIR,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file = os.path.join(tmp_path, "output.parquet")
    path_to_initial_values = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_s3_select_datetime",
        "mock_late_date.parquet",
    )
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output_file],
        input_initial_values_path=path_to_initial_values,
    )
    tabsserver_output_folder = os.path.join(tmp_path, "tabsserver_output")
    environment_name, result = tabsserver_main(
        tmp_path,
        response_folder,
        tabsserver_output_folder,
        environment_prefix=PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
        logs_folder=logs_folder,
    )
    assert result != 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_input_s3_uri_list(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_s3_uri_list,
        local_packages=ROOT_PROJECT_DIR,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file1 = os.path.join(tmp_path, "output1.parquet")
    output_file2 = os.path.join(tmp_path, "output2.parquet")
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output_file1, output_file2],
    )
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

    assert os.path.isfile(output_file1)
    output = pl.read_parquet(output_file1)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_s3_uri_list",
        "expected_result1.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    assert os.path.isfile(output_file2)
    output = pl.read_parquet(output_file2)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_s3_uri_list",
        "expected_result2.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_input_s3_explicit_format_object(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_s3_explicit_format_object,
        local_packages=ROOT_PROJECT_DIR,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file = os.path.join(tmp_path, "output.parquet")
    write_v1_yaml_file(
        input_yaml_file, context_archive, mock_table_location=[output_file]
    )
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
    assert os.path.isfile(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_s3_explicit_format_object",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


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
        local_packages=ROOT_PROJECT_DIR,
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
        local_packages=ROOT_PROJECT_DIR,
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
        local_packages=ROOT_PROJECT_DIR,
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
        local_packages=ROOT_PROJECT_DIR,
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
        local_packages=ROOT_PROJECT_DIR,
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
        local_packages=ROOT_PROJECT_DIR,
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
        local_packages=ROOT_PROJECT_DIR,
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
