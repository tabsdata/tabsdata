#
# Copyright 2025 Tabs Data Inc.
#

import inspect
import logging
import os
import urllib.parse

import polars as pl
import pytest

from tabsdata.utils.bundle_utils import create_bundle_archive
from tabsserver.function_execution.response_utils import RESPONSE_FILE_NAME
from tabsserver.main import EXECUTION_CONTEXT_FILE_NAME
from tabsserver.main import do as tabsserver_main
from tests.conftest import (
    ABSOLUTE_TEST_FOLDER_LOCATION,
    DB_HOST,
    DB_NAME,
    DB_PASSWORD,
    DB_USER,
    POSTGRES_PORT,
    PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
    TESTING_RESOURCES_FOLDER,
    clean_polars_df,
    read_json_and_clean,
    write_v1_yaml_file,
)
from tests.testing_resources.test_input_postgres.example import input_postgres
from tests.testing_resources.test_input_postgres_initial_values.example import (
    input_postgres_initial_values,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


ESCAPED_USER = urllib.parse.quote(DB_USER)
ESCAPED_PASSWORD = urllib.parse.quote(DB_PASSWORD)
POSTGRES_URI = (
    f"postgres://{ESCAPED_USER}:{ESCAPED_PASSWORD}@{DB_HOST}:{POSTGRES_PORT}/{DB_NAME}"
)
ROOT_PROJECT_DIR = os.path.dirname(
    os.path.dirname(os.path.dirname(ABSOLUTE_TEST_FOLDER_LOCATION))
)
RESPONSE_FOLDER = "response_folder"

LOCAL_DEV_FOLDER = os.path.join(
    os.path.dirname(ABSOLUTE_TEST_FOLDER_LOCATION), "local_dev"
)


@pytest.mark.postgres
@pytest.mark.requires_internet
@pytest.mark.slow
def test_input_postgres(testing_postgres, tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_postgres, local_packages=ROOT_PROJECT_DIR, save_location=tmp_path
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file1 = os.path.join(tmp_path, "output1.parquet")
    output_file2 = os.path.join(tmp_path, "output2.parquet")
    path_to_output_initial_values = os.path.join(tmp_path, "initial_values.parquet")
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output_file1, output_file2],
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

    assert os.path.isfile(output_file1)
    output = pl.read_parquet(output_file1)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_postgres",
        "expected_result1.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    assert os.path.isfile(output_file2)
    output = pl.read_parquet(output_file2)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_postgres",
        "expected_result2.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)
    assert not os.path.isfile(path_to_output_initial_values)


@pytest.mark.postgres
@pytest.mark.requires_internet
@pytest.mark.slow
def test_input_postgres_initial_values(testing_postgres, tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_postgres_initial_values,
        local_packages=ROOT_PROJECT_DIR,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file1 = os.path.join(tmp_path, "output1.parquet")
    output_file2 = os.path.join(tmp_path, "output2.parquet")
    path_to_output_initial_values = os.path.join(tmp_path, "initial_values.parquet")
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output_file1, output_file2],
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

    assert os.path.isfile(output_file1)
    output = pl.read_parquet(output_file1)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_postgres_initial_values",
        "expected_result1.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    assert os.path.isfile(output_file2)
    output = pl.read_parquet(output_file2)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_postgres_initial_values",
        "expected_result2.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)
    assert os.path.isfile(path_to_output_initial_values)
    output_initial_values = pl.read_parquet(path_to_output_initial_values)
    assert (
        output_initial_values.filter(pl.col("variable") == "number")
        .select("value")
        .item()
    )


@pytest.mark.postgres
@pytest.mark.requires_internet
@pytest.mark.slow
def test_input_postgres_initial_values_stored_number_0(testing_postgres, tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_postgres_initial_values,
        local_packages=ROOT_PROJECT_DIR,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file1 = os.path.join(tmp_path, "output1.parquet")
    output_file2 = os.path.join(tmp_path, "output2.parquet")
    path_to_input_initial_values = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_postgres_initial_values",
        "mock_number_0.parquet",
    )
    path_to_output_initial_values = os.path.join(tmp_path, "initial_values.parquet")
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output_file1, output_file2],
        input_initial_values_path=path_to_input_initial_values,
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

    assert os.path.isfile(output_file1)
    output = pl.read_parquet(output_file1)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_postgres_initial_values",
        "expected_result1_number_0.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    assert os.path.isfile(output_file2)
    output = pl.read_parquet(output_file2)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_postgres_initial_values",
        "expected_result2_number_0.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)
    assert os.path.isfile(path_to_output_initial_values)
    output_initial_values = pl.read_parquet(path_to_output_initial_values)
    assert (
        output_initial_values.filter(pl.col("variable") == "number")
        .select("value")
        .item()
    )


@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.postgres
def test_input_postgres_initial_values_stored_number_2(testing_postgres, tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_postgres_initial_values,
        local_packages=ROOT_PROJECT_DIR,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file1 = os.path.join(tmp_path, "output1.parquet")
    output_file2 = os.path.join(tmp_path, "output2.parquet")
    path_to_input_initial_values = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_postgres_initial_values",
        "mock_number_2.parquet",
    )
    path_to_output_initial_values = os.path.join(tmp_path, "initial_values.parquet")
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output_file1, output_file2],
        input_initial_values_path=path_to_input_initial_values,
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

    assert os.path.isfile(output_file1)
    output = pl.read_parquet(output_file1)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_postgres_initial_values",
        "expected_result1.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    assert os.path.isfile(output_file2)
    output = pl.read_parquet(output_file2)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_postgres_initial_values",
        "expected_result2.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)
    assert os.path.isfile(path_to_output_initial_values)
    output_initial_values = pl.read_parquet(path_to_output_initial_values)
    assert (
        output_initial_values.filter(pl.col("variable") == "number")
        .select("value")
        .item()
    )
