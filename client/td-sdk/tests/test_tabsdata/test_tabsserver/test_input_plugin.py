#
# Copyright 2025 Tabs Data Inc.
#

import inspect
import logging
import os

import polars as pl
import pytest

from tabsdata.utils.bundle_utils import create_bundle_archive
from tabsserver.function_execution.response_utils import RESPONSE_FILE_NAME
from tabsserver.main import EXECUTION_CONTEXT_FILE_NAME
from tabsserver.main import do as tabsserver_main
from tests.conftest import (
    ABSOLUTE_TEST_FOLDER_LOCATION,
    PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
    TESTING_RESOURCES_FOLDER,
    clean_polars_df,
    read_json_and_clean,
    write_v1_yaml_file,
)
from tests.testing_resources.test_input_plugin.example import input_plugin
from tests.testing_resources.test_input_plugin_from_pypi.example import (
    input_plugin_from_pypi,
)
from tests.testing_resources.test_input_plugin_initial_values.example import (
    input_plugin_initial_values,
)
from tests.testing_resources.test_input_plugin_multiple_inputs.example import (
    input_plugin_multiple_inputs,
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


@pytest.mark.requires_internet
@pytest.mark.slow
def test_input_plugin(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_plugin, local_packages=ROOT_PROJECT_DIR, save_location=tmp_path
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
        "test_input_plugin",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.requires_internet
@pytest.mark.slow
def test_input_plugin_multiple_inputs(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_plugin_multiple_inputs,
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
        "test_input_plugin_multiple_inputs",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.requires_internet
@pytest.mark.slow
def test_input_plugin_initial_values(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_plugin_initial_values,
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
        "test_input_plugin_initial_values",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    assert os.path.isfile(path_to_output_initial_values)
    output_initial_values = pl.read_parquet(path_to_output_initial_values)
    assert (
        output_initial_values.filter(pl.col("variable") == "number")
        .select("value")
        .item()
        == "2"
    )


@pytest.mark.requires_internet
@pytest.mark.slow
def test_input_plugin_initial_values_stored_number_2(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_plugin_initial_values,
        local_packages=ROOT_PROJECT_DIR,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file = os.path.join(tmp_path, "output.parquet")
    path_to_input_initial_values = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_plugin_initial_values",
        "mock_number_2.parquet",
    )
    path_to_output_initial_values = os.path.join(tmp_path, "initial_values.parquet")
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output_file],
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

    assert os.path.isfile(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_plugin_initial_values",
        "expected_result_2.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    assert os.path.isfile(path_to_output_initial_values)
    output_initial_values = pl.read_parquet(path_to_output_initial_values)
    assert (
        output_initial_values.filter(pl.col("variable") == "number")
        .select("value")
        .item()
        == "3"
    )


@pytest.mark.requires_internet
@pytest.mark.slow
def test_input_plugin_from_pypi(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_plugin_from_pypi, local_packages=ROOT_PROJECT_DIR, save_location=tmp_path
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
