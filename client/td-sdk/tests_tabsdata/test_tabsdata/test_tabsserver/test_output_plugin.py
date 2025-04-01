#
# Copyright 2025 Tabs Data Inc.
#

import inspect
import logging
import os

import polars as pl
import pytest
from tests_tabsdata.bootest import TDLOCAL_FOLDER
from tests_tabsdata.conftest import (
    ABSOLUTE_TEST_FOLDER_LOCATION,
    LOCAL_PACKAGES_LIST,
    PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
    TESTING_RESOURCES_FOLDER,
    clean_polars_df,
    read_json_and_clean,
    write_v1_yaml_file,
)
from tests_tabsdata.testing_resources.test_output_plugin.example import output_plugin
from tests_tabsdata.testing_resources.test_output_plugin_multiple_outputs.example import (
    output_plugin_multiple_outputs,
)
from tests_tabsdata.testing_resources.test_output_plugin_multiple_with_none.example import (
    output_plugin_multiple_outputs_with_none,
)
from tests_tabsdata.testing_resources.test_output_plugin_with_none.example import (
    output_plugin_with_none,
)

from tabsdata.tabsserver.function.response_utils import RESPONSE_FILE_NAME
from tabsdata.tabsserver.invoker import EXECUTION_CONTEXT_FILE_NAME
from tabsdata.tabsserver.invoker import invoke as tabsserver_main
from tabsdata.utils.bundle_utils import create_bundle_archive

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ROOT_PROJECT_DIR = os.path.dirname(
    os.path.dirname(os.path.dirname(ABSOLUTE_TEST_FOLDER_LOCATION))
)
RESPONSE_FOLDER = "response_folder"

LOCAL_DEV_FOLDER = TDLOCAL_FOLDER


@pytest.mark.requires_internet
@pytest.mark.slow
def test_output_plugin(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        output_plugin,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_plugin", "mock_table.parquet"
    )
    write_v1_yaml_file(input_yaml_file, context_archive, [mock_parquet_table])
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

    output_file1 = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_plugin",
        "output.json",
    )
    assert os.path.isfile(output_file1)
    output = pl.read_ndjson(output_file1)
    output = clean_polars_df(output)
    os.remove(output_file1)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_plugin",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.requires_internet
@pytest.mark.slow
def test_output_plugin_multiple_outputs(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    first_output_file = os.path.join(
        tmp_path, "test_output_plugin_multiple_outputs1.ndjson"
    )
    second_output_file = os.path.join(
        tmp_path, "test_output_plugin_multiple_outputs2.ndjson"
    )
    output_plugin_multiple_outputs.output.destination_ndjson_file = first_output_file
    output_plugin_multiple_outputs.output.second_destination_ndjson_file = (
        second_output_file
    )
    context_archive = create_bundle_archive(
        output_plugin_multiple_outputs,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_plugin_multiple_outputs",
        "mock_table.parquet",
    )
    write_v1_yaml_file(input_yaml_file, context_archive, [mock_parquet_table])
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

    assert os.path.isfile(first_output_file)
    output = pl.read_ndjson(first_output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_plugin_multiple_outputs",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    assert os.path.isfile(second_output_file)
    output = pl.read_ndjson(second_output_file)
    output = clean_polars_df(output)
    assert output.equals(expected_output)


@pytest.mark.requires_internet
@pytest.mark.slow
def test_output_plugin_multiple_outputs_with_none(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    first_output_file = os.path.join(
        tmp_path, "test_output_plugin_multiple_outputs_with_none1.ndjson"
    )
    second_output_file = os.path.join(
        tmp_path, "test_output_plugin_multiple_outputs_with_none2.ndjson"
    )
    output_plugin_multiple_outputs_with_none.output.destination_ndjson_file = (
        first_output_file
    )
    output_plugin_multiple_outputs_with_none.output.second_destination_ndjson_file = (
        second_output_file
    )
    context_archive = create_bundle_archive(
        output_plugin_multiple_outputs_with_none,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_plugin_multiple_with_none",
        "mock_table.parquet",
    )
    write_v1_yaml_file(input_yaml_file, context_archive, [mock_parquet_table])
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


@pytest.mark.requires_internet
@pytest.mark.slow
def test_output_plugin_with_none(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        output_plugin_with_none,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_plugin_with_none", "mock_table.parquet"
    )
    write_v1_yaml_file(input_yaml_file, context_archive, [mock_parquet_table])
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
