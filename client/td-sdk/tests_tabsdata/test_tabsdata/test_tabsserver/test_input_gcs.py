#
# Copyright 2025 Tabs Data Inc.
#

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
from tests_tabsdata.testing_resources.test_input_gcs_csv.example import (
    input_gcs_csv,
)
from tests_tabsdata.testing_resources.test_input_gcs_log.example import (
    input_gcs_log,
)
from tests_tabsdata.testing_resources.test_input_gcs_ndjson.example import (
    input_gcs_ndjson,
)
from tests_tabsdata.testing_resources.test_input_gcs_parquet.example import (
    input_gcs_parquet,
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
def test_input_gcs_csv(tmp_path, gcs_config):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    bucket_name = gcs_config["BUCKET"]
    input_gcs_csv.input.uri = input_gcs_csv.input.uri.replace(
        "BUCKET_NAME", bucket_name
    )
    input_gcs_csv.input.credentials = td.GCPServiceAccountKeyCredentials(
        td.EnvironmentSecret(gcs_config["ENV"])
    )
    context_archive = create_bundle_archive(
        input_gcs_csv, local_packages=LOCAL_PACKAGES_LIST, save_location=tmp_path
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file = os.path.join(tmp_path, "output.parquet")
    path_to_output_initial_values = os.path.join(tmp_path, "initial_values.parquet")
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output_file],
        output_initial_values_path=path_to_output_initial_values,
        function_data_path=function_data_folder,
    )
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
    assert os.path.isfile(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_gcs_csv",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)
    assert not os.path.isfile(path_to_output_initial_values)


@pytest.mark.gcs
@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_input_gcs_log(tmp_path, gcs_config):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    bucket_name = gcs_config["BUCKET"]
    input_gcs_log.input.uri = input_gcs_log.input.uri.replace(
        "BUCKET_NAME", bucket_name
    )
    input_gcs_log.input.credentials = td.GCPServiceAccountKeyCredentials(
        td.EnvironmentSecret(gcs_config["ENV"])
    )
    context_archive = create_bundle_archive(
        input_gcs_log, local_packages=LOCAL_PACKAGES_LIST, save_location=tmp_path
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file = os.path.join(tmp_path, "output.parquet")
    path_to_output_initial_values = os.path.join(tmp_path, "initial_values.parquet")
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output_file],
        output_initial_values_path=path_to_output_initial_values,
        function_data_path=function_data_folder,
    )
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
    assert os.path.isfile(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_gcs_log",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)
    assert not os.path.isfile(path_to_output_initial_values)


@pytest.mark.gcs
@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_input_gcs_parquet(tmp_path, gcs_config):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    bucket_name = gcs_config["BUCKET"]
    input_gcs_parquet.input.uri = input_gcs_parquet.input.uri.replace(
        "BUCKET_NAME", bucket_name
    )
    input_gcs_parquet.input.credentials = td.GCPServiceAccountKeyCredentials(
        td.EnvironmentSecret(gcs_config["ENV"])
    )
    context_archive = create_bundle_archive(
        input_gcs_parquet, local_packages=LOCAL_PACKAGES_LIST, save_location=tmp_path
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file = os.path.join(tmp_path, "output.parquet")
    path_to_output_initial_values = os.path.join(tmp_path, "initial_values.parquet")
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output_file],
        output_initial_values_path=path_to_output_initial_values,
        function_data_path=function_data_folder,
    )
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
    assert os.path.isfile(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_gcs_parquet",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)
    assert not os.path.isfile(path_to_output_initial_values)


@pytest.mark.gcs
@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_input_gcs_ndjson(tmp_path, gcs_config):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    bucket_name = gcs_config["BUCKET"]
    input_gcs_ndjson.input.uri = input_gcs_ndjson.input.uri.replace(
        "BUCKET_NAME", bucket_name
    )
    input_gcs_ndjson.input.credentials = td.GCPServiceAccountKeyCredentials(
        td.EnvironmentSecret(gcs_config["ENV"])
    )
    context_archive = create_bundle_archive(
        input_gcs_ndjson, local_packages=LOCAL_PACKAGES_LIST, save_location=tmp_path
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file = os.path.join(
        tmp_path,
        "output.parquet",
    )
    path_to_output_initial_values = os.path.join(tmp_path, "initial_values.parquet")
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output_file],
        output_initial_values_path=path_to_output_initial_values,
        function_data_path=function_data_folder,
    )
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
    assert os.path.isfile(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_gcs_ndjson",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)
    assert not os.path.isfile(path_to_output_initial_values)
