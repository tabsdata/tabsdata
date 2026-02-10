#
# Copyright 2025 Tabs Data Inc.
#

import inspect
import logging
import os
import urllib.parse
from io import StringIO
from unittest import mock

import polars as pl
import pytest

from tabsdata._tabsserver.function.response_utils import RESPONSE_FILE_NAME
from tabsdata._tabsserver.invoker import REQUEST_FILE_NAME
from tabsdata._tabsserver.invoker import invoke as tabsserver_main
from tabsdata._utils.bundle_utils import create_bundle_archive
from tests_tabsdata.bootest import TDLOCAL_FOLDER
from tests_tabsdata.common.backend import MARIADB_PORT
from tests_tabsdata.common.constants import (
    DB_HOST,
    DB_NAME,
    DB_PASSWORD,
    DB_USER,
    PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
)
from tests_tabsdata.conftest import (
    ABSOLUTE_TEST_FOLDER_LOCATION,
    FUNCTION_DATA_FOLDER,
    LOCAL_PACKAGES_LIST,
    TESTING_RESOURCES_FOLDER,
    clean_polars_df,
    read_json_and_clean,
    write_v2_function_yaml_file,
)
from tests_tabsdata.testing_resources.test_output_mariadb_driver_provided.example import (
    testing_tabsdata_function as output_mariadb_driver_provided,
)
from tests_tabsdata.testing_resources.test_output_mariadb_list.example import (
    testing_tabsdata_function as output_mariadb_list,
)
from tests_tabsdata.testing_resources.test_output_mariadb_list_single_element.example import (
    testing_tabsdata_function as output_mariadb_list_single_element,
)
from tests_tabsdata.testing_resources.test_output_mariadb_transaction.example import (
    testing_tabsdata_function as output_mariadb_transaction,
)
from tests_tabsdata.testing_resources.test_output_mariadb_with_charset.example import (
    testing_tabsdata_function as output_mariadb_with_charset,
)
from tests_tabsdata.testing_resources.test_output_mariadb_with_collation.example import (
    testing_tabsdata_function as output_mariadb_with_collation,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


ESCAPED_USER = urllib.parse.quote(DB_USER)
ESCAPED_PASSWORD = urllib.parse.quote(DB_PASSWORD)
MARIADB_URI = (
    f"mysql://{ESCAPED_USER}:{ESCAPED_PASSWORD}@{DB_HOST}:{MARIADB_PORT}/{DB_NAME}"
)
ROOT_PROJECT_DIR = os.path.dirname(
    os.path.dirname(os.path.dirname(ABSOLUTE_TEST_FOLDER_LOCATION))
)
RESPONSE_FOLDER = "response_folder"

LOCAL_DEV_FOLDER = TDLOCAL_FOLDER


@pytest.mark.mariadb
@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_mariadb_list(tmp_path, mariadb_config):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    destination_tables = ["output_mariadb_list", "second_output_mariadb_list"]
    context_archive = create_bundle_archive(
        output_mariadb_list(
            mariadb_config.uri,
            credentials=mariadb_config.credentials,
            destination_tables=destination_tables,
        ),
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_mariadb_list", "mock_table.parquet"
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_function_yaml_file(
        input_yaml_file,
        context_archive,
        mock_dependency_location=[mock_parquet_table],
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
    output = pl.read_database_uri(
        uri=mariadb_config.uri_with_credentials,
        query="SELECT * FROM output_mariadb_list",
    )
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_mariadb_list",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    output = pl.read_database_uri(
        uri=mariadb_config.uri_with_credentials,
        query="SELECT * FROM second_output_mariadb_list",
    )
    output = clean_polars_df(output)
    assert output.equals(expected_output)


@pytest.mark.mariadb
@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_mariadb_with_charset(tmp_path, mariadb_config):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        output_mariadb_with_charset(
            mariadb_config.uri,
            credentials=mariadb_config.credentials,
            destination_tables=["output_mariadb_with_charset"],
        ),
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_mariadb_with_charset",
        "mock_table.parquet",
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_function_yaml_file(
        input_yaml_file,
        context_archive,
        mock_dependency_location=[mock_parquet_table],
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
    output = pl.read_database_uri(
        uri=mariadb_config.uri_with_credentials,
        query="SELECT * FROM output_mariadb_with_charset",
    )
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_mariadb_with_charset",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.mariadb
@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_mariadb_with_collation(tmp_path, mariadb_config):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        output_mariadb_with_collation(
            mariadb_config.uri,
            credentials=mariadb_config.credentials,
            destination_tables=["output_mariadb_with_collation"],
        ),
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_mariadb_with_collation",
        "mock_table.parquet",
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_function_yaml_file(
        input_yaml_file,
        context_archive,
        mock_dependency_location=[mock_parquet_table],
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
    output = pl.read_database_uri(
        uri=mariadb_config.uri_with_credentials,
        query="SELECT * FROM output_mariadb_with_collation",
    )
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_mariadb_with_collation",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.mariadb
@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_mariadb_driver_provided(tmp_path, mariadb_config):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        output_mariadb_driver_provided(
            mariadb_config.uri,
            credentials=mariadb_config.credentials,
            destination_tables=["output_mariadb_driver_provided"],
        ),
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_mariadb_driver_provided",
        "mock_table.parquet",
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_function_yaml_file(
        input_yaml_file,
        context_archive,
        mock_dependency_location=[mock_parquet_table],
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
    output = pl.read_database_uri(
        uri=mariadb_config.uri_with_credentials,
        query="SELECT * FROM output_mariadb_driver_provided",
    )
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_mariadb_driver_provided",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.mariadb
@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_mariadb_list_single_element(tmp_path, mariadb_config):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        output_mariadb_list_single_element(
            mariadb_config.uri,
            credentials=mariadb_config.credentials,
            destination_tables=["output_mariadb_list_single_element"],
        ),
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_mariadb_list_single_element",
        "mock_table.parquet",
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_function_yaml_file(
        input_yaml_file,
        context_archive,
        mock_dependency_location=[mock_parquet_table],
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
    output = pl.read_database_uri(
        uri=mariadb_config.uri_with_credentials,
        query="SELECT * FROM output_mariadb_list_single_element",
    )
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_mariadb_list_single_element",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.mariadb
@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_mariadb_transaction(tmp_path, mariadb_config):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        output_mariadb_transaction(
            mariadb_config.uri,
            credentials=mariadb_config.credentials,
            destination_tables=[
                "output_mariadb_transaction",
                "second_output_mariadb_transaction",
            ],
        ),
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_mariadb_transaction",
        "mock_table.parquet",
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_function_yaml_file(
        input_yaml_file,
        context_archive,
        mock_dependency_location=[mock_parquet_table],
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
    assert result != 0
    output = pl.read_database_uri(
        uri=mariadb_config.uri_with_credentials,
        query="SELECT * FROM output_mariadb_transaction",
    )
    output = clean_polars_df(output)
    assert output.is_empty()

    output = pl.read_database_uri(
        uri=mariadb_config.uri_with_credentials,
        query="SELECT * FROM second_output_mariadb_transaction",
    )
    output = clean_polars_df(output)
    assert output.is_empty()
