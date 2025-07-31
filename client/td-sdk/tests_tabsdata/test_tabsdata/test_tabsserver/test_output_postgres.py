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
from tests_tabsdata.bootest import TDLOCAL_FOLDER
from tests_tabsdata.conftest import (
    ABSOLUTE_TEST_FOLDER_LOCATION,
    DB_HOST,
    DB_NAME,
    DB_PASSWORD,
    DB_USER,
    FUNCTION_DATA_FOLDER,
    LOCAL_PACKAGES_LIST,
    POSTGRES_PORT,
    PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
    TESTING_RESOURCES_FOLDER,
    clean_polars_df,
    read_json_and_clean,
    write_v2_yaml_file,
)
from tests_tabsdata.testing_resources.test_output_postgres_driver_provided.example import (
    output_postgres_driver_provided,
)
from tests_tabsdata.testing_resources.test_output_postgres_list.example import (
    output_postgres_list,
)
from tests_tabsdata.testing_resources.test_output_postgres_schema.example import (
    output_postgres_schema,
)
from tests_tabsdata.testing_resources.test_output_postgres_table_replace.example import (
    output_postgres_table_replace,
)
from tests_tabsdata.testing_resources.test_output_postgres_transaction.example import (
    output_postgres_transaction,
)

from tabsdata._tabsserver.function.response_utils import RESPONSE_FILE_NAME
from tabsdata._tabsserver.invoker import REQUEST_FILE_NAME
from tabsdata._tabsserver.invoker import invoke as tabsserver_main
from tabsdata._utils.bundle_utils import create_bundle_archive

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

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

LOCAL_DEV_FOLDER = TDLOCAL_FOLDER


@pytest.mark.postgres
@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_postgres_list(tmp_path, testing_postgres):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        output_postgres_list, local_packages=LOCAL_PACKAGES_LIST, save_location=tmp_path
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_postgres_list", "mock_table.parquet"
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
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
        uri=POSTGRES_URI,
        query="SELECT * FROM output_postgres_list",
    )
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_postgres_list",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    output = pl.read_database_uri(
        uri=POSTGRES_URI,
        query="SELECT * FROM second_output_postgres_list",
    )
    output = clean_polars_df(output)
    assert output.equals(expected_output)


@pytest.mark.postgres
@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_postgres_driver_provided(tmp_path, testing_postgres):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        output_postgres_driver_provided,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_postgres_driver_provided",
        "mock_table.parquet",
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
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
        uri=POSTGRES_URI,
        query="SELECT * FROM output_postgres_driver_provided",
    )
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_postgres_driver_provided",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.postgres
@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_postgres_transaction(tmp_path, testing_postgres):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        output_postgres_transaction,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_postgres_transaction",
        "mock_table.parquet",
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
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
        uri=POSTGRES_URI,
        query="SELECT * FROM output_postgres_transaction",
    )
    output = clean_polars_df(output)
    assert output.is_empty()

    output = pl.read_database_uri(
        uri=POSTGRES_URI,
        query="SELECT * FROM second_output_postgres_transaction",
    )
    output = clean_polars_df(output)
    assert output.is_empty()


@pytest.mark.postgres
@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_postgres_table_replace(tmp_path, testing_postgres):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        output_postgres_table_replace,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_postgres_table_replace",
        "mock_table.parquet",
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        mock_dependency_location=[mock_parquet_table],
        function_data_path=function_data_folder,
    )
    tabsserver_output_folder = os.path.join(tmp_path, "tabsserver_output")
    os.makedirs(tabsserver_output_folder, exist_ok=True)
    for _ in range(2):
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
            uri=POSTGRES_URI,
            query="SELECT * FROM output_postgres_table_replace",
        )
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_postgres_table_replace",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)


@pytest.mark.postgres
@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_postgres_schema(tmp_path, testing_postgres):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        output_postgres_schema,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_postgres_schema",
        "mock_table.parquet",
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        mock_dependency_location=[mock_parquet_table],
        function_data_path=function_data_folder,
    )
    tabsserver_output_folder = os.path.join(tmp_path, "tabsserver_output")
    os.makedirs(tabsserver_output_folder, exist_ok=True)
    for _ in range(2):
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
            uri=POSTGRES_URI,
            query="SELECT * FROM testing_schema.output_postgres_schema",
        )
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_postgres_schema",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
