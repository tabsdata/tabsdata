#
# Copyright 2025 Tabs Data Inc.
#

import inspect
import json
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
from tests_tabsdata.common.backend import ORACLE_PORT
from tests_tabsdata.common.constants import (
    DB_HOST,
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
from tests_tabsdata.testing_resources.test_input_new_oracle.example import (
    testing_tabsdata_function as input_new_oracle,
)
from tests_tabsdata.testing_resources.test_input_new_oracle_initial_values.example import (
    testing_tabsdata_function as input_new_oracle_initial_values,
)
from tests_tabsdata.testing_resources.test_input_new_oracle_types_check.example import (
    testing_tabsdata_function as input_new_oracle_types_check,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def json_to_polars_type(value):
    if isinstance(value, str):
        return getattr(pl, value)
    elif isinstance(value, dict):
        if "Struct" in value:
            fields = value["Struct"]
            return pl.Struct(
                [pl.Field(name, getattr(pl, dtype)) for name, dtype in fields.items()]
            )
    raise ValueError(f"Unknown type definition: {value}")


ESCAPED_USER = urllib.parse.quote(DB_USER)
ESCAPED_PASSWORD = urllib.parse.quote(DB_PASSWORD)
ORACLE_URI = f"oracle://system:{ESCAPED_PASSWORD}@{DB_HOST}:{ORACLE_PORT}/FREE"
ROOT_PROJECT_DIR = os.path.dirname(
    os.path.dirname(os.path.dirname(ABSOLUTE_TEST_FOLDER_LOCATION))
)
RESPONSE_FOLDER = "response_folder"

LOCAL_DEV_FOLDER = TDLOCAL_FOLDER


@pytest.mark.oracle
@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_input_new_oracle(oracle_config, tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_new_oracle(oracle_config.uri, oracle_config.credentials),
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file1 = os.path.join(tmp_path, "output1.parquet")
    output_file2 = os.path.join(tmp_path, "output2.parquet")
    path_to_output_initial_values = os.path.join(tmp_path, "initial_values.parquet")
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_function_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output_file1, output_file2],
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

    assert os.path.isfile(output_file1)
    output = pl.read_parquet(output_file1)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_oracle",
        "expected_result1.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    assert os.path.isfile(output_file2)
    output = pl.read_parquet(output_file2)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_oracle",
        "expected_result2.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)
    assert not os.path.isfile(path_to_output_initial_values)


@pytest.mark.oracle
@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_input_new_oracle_pandas(oracle_config, tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_new_oracle(oracle_config.uri, oracle_config.credentials, loader="pandas"),
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file1 = os.path.join(tmp_path, "output1.parquet")
    output_file2 = os.path.join(tmp_path, "output2.parquet")
    path_to_output_initial_values = os.path.join(tmp_path, "initial_values.parquet")
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_function_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output_file1, output_file2],
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

    assert os.path.isfile(output_file1)
    output = pl.read_parquet(output_file1)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_oracle",
        "expected_result1.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    assert os.path.isfile(output_file2)
    output = pl.read_parquet(output_file2)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_oracle",
        "expected_result2.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)
    assert not os.path.isfile(path_to_output_initial_values)


@pytest.mark.oracle
@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_input_new_oracle_chunk_size(oracle_config, tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_new_oracle(oracle_config.uri, oracle_config.credentials, chunk_size=1),
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file1 = os.path.join(tmp_path, "output1.parquet")
    output_file2 = os.path.join(tmp_path, "output2.parquet")
    path_to_output_initial_values = os.path.join(tmp_path, "initial_values.parquet")
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_function_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output_file1, output_file2],
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

    assert os.path.isfile(output_file1)
    output = pl.read_parquet(output_file1)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_oracle",
        "expected_result1.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    assert os.path.isfile(output_file2)
    output = pl.read_parquet(output_file2)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_oracle",
        "expected_result2.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)
    assert not os.path.isfile(path_to_output_initial_values)


@pytest.mark.oracle
@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_input_new_oracle_chunk_size_pandas(oracle_config, tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_new_oracle(
            oracle_config.uri, oracle_config.credentials, loader="pandas", chunk_size=1
        ),
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file1 = os.path.join(tmp_path, "output1.parquet")
    output_file2 = os.path.join(tmp_path, "output2.parquet")
    path_to_output_initial_values = os.path.join(tmp_path, "initial_values.parquet")
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_function_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output_file1, output_file2],
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

    assert os.path.isfile(output_file1)
    output = pl.read_parquet(output_file1)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_oracle",
        "expected_result1.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    assert os.path.isfile(output_file2)
    output = pl.read_parquet(output_file2)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_oracle",
        "expected_result2.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)
    assert not os.path.isfile(path_to_output_initial_values)


@pytest.mark.oracle
@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_input_new_oracle_transactional_false(oracle_config, tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_new_oracle(
            oracle_config.uri, oracle_config.credentials, transactional=False
        ),
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file1 = os.path.join(tmp_path, "output1.parquet")
    output_file2 = os.path.join(tmp_path, "output2.parquet")
    path_to_output_initial_values = os.path.join(tmp_path, "initial_values.parquet")
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_function_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output_file1, output_file2],
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

    assert os.path.isfile(output_file1)
    output = pl.read_parquet(output_file1)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_oracle",
        "expected_result1.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    assert os.path.isfile(output_file2)
    output = pl.read_parquet(output_file2)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_oracle",
        "expected_result2.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)
    assert not os.path.isfile(path_to_output_initial_values)


@pytest.mark.oracle
@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_input_new_oracle_types_check(oracle_config, tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_new_oracle_types_check(oracle_config.uri, oracle_config.credentials),
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file1 = os.path.join(tmp_path, "output1.parquet")
    path_to_output_initial_values = os.path.join(tmp_path, "initial_values.parquet")
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_function_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output_file1],
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

    assert os.path.isfile(output_file1)
    output = pl.read_parquet(output_file1)
    output = clean_polars_df(output)
    output_schema = dict(
        sorted({k.lower(): v for k, v in output.schema.items()}.items())
    )

    expected_output_schema_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_oracle_types_check",
        "expected_result_polars_sqlalchemy_schema.json",
    )
    with open(expected_output_schema_file, "r") as f:
        expected_output_schema = json.load(f)
    expected_output_schema = {
        k.lower(): json_to_polars_type(v) for k, v in expected_output_schema.items()
    }
    expected_output_schema = dict(sorted(expected_output_schema.items()))

    assert output_schema == expected_output_schema


@pytest.mark.oracle
@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_input_new_oracle_types_check_pandas(oracle_config, tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_new_oracle_types_check(
            oracle_config.uri, oracle_config.credentials, loader="pandas"
        ),
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file1 = os.path.join(tmp_path, "output1.parquet")
    path_to_output_initial_values = os.path.join(tmp_path, "initial_values.parquet")
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_function_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output_file1],
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

    assert os.path.isfile(output_file1)
    output = pl.read_parquet(output_file1)
    output = clean_polars_df(output)
    output_schema = dict(
        sorted({k.lower(): v for k, v in output.schema.items()}.items())
    )

    expected_output_schema_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_oracle_types_check",
        "expected_result_pandas_schema.json",
    )
    with open(expected_output_schema_file, "r") as f:
        expected_output_schema = json.load(f)
    expected_output_schema = {
        k.lower(): json_to_polars_type(v) for k, v in expected_output_schema.items()
    }
    expected_output_schema = dict(sorted(expected_output_schema.items()))

    assert output_schema == expected_output_schema


@pytest.mark.oracle
@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_input_new_oracle_types_check_w_schema_overrides(oracle_config, tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_new_oracle_types_check(
            oracle_config.uri,
            oracle_config.credentials,
            schema_overrides={
                "C_NUMBER": pl.Float64(),
                "C_FLOAT": pl.Int64(),
                "C_BINARY_FLOAT": pl.String(),
            },
        ),
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file1 = os.path.join(tmp_path, "output1.parquet")
    path_to_output_initial_values = os.path.join(tmp_path, "initial_values.parquet")
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_function_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output_file1],
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

    assert os.path.isfile(output_file1)
    output = pl.read_parquet(output_file1)
    output = clean_polars_df(output)
    output_schema = dict(
        sorted({k.lower(): v for k, v in output.schema.items()}.items())
    )

    expected_output_schema_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_oracle_types_check",
        "expected_result_polars_sqlalchemy_schema_override.json",
    )
    with open(expected_output_schema_file, "r") as f:
        expected_output_schema = json.load(f)
    expected_output_schema = {
        k.lower(): json_to_polars_type(v) for k, v in expected_output_schema.items()
    }
    expected_output_schema = dict(sorted(expected_output_schema.items()))

    assert output_schema == expected_output_schema


@pytest.mark.oracle
@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_input_new_oracle_types_check_pandas_w_schema_overrides(
    oracle_config, tmp_path
):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_new_oracle_types_check(
            oracle_config.uri,
            oracle_config.credentials,
            schema_overrides={
                "c_number": pl.Float64(),
                "c_float": pl.Int64(),
                "c_binary_float": pl.String(),
            },
            loader="pandas",
        ),
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file1 = os.path.join(tmp_path, "output1.parquet")
    path_to_output_initial_values = os.path.join(tmp_path, "initial_values.parquet")
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_function_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output_file1],
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

    assert os.path.isfile(output_file1)
    output = pl.read_parquet(output_file1)
    output = clean_polars_df(output)
    output_schema = dict(
        sorted({k.lower(): v for k, v in output.schema.items()}.items())
    )

    expected_output_schema_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_oracle_types_check",
        "expected_result_pandas_schema_override.json",
    )
    with open(expected_output_schema_file, "r") as f:
        expected_output_schema = json.load(f)
    expected_output_schema = {
        k.lower(): json_to_polars_type(v) for k, v in expected_output_schema.items()
    }
    expected_output_schema = dict(sorted(expected_output_schema.items()))

    assert output_schema == expected_output_schema


@pytest.mark.oracle
@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_input_new_oracle_initial_values(oracle_config, tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_new_oracle_initial_values(oracle_config.uri, oracle_config.credentials),
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file1 = os.path.join(tmp_path, "output1.parquet")
    output_file2 = os.path.join(tmp_path, "output2.parquet")
    path_to_output_initial_values = os.path.join(tmp_path, "initial_values.parquet")
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_function_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output_file1, output_file2],
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

    assert os.path.isfile(output_file1)
    output = pl.read_parquet(output_file1)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_oracle_initial_values",
        "expected_result1.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    assert os.path.isfile(output_file2)
    output = pl.read_parquet(output_file2)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_oracle_initial_values",
        "expected_result2.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)
    assert os.path.isfile(path_to_output_initial_values)
    output_initial_values = pl.read_parquet(path_to_output_initial_values)
    assert output_initial_values.equals(pl.DataFrame({"number": 3}))


@pytest.mark.oracle
@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_input_new_oracle_initial_values_stored_number_0(oracle_config, tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_new_oracle_initial_values(oracle_config.uri, oracle_config.credentials),
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file1 = os.path.join(tmp_path, "output1.parquet")
    output_file2 = os.path.join(tmp_path, "output2.parquet")
    path_to_input_initial_values = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_oracle_initial_values",
        "mock_number_0.parquet",
    )
    path_to_output_initial_values = os.path.join(tmp_path, "initial_values.parquet")
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_function_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output_file1, output_file2],
        input_initial_values_path=path_to_input_initial_values,
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

    assert os.path.isfile(output_file1)
    output = pl.read_parquet(output_file1)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_oracle_initial_values",
        "expected_result1_number_0.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    assert os.path.isfile(output_file2)
    output = pl.read_parquet(output_file2)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_oracle_initial_values",
        "expected_result2_number_0.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)
    assert os.path.isfile(path_to_output_initial_values)
    output_initial_values = pl.read_parquet(path_to_output_initial_values)
    assert output_initial_values.equals(pl.DataFrame({"number": 3}))


@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.oracle
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_input_new_oracle_initial_values_stored_number_2(oracle_config, tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_new_oracle_initial_values(oracle_config.uri, oracle_config.credentials),
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file1 = os.path.join(tmp_path, "output1.parquet")
    output_file2 = os.path.join(tmp_path, "output2.parquet")
    path_to_input_initial_values = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_oracle_initial_values",
        "mock_number_2.parquet",
    )
    path_to_output_initial_values = os.path.join(tmp_path, "initial_values.parquet")
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_function_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output_file1, output_file2],
        input_initial_values_path=path_to_input_initial_values,
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

    assert os.path.isfile(output_file1)
    output = pl.read_parquet(output_file1)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_oracle_initial_values",
        "expected_result1.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    assert os.path.isfile(output_file2)
    output = pl.read_parquet(output_file2)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_new_oracle_initial_values",
        "expected_result2.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)
    assert os.path.isfile(path_to_output_initial_values)
    output_initial_values = pl.read_parquet(path_to_output_initial_values)
    assert output_initial_values.equals(pl.DataFrame({"number": 3}))
