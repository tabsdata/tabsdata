#
# Copyright 2025 Tabs Data Inc.
#

import inspect
import logging
import os
import shutil

import polars as pl
import pytest
from pyiceberg.catalog import load_catalog

import tabsdata as td
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
from tests.testing_resources.test_input_file_csv_modified_format.example import (
    input_file_csv_modified_format,
)
from tests.testing_resources.test_input_file_csv_modified_path.example import (
    input_file_csv_modified_path,
)
from tests.testing_resources.test_input_file_csv_separator.example import (
    input_file_csv_separator,
)
from tests.testing_resources.test_input_file_csv_string_format.example import (
    input_file_csv_string_format,
)
from tests.testing_resources.test_input_file_log_string_format.example import (
    input_file_log_string_format,
)
from tests.testing_resources.test_input_file_log_wildcard.example import (
    input_file_log_wildcard,
)
from tests.testing_resources.test_input_file_ndjson_string_format.example import (
    input_file_ndjson_string_format,
)
from tests.testing_resources.test_input_file_ndjson_wildcard.example import (
    input_file_ndjson_wildcard,
)
from tests.testing_resources.test_input_file_parquet_string_format.example import (
    input_file_parquet_string_format,
)
from tests.testing_resources.test_input_file_parquet_wildcard.example import (
    input_file_parquet_wildcard,
)
from tests.testing_resources.test_input_file_wildcard.example import input_file_wildcard
from tests.testing_resources.test_output_file.example import (
    output_file as output_file_format_testing,
)
from tests.testing_resources.test_output_file_catalog.example import (
    output_file_catalog,
)
from tests.testing_resources.test_output_file_catalog_append.example import (
    output_file_catalog_append,
)
from tests.testing_resources.test_output_file_catalog_replace.example import (
    output_file_catalog_replace,
)
from tests.testing_resources.test_output_file_multiple_files.example import (
    output_file_multiple_files,
)
from tests.testing_resources.test_output_file_multiple_files_with_none.example import (
    output_file_multiple_files_with_none,
)
from tests.testing_resources.test_output_file_with_none.example import (
    output_file_with_none,
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
def test_input_file_wildcard(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_file_wildcard, local_packages=ROOT_PROJECT_DIR, save_location=tmp_path
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
        "test_input_file_wildcard",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.requires_internet
@pytest.mark.slow
def test_input_file_csv_string_format(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_file_csv_string_format,
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
        "test_input_file_csv_string_format",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.requires_internet
@pytest.mark.slow
def test_input_file_parquet_string_format(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_file_parquet_string_format,
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
        "test_input_file_parquet_string_format",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.requires_internet
@pytest.mark.slow
def test_input_file_parquet_wildcard(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_file_parquet_wildcard,
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
        "test_input_file_parquet_wildcard",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.requires_internet
@pytest.mark.slow
def test_input_file_csv_separator(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_file_csv_separator,
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
        "test_input_file_csv_separator",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.requires_internet
@pytest.mark.slow
def test_input_file_csv_modified_path(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_file_csv_modified_path,
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
        "test_input_file_csv_modified_path",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.requires_internet
@pytest.mark.slow
def test_input_file_csv_modified_format(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_file_csv_modified_format,
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
        "test_input_file_csv_modified_format",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.requires_internet
@pytest.mark.slow
def test_output_file_parquet(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    output_file = os.path.join(tmp_path, "test_output_file_parquet.parquet")
    output_file_format_testing.output = td.LocalFileDestination(output_file)
    context_archive = create_bundle_archive(
        output_file_format_testing,
        local_packages=ROOT_PROJECT_DIR,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_file", "mock_table.parquet"
    )
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
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

    temporary_output_file = os.path.join(tabsserver_output_folder, "0.parquet")
    assert os.path.isfile(temporary_output_file)
    output = pl.read_parquet(temporary_output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_file",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    assert os.path.isfile(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_file",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.requires_internet
@pytest.mark.slow
def test_output_file_multiple_files(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    first_output_file = os.path.join(
        tmp_path, "test_output_file_multiple_files1.parquet"
    )
    second_output_file = os.path.join(
        tmp_path, "test_output_file_multiple_files2.parquet"
    )
    output_file_multiple_files.output = td.LocalFileDestination(
        [first_output_file, second_output_file]
    )
    context_archive = create_bundle_archive(
        output_file_multiple_files,
        local_packages=ROOT_PROJECT_DIR,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_file", "mock_table.parquet"
    )
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
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

    temporary_output_file = os.path.join(tabsserver_output_folder, "0.parquet")
    assert os.path.isfile(temporary_output_file)
    output = pl.read_parquet(temporary_output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_file_multiple_files",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    assert os.path.isfile(first_output_file)
    output = pl.read_parquet(first_output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_file_multiple_files",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    assert os.path.isfile(second_output_file)
    output = pl.read_parquet(second_output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_file_multiple_files",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.requires_internet
@pytest.mark.slow
def test_output_file_csv(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    output_file = os.path.join(tmp_path, "test_output_file_csv.csv")
    output_file_format_testing.output = td.LocalFileDestination(
        output_file,
        td.CSVFormat(eol_char="\t", separator="|", output_float_precision=4),
    )
    context_archive = create_bundle_archive(
        output_file_format_testing,
        local_packages=ROOT_PROJECT_DIR,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_file", "mock_table.parquet"
    )
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
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

    temporary_output_file = os.path.join(tabsserver_output_folder, "0.csv")
    assert os.path.isfile(temporary_output_file)
    output = pl.read_csv(temporary_output_file, separator="|", eol_char="\t")
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_file",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    assert os.path.isfile(output_file)
    output = pl.read_csv(output_file, separator="|", eol_char="\t")
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_file",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.requires_internet
@pytest.mark.slow
def test_output_file_ndjson(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    output_file = os.path.join(tmp_path, "test_output_file_ndjson.ndjson")
    output_file_format_testing.output = td.LocalFileDestination(output_file)
    context_archive = create_bundle_archive(
        output_file_format_testing,
        local_packages=ROOT_PROJECT_DIR,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_file", "mock_table.parquet"
    )
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
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

    temporary_output_file = os.path.join(tabsserver_output_folder, "0.ndjson")
    assert os.path.isfile(temporary_output_file)
    output = pl.read_ndjson(temporary_output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_file",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    assert os.path.isfile(output_file)
    output = pl.read_ndjson(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_file",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.requires_internet
@pytest.mark.slow
def test_input_file_log_string_format(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_file_log_string_format,
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
        "test_input_file_log_string_format",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.requires_internet
@pytest.mark.slow
def test_input_file_log_wildcard(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_file_log_wildcard, local_packages=ROOT_PROJECT_DIR, save_location=tmp_path
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
        "test_input_file_log_wildcard",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.requires_internet
@pytest.mark.slow
def test_input_file_ndjson_string_format(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_file_ndjson_string_format,
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
        "test_input_file_ndjson_string_format",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.requires_internet
@pytest.mark.slow
def test_input_file_ndjson_wildcard(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_file_ndjson_wildcard,
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
        "test_input_file_ndjson_wildcard",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.requires_internet
@pytest.mark.slow
def test_output_file_multiple_files_with_none(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    first_output_file = os.path.join(
        tmp_path, "test_output_file_multiple_files_with_none1.parquet"
    )
    second_output_file = os.path.join(
        tmp_path, "test_output_file_multiple_files_with_none2.parquet"
    )
    output_file_multiple_files_with_none.output = td.LocalFileDestination(
        [first_output_file, second_output_file]
    )
    context_archive = create_bundle_archive(
        output_file_multiple_files_with_none,
        local_packages=ROOT_PROJECT_DIR,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_file", "mock_table.parquet"
    )
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
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

    temporary_output_file = os.path.join(tabsserver_output_folder, "0.parquet")
    assert os.path.isfile(temporary_output_file)
    output = pl.read_parquet(temporary_output_file)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_file_multiple_files_with_none",
        "expected_result.json",
    )
    expected_output = pl.read_json(expected_output_file)
    assert output.equals(expected_output)

    assert os.path.isfile(first_output_file)
    output = pl.read_parquet(first_output_file)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_file_multiple_files_with_none",
        "expected_result.json",
    )
    expected_output = pl.read_json(expected_output_file)
    assert output.equals(expected_output)

    assert os.path.isfile(second_output_file)
    output = pl.read_parquet(second_output_file)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_file_multiple_files_with_none",
        "expected_result.json",
    )
    expected_output = pl.read_json(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.requires_internet
@pytest.mark.slow
def test_output_file_with_none(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    output_file = os.path.join(tmp_path, "test_output_file_with_none.parquet")
    output_file_with_none.output = td.LocalFileDestination(output_file)
    context_archive = create_bundle_archive(
        output_file_with_none,
        local_packages=ROOT_PROJECT_DIR,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_file_with_none", "mock_table.parquet"
    )
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
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

    temporary_output_file = os.path.join(tabsserver_output_folder, "0.parquet")
    assert os.path.isfile(temporary_output_file)
    output = pl.read_parquet(temporary_output_file)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_file_with_none",
        "expected_result.json",
    )
    expected_output = pl.read_json(expected_output_file)
    assert output.equals(expected_output)

    assert os.path.isfile(output_file)
    output = pl.read_parquet(output_file)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_file_with_none",
        "expected_result.json",
    )
    expected_output = pl.read_json(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.requires_internet
@pytest.mark.slow
def test_output_file_catalog(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    first_output_file = os.path.join(tmp_path, "test_output_file_catalog1.parquet")
    second_output_file = os.path.join(tmp_path, "test_output_file_catalog2.parquet")
    output_file_catalog.output.path = [
        first_output_file,
        second_output_file,
    ]
    context_archive = create_bundle_archive(
        output_file_catalog,
        local_packages=ROOT_PROJECT_DIR,
        save_location=tmp_path,
    )

    # Set up the catalog
    from tests.testing_resources.test_output_file_catalog.example import (
        warehouse_path,
    )

    # Remove the folder if it exists
    if os.path.exists(warehouse_path):
        shutil.rmtree(warehouse_path)

    # Create the folder
    os.makedirs(warehouse_path)
    catalog = load_catalog(**output_file_catalog.output.catalog.definition)
    catalog.create_namespace("testing_namespace")

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_file", "mock_table.parquet"
    )
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
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

    temporary_output_file = os.path.join(tabsserver_output_folder, "0.parquet")
    assert os.path.isfile(temporary_output_file)
    output = pl.read_parquet(temporary_output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_file_catalog",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    assert os.path.isfile(first_output_file)
    output = pl.read_parquet(first_output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_file_catalog",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    assert os.path.isfile(second_output_file)
    output = pl.read_parquet(second_output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_file_catalog",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    # Verify the catalog has the proper data
    table = catalog.load_table("testing_namespace.output_file_parquet")
    output = pl.DataFrame(table.scan().to_arrow())
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_file_catalog",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    # Verify the catalog has the proper data
    table = catalog.load_table("testing_namespace.second_output_file")
    output = pl.DataFrame(table.scan().to_arrow())
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_file_catalog",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.requires_internet
@pytest.mark.slow
def test_output_file_catalog_append(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    first_output_file = os.path.join(
        tmp_path, "test_output_file_catalog_append1.parquet"
    )
    second_output_file = os.path.join(
        tmp_path, "test_output_file_catalog_append2.parquet"
    )
    output_file_catalog_append.output.path = [
        first_output_file,
        second_output_file,
    ]
    context_archive = create_bundle_archive(
        output_file_catalog_append,
        local_packages=ROOT_PROJECT_DIR,
        save_location=tmp_path,
    )

    # Set up the catalog
    from tests.testing_resources.test_output_file_catalog_append.example import (
        warehouse_path,
    )

    # Remove the folder if it exists
    if os.path.exists(warehouse_path):
        shutil.rmtree(warehouse_path)

    # Create the folder
    os.makedirs(warehouse_path)
    catalog = load_catalog(**output_file_catalog_append.output.catalog.definition)
    catalog.create_namespace("testing_namespace")

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_file", "mock_table.parquet"
    )
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
    )
    tabsserver_output_folder = os.path.join(tmp_path, "tabsserver_output")
    for _ in range(2):
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
            "test_output_file_catalog_append",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)

        assert os.path.isfile(first_output_file)
        output = pl.read_parquet(first_output_file)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_file_catalog_append",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)

        assert os.path.isfile(second_output_file)
        output = pl.read_parquet(second_output_file)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_file_catalog_append",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)

    # Verify the catalog has the proper data
    table = catalog.load_table("testing_namespace.output_file_parquet")
    output = pl.DataFrame(table.scan().to_arrow())
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_file_catalog_append",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    expected_output = clean_polars_df(pl.concat([expected_output, expected_output]))
    assert output.equals(expected_output)

    # Verify the catalog has the proper data
    table = catalog.load_table("testing_namespace.second_output_file")
    output = pl.DataFrame(table.scan().to_arrow())
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_file_catalog_append",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    expected_output = clean_polars_df(pl.concat([expected_output, expected_output]))
    assert output.equals(expected_output)


@pytest.mark.requires_internet
@pytest.mark.slow
def test_output_file_catalog_replace(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    first_output_file = os.path.join(
        tmp_path, "test_output_file_catalog_replace1.parquet"
    )
    second_output_file = os.path.join(
        tmp_path, "test_output_file_catalog_replace2.parquet"
    )
    output_file_catalog_replace.output.path = [
        first_output_file,
        second_output_file,
    ]
    context_archive = create_bundle_archive(
        output_file_catalog_replace,
        local_packages=ROOT_PROJECT_DIR,
        save_location=tmp_path,
    )

    # Set up the catalog
    from tests.testing_resources.test_output_file_catalog_replace.example import (
        warehouse_path,
    )

    # Remove the folder if it exists
    if os.path.exists(warehouse_path):
        shutil.rmtree(warehouse_path)

    # Create the folder
    os.makedirs(warehouse_path)
    catalog = load_catalog(**output_file_catalog_replace.output.catalog.definition)
    catalog.create_namespace("testing_namespace")

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_file", "mock_table.parquet"
    )
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
    )
    tabsserver_output_folder = os.path.join(tmp_path, "tabsserver_output")

    for _ in range(2):
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
            "test_output_file_catalog_replace",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)

        assert os.path.isfile(first_output_file)
        output = pl.read_parquet(first_output_file)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_file_catalog_replace",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)

        assert os.path.isfile(second_output_file)
        output = pl.read_parquet(second_output_file)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_file_catalog_replace",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)

    # Verify the catalog has the proper data
    table = catalog.load_table("testing_namespace.output_file_parquet")
    output = pl.DataFrame(table.scan().to_arrow())
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_file_catalog_replace",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    # Verify the catalog has the proper data
    table = catalog.load_table("testing_namespace.second_output_file")
    output = pl.DataFrame(table.scan().to_arrow())
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_file_catalog_replace",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)
