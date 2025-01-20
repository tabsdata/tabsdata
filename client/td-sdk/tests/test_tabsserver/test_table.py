#
# Copyright 2025 Tabs Data Inc.
#

import inspect
import logging
import os

import polars as pl
import pytest

from tabsdata.utils.bundle_utils import create_bundle_archive
from tabsdata.utils.tableframe._helpers import SYSTEM_COLUMNS
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
from tests.testing_resources.test_input_table.example import input_table
from tests.testing_resources.test_input_table_multiple_tables.example import (
    input_table_multiple_tables,
)
from tests.testing_resources.test_output_table.example import output_table
from tests.testing_resources.test_output_table_multiple_tables.example import (
    output_table_multiple_tables,
)
from tests.testing_resources.test_output_table_multiple_with_none.example import (
    output_table_multiple_tables_with_none,
)
from tests.testing_resources.test_output_table_with_none.example import (
    output_table_with_none,
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
def test_input_table(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_table,
        local_packages=ROOT_PROJECT_DIR,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_input_table", "mock_table.parquet"
    )
    output_file = os.path.join(tmp_path, "output.parquet")
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
        mock_table_location=[output_file],
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
        "test_input_table",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.requires_internet
@pytest.mark.slow
def test_input_table_uri_null(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_table,
        local_packages=ROOT_PROJECT_DIR,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file = os.path.join(tmp_path, "output.parquet")
    write_v1_yaml_file(
        input_yaml_file, context_archive, ["null"], mock_table_location=[output_file]
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
        "test_input_table",
        "expected_result_with_none.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.requires_internet
@pytest.mark.slow
def test_input_table_multiple_tables(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_table_multiple_tables,
        local_packages=ROOT_PROJECT_DIR,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_invoice_headers = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_table_multiple_tables",
        "invoice-headers.parquet",
    )
    mock_invoice_items = [
        os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_input_table_multiple_tables",
            "invoice-items-1.parquet",
        ),
        os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_input_table_multiple_tables",
            "invoice-items-2.parquet",
        ),
    ]
    output_file1 = os.path.join(tmp_path, "output1.parquet")
    output_file2 = os.path.join(tmp_path, "output2.parquet")
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_invoice_headers, mock_invoice_items],
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
        "test_input_table_multiple_tables",
        "expected_result1.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    assert os.path.isfile(output_file2)
    output = pl.read_parquet(output_file2)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_table_multiple_tables",
        "expected_result2.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.requires_internet
@pytest.mark.slow
def test_output_table_multiple_tables(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        output_table_multiple_tables,
        local_packages=ROOT_PROJECT_DIR,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output1_path = os.path.join(tmp_path, "output1.parquet")
    output2_path = os.path.join(tmp_path, "output2.parquet")
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output1_path, output2_path],
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
    output = pl.read_parquet(output1_path)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_table_multiple_tables",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    output = pl.read_parquet(output2_path)
    output = clean_polars_df(output)
    assert output.equals(expected_output)


@pytest.mark.requires_internet
@pytest.mark.slow
def test_output_table(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        output_table,
        local_packages=ROOT_PROJECT_DIR,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_dependency_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_table", "mock_table.parquet"
    )
    output_path = os.path.join(tmp_path, "output.parquet")
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        mock_dependency_location=[mock_dependency_table],
        mock_table_location=[output_path],
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

    assert os.path.isfile(output_path)
    output = pl.read_parquet(output_path)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_table",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.requires_internet
@pytest.mark.slow
def test_output_table_multiple_tables_with_none(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        output_table_multiple_tables_with_none,
        local_packages=ROOT_PROJECT_DIR,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output1_path = os.path.join(tmp_path, "output1.parquet")
    output2_path = os.path.join(tmp_path, "output2.parquet")
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output1_path, output2_path],
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
    output = pl.read_parquet(output1_path)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_table_multiple_with_none",
        "expected_result.parquet",
    )

    expected_output = pl.read_parquet(expected_output_file)
    # ToDo: ⚠️ Aleix: This is just a workaround...
    #  The persisted expected output does not take into account the additional
    #  columns in enterprise.
    assert drop_system_columns(output).equals(drop_system_columns(expected_output))

    output = pl.read_parquet(output2_path)
    assert drop_system_columns(output).equals(drop_system_columns(expected_output))


@pytest.mark.requires_internet
@pytest.mark.slow
def test_output_table_with_none(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        output_table_with_none,
        local_packages=ROOT_PROJECT_DIR,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_dependency_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_table_with_none", "mock_table.parquet"
    )
    output_path = os.path.join(tmp_path, "output.parquet")
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        mock_dependency_location=[mock_dependency_table],
        mock_table_location=[output_path],
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

    assert os.path.isfile(output_path)
    output = pl.read_parquet(output_path)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_table_with_none",
        "expected_result.parquet",
    )
    expected_output = pl.read_parquet(expected_output_file)
    # ToDo: ⚠️ Aleix: This is just a workaround...
    #  The persisted expected output does not take into account the additional
    #  columns in enterprise.
    assert drop_system_columns(output).equals(drop_system_columns(expected_output))


# ToDo: ⚠️ Aleix: This is just a workaround...
def drop_system_columns(df: pl.DataFrame, ignore_missing: bool = True) -> pl.DataFrame:
    columns_to_remove = list(SYSTEM_COLUMNS)
    if ignore_missing:
        existing_columns = set(df.collect_schema().names())
        columns_to_remove = [
            col for col in columns_to_remove if col in existing_columns
        ]
    return df.drop(columns_to_remove)
