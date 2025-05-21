#
# Copyright 2025 Tabs Data Inc.
#

import inspect
import logging
import os
import uuid

import polars as pl
import pytest
from tests_tabsdata.bootest import ROOT_FOLDER, TDLOCAL_FOLDER
from tests_tabsdata.conftest import (
    FUNCTION_DATA_FOLDER,
    LOCAL_PACKAGES_LIST,
    PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
    clean_polars_df,
    read_json_and_clean,
    write_v2_yaml_file,
)
from tests_tabsdata_snowflake.conftest import (
    TESTING_RESOURCES_FOLDER,
)
from tests_tabsdata_snowflake.testing_resources.test_multiple_outputs_snowflake.example import (
    multiple_outputs_snowflake,
)
from tests_tabsdata_snowflake.testing_resources.test_output_snowflake.example import (
    output_snowflake,
)

import tabsdata as td
from tabsdata.tabsserver.function.response_utils import RESPONSE_FILE_NAME
from tabsdata.tabsserver.invoker import REQUEST_FILE_NAME
from tabsdata.tabsserver.invoker import invoke as tabsserver_main
from tabsdata.utils.bundle_utils import create_bundle_archive

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ROOT_PROJECT_DIR = ROOT_FOLDER
RESPONSE_FOLDER = "response_folder"

LOCAL_DEV_FOLDER = TDLOCAL_FOLDER

FAKE_CONNECTION_PARAMETERS = {"fake": "parameter"}

REAL_CONNECTION_PARAMETERS = {
    "account": td.EnvironmentSecret("TD_SNOWFLAKE_ACCOUNT"),
    "user": td.EnvironmentSecret("TD_SNOWFLAKE_USER"),
    "password": td.EnvironmentSecret("TD_SNOWFLAKE_PASSWORD"),
    "role": "SYSADMIN",
    "database": "TESTING_DB",
    "schema": "PUBLIC",
    "warehouse": "SNOWFLAKE_LEARNING_WH",
}

PREEXISTING_STAGE = "PREEXISTING_STAGE"


@pytest.mark.snowflake
def test_snowflake_class_parameters():
    snowflake_destination = td.SnowflakeDestination(FAKE_CONNECTION_PARAMETERS, "table")
    assert snowflake_destination.connection_parameters == FAKE_CONNECTION_PARAMETERS
    assert snowflake_destination.destination_table == ["table"]
    assert snowflake_destination.if_table_exists == "append"
    assert snowflake_destination.stage is None

    snowflake_destination = td.SnowflakeDestination(
        FAKE_CONNECTION_PARAMETERS,
        ["table1", "table2"],
        if_table_exists="replace",
        stage="fake_stage",
    )
    assert snowflake_destination.connection_parameters == FAKE_CONNECTION_PARAMETERS
    assert snowflake_destination.destination_table == ["table1", "table2"]
    assert snowflake_destination.if_table_exists == "replace"
    assert snowflake_destination.stage == "fake_stage"


@pytest.mark.snowflake
def test_snowflake_wrong_value_if_table_exists():
    with pytest.raises(ValueError):
        td.SnowflakeDestination(FAKE_CONNECTION_PARAMETERS, "table", "wrong_value")


@pytest.mark.snowflake
def test_snowflake_wrong_table_type():
    with pytest.raises(TypeError):
        td.SnowflakeDestination(FAKE_CONNECTION_PARAMETERS, 42)


@pytest.mark.snowflake
def test_snowflake_wrong_table_list_type():
    with pytest.raises(TypeError):
        td.SnowflakeDestination(FAKE_CONNECTION_PARAMETERS, [42])


@pytest.mark.snowflake
def test_snowflake_chunk(tmp_path):
    snowflake_destination = td.SnowflakeDestination(FAKE_CONNECTION_PARAMETERS, "table")
    df1 = pl.LazyFrame({"a": [1, 2, 3], "b": ["c", "d", "e"]})
    df2 = pl.LazyFrame({"c": [4, 5, 6], "d": ["hi", "hello", "bye"]})
    resulting_files = snowflake_destination.chunk(str(tmp_path), df1, None, df2)
    assert str(tmp_path) in resulting_files[0]
    assert resulting_files[1] is None
    assert str(tmp_path) in resulting_files[2]
    result1 = pl.read_parquet(resulting_files[0])
    result2 = pl.read_parquet(resulting_files[2])
    assert df1.collect().equals(result1)
    assert df2.collect().equals(result2)


@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.snowflake
def test_write_snowflake(tmp_path, snowflake_connection):
    table_name = f"write_snowflake_table_{uuid.uuid4()}".replace("-", "_")
    destination = td.SnowflakeDestination(REAL_CONNECTION_PARAMETERS, table_name)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "example_file", "mock_table.parquet"
    )
    try:
        destination.write([mock_parquet_table])
        cursor = snowflake_connection.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        results = cursor.fetchall()
        output = pl.DataFrame(
            results, ["Duration", "Pulse", "Maxpulse", "Calories"], orient="row"
        )
        output = clean_polars_df(output)
        expected_output = pl.read_parquet(mock_parquet_table)
        expected_output = clean_polars_df(expected_output)
        assert output.equals(expected_output)
    finally:
        # Clean up the generated table
        cursor = snowflake_connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        cursor.close()


@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.snowflake
def test_write_snowflake_multiple_files(tmp_path, snowflake_connection):
    table_name_0 = f"write_snowflake_multiple_files_table_0_{uuid.uuid4()}".replace(
        "-", "_"
    )
    table_name_1 = f"write_snowflake_multiple_files_table_1_{uuid.uuid4()}".replace(
        "-", "_"
    )
    destination = td.SnowflakeDestination(
        REAL_CONNECTION_PARAMETERS, [table_name_0, table_name_1]
    )
    mock_parquet_table_0 = os.path.join(
        TESTING_RESOURCES_FOLDER, "example_file", "mock_table.parquet"
    )
    df = pl.DataFrame({"a": [1, 2, 3], "b": ["c", "d", "e"]})
    mock_parquet_table_1 = os.path.join(tmp_path, "mock_table_1.parquet")
    df.write_parquet(mock_parquet_table_1)
    try:
        destination.write([mock_parquet_table_0, mock_parquet_table_1])

        # Check first
        cursor = snowflake_connection.cursor()
        cursor.execute(f"SELECT * FROM {table_name_0}")
        results = cursor.fetchall()
        output = pl.DataFrame(
            results, ["Duration", "Pulse", "Maxpulse", "Calories"], orient="row"
        )
        output = clean_polars_df(output)
        expected_output = pl.read_parquet(mock_parquet_table_0)
        expected_output = clean_polars_df(expected_output)
        assert output.equals(expected_output)

        # Check second
        cursor = snowflake_connection.cursor()
        cursor.execute(f"SELECT * FROM {table_name_1}")
        results = cursor.fetchall()
        output = pl.DataFrame(results, ["a", "b"], orient="row")
        expected_output_file = mock_parquet_table_1
        expected_output = pl.read_parquet(expected_output_file)
        assert output.equals(expected_output)

    finally:
        # Clean up the generated tables
        try:
            cursor = snowflake_connection.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {table_name_0}")
            cursor.close()
        except:
            pass
        try:
            cursor = snowflake_connection.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {table_name_1}")
            cursor.close()
        except:
            pass


@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.snowflake
def test_write_snowflake_with_stage(tmp_path, snowflake_connection):
    table_name = f"write_snowflake_with_stage_table_{uuid.uuid4()}".replace("-", "_")
    destination = td.SnowflakeDestination(
        REAL_CONNECTION_PARAMETERS, table_name, stage=PREEXISTING_STAGE
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "example_file", "mock_table.parquet"
    )
    try:
        destination.write([mock_parquet_table])
        cursor = snowflake_connection.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        results = cursor.fetchall()
        output = pl.DataFrame(
            results, ["Duration", "Pulse", "Maxpulse", "Calories"], orient="row"
        )
        output = clean_polars_df(output)
        expected_output = pl.read_parquet(mock_parquet_table)
        expected_output = clean_polars_df(expected_output)
        assert output.equals(expected_output)
    finally:
        # Clean up the generated table
        cursor = snowflake_connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        cursor.close()


@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.snowflake
def test_write_snowflake_append(tmp_path, snowflake_connection):
    table_name = f"write_snowflake_append_table_{uuid.uuid4()}".replace("-", "_")
    destination = td.SnowflakeDestination(
        REAL_CONNECTION_PARAMETERS, table_name, if_table_exists="append"
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "example_file", "mock_table.parquet"
    )
    try:
        for i in range(2):
            destination.write([mock_parquet_table])
            cursor = snowflake_connection.cursor()
            cursor.execute(f"SELECT * FROM {table_name}")
            results = cursor.fetchall()
            output = pl.DataFrame(
                results, ["Duration", "Pulse", "Maxpulse", "Calories"], orient="row"
            )
            output = clean_polars_df(output)
            expected_output = pl.read_parquet(mock_parquet_table)
            if i == 1:
                expected_output = pl.concat([expected_output, expected_output])
            expected_output = clean_polars_df(expected_output)
            assert output.equals(expected_output)
    finally:
        # Clean up the generated table
        cursor = snowflake_connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        cursor.close()


@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.snowflake
def test_write_snowflake_replace(tmp_path, snowflake_connection):
    table_name = f"write_snowflake_replace_table_{uuid.uuid4()}".replace("-", "_")
    destination = td.SnowflakeDestination(
        REAL_CONNECTION_PARAMETERS, table_name, if_table_exists="replace"
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "example_file", "mock_table.parquet"
    )
    try:
        for _ in range(2):
            destination.write([mock_parquet_table])
            cursor = snowflake_connection.cursor()
            cursor.execute(f"SELECT * FROM {table_name}")
            results = cursor.fetchall()
            output = pl.DataFrame(
                results, ["Duration", "Pulse", "Maxpulse", "Calories"], orient="row"
            )
            output = clean_polars_df(output)
            expected_output = pl.read_parquet(mock_parquet_table)
            expected_output = clean_polars_df(expected_output)
            assert output.equals(expected_output)
    finally:
        # Clean up the generated table
        cursor = snowflake_connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        cursor.close()


@pytest.mark.snowflake
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tabsserver
def test_output_snowflake(tmp_path, snowflake_connection):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    table_name = f"output_snowflake_table_{uuid.uuid4()}".replace("-", "_")
    destination = td.SnowflakeDestination(REAL_CONNECTION_PARAMETERS, table_name)
    output_snowflake.output = destination
    context_archive = create_bundle_archive(
        output_snowflake,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_snowflake", "mock_table.parquet"
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
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
    )
    try:
        assert result == 0
        assert os.path.exists(os.path.join(response_folder, RESPONSE_FILE_NAME))
        cursor = snowflake_connection.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        results = cursor.fetchall()
        output = pl.DataFrame(
            results, ["Duration", "Pulse", "Maxpulse", "Calories"], orient="row"
        )
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_snowflake",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        # Clean up the generated table
        cursor = snowflake_connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        cursor.close()


@pytest.mark.snowflake
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tabsserver
def test_multiple_outputs_snowflake(tmp_path, snowflake_connection):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    table_name_0 = f"multiple_outputs_snowflake_table_0_{uuid.uuid4()}".replace(
        "-", "_"
    )
    table_name_1 = f"multiple_outputs_snowflake_table_1_{uuid.uuid4()}".replace(
        "-", "_"
    )
    destination = td.SnowflakeDestination(
        REAL_CONNECTION_PARAMETERS, [table_name_0, table_name_1]
    )
    multiple_outputs_snowflake.output = destination
    context_archive = create_bundle_archive(
        multiple_outputs_snowflake,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_multiple_outputs_snowflake",
        "mock_table.parquet",
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
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
    )
    try:
        assert result == 0
        assert os.path.exists(os.path.join(response_folder, RESPONSE_FILE_NAME))

        # Check first result
        cursor = snowflake_connection.cursor()
        cursor.execute(f"SELECT * FROM {table_name_0}")
        results = cursor.fetchall()
        output = pl.DataFrame(
            results, ["Duration", "Pulse", "Maxpulse", "Calories"], orient="row"
        )
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_multiple_outputs_snowflake",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)

        # Check second result
        cursor = snowflake_connection.cursor()
        cursor.execute(f"SELECT * FROM {table_name_1}")
        results = cursor.fetchall()
        output = pl.DataFrame(
            results, ["Duration", "Pulse", "Maxpulse", "Calories"], orient="row"
        )
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_multiple_outputs_snowflake",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        # Clean up the generated tables
        try:
            cursor = snowflake_connection.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {table_name_0}")
            cursor.close()
        except:
            pass
        try:
            cursor = snowflake_connection.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {table_name_1}")
            cursor.close()
        except:
            pass
