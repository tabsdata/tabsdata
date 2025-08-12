#
# Copyright 2025 Tabs Data Inc.
#

import copy
import inspect
import logging
import os
import uuid
from io import StringIO
from unittest import mock

import pandas as pd
import polars as pl

# noinspection PyPackageRequirements
import pytest

# noinspection PyPackageRequirements
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from tests_tabsdata_mssql.conftest import (
    DB_PASSWORD,
    MSSQL_USER,
    TESTING_RESOURCES_FOLDER,
)

import tabsdata as td
from tabsdata._tabsserver.function.response_utils import RESPONSE_FILE_NAME
from tabsdata._tabsserver.invoker import REQUEST_FILE_NAME
from tabsdata._tabsserver.invoker import invoke as tabsserver_main
from tabsdata._utils.bundle_utils import create_bundle_archive
from tests_tabsdata.bootest import ROOT_FOLDER, TDLOCAL_FOLDER
from tests_tabsdata.conftest import (
    FUNCTION_DATA_FOLDER,
    LOCAL_PACKAGES_LIST,
    PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
    clean_polars_df,
    get_lf,
    read_json_and_clean,
    write_v2_yaml_file,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ROOT_PROJECT_DIR = ROOT_FOLDER
RESPONSE_FOLDER = "response_folder"

LOCAL_DEV_FOLDER = TDLOCAL_FOLDER

FAKE_CONNECTION_PARAMETERS = "fake_connection_parameters"


def get_df_from_connection(partial_connection_string: str, table: str):
    """
    Helper function to get a DataFrame from a connection string and table name.
    """
    connection_string = (
        f"{partial_connection_string}UID={MSSQL_USER};PWD={DB_PASSWORD};"
    )
    connection_url = URL.create(
        "mssql+pyodbc", query={"odbc_connect": connection_string}
    )
    engine = create_engine(connection_url)
    query = f"SELECT * FROM {table}"
    df = pd.read_sql_query(query, engine)
    engine.dispose()
    return clean_polars_df(pl.from_pandas(df))


@pytest.mark.mssql
def test_mssql_class_parameters():
    mssql_destination = td.MSSQLDestination(FAKE_CONNECTION_PARAMETERS, "table")
    assert mssql_destination.connection_string == FAKE_CONNECTION_PARAMETERS + ";"
    assert mssql_destination.destination_table == ["table"]
    assert mssql_destination.if_table_exists == "append"
    assert mssql_destination.chunk_size == 1000
    assert mssql_destination.credentials is None
    assert mssql_destination.server is None
    assert mssql_destination.driver is None

    mssql_destination = td.MSSQLDestination(
        FAKE_CONNECTION_PARAMETERS,
        ["table1", "table2"],
        if_table_exists="replace",
        chunk_size=2,
    )
    assert mssql_destination.connection_string == FAKE_CONNECTION_PARAMETERS + ";"
    assert mssql_destination.destination_table == ["table1", "table2"]
    assert mssql_destination.if_table_exists == "replace"
    assert mssql_destination.chunk_size == 2
    assert mssql_destination.credentials is None
    assert mssql_destination.server is None
    assert mssql_destination.driver is None


@pytest.mark.mssql
def test_mssql_wrong_value_if_table_exists():
    with pytest.raises(ValueError):
        # noinspection PyTypeChecker
        td.MSSQLDestination(
            FAKE_CONNECTION_PARAMETERS,
            "table",
            if_table_exists="wrong_value",
        )


@pytest.mark.mssql
def test_mssql_wrong_table_type():
    with pytest.raises(TypeError):
        # noinspection PyTypeChecker
        td.MSSQLDestination(FAKE_CONNECTION_PARAMETERS, 42)


@pytest.mark.mssql
def test_mssql_wrong_table_list_type():
    with pytest.raises(TypeError):
        # noinspection PyTypeChecker
        td.MSSQLDestination(FAKE_CONNECTION_PARAMETERS, [42])


@pytest.mark.mssql
def test_mssql_chunk(tmp_path):
    mssql_destination = td.MSSQLDestination(FAKE_CONNECTION_PARAMETERS, "table")
    mssql_destination._tabsdata_internal_logger = logger
    df1 = pl.LazyFrame({"a": [1, 2, 3], "b": ["c", "d", "e"]})
    df2 = pl.LazyFrame({"c": [4, 5, 6], "d": ["hi", "hello", "bye"]})
    resulting_files = mssql_destination.chunk(str(tmp_path), df1, None, df2)
    assert str(tmp_path) in resulting_files[0]
    assert resulting_files[1] is None
    assert str(tmp_path) in resulting_files[2]
    result1 = pl.read_parquet(resulting_files[0])
    result2 = pl.read_parquet(resulting_files[2])
    assert df1.collect().equals(result1)
    assert df2.collect().equals(result2)


@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.mssql
def test_write_mssql(tmp_path, mssql_connection):
    table_name = f"write_mssql_table_{uuid.uuid4().hex[:8]}".replace("-", "_")
    destination = td.MSSQLDestination(
        mssql_connection,
        table_name,
        credentials=td.UserPasswordCredentials(MSSQL_USER, DB_PASSWORD),
        chunk_size=1,
    )
    destination._tabsdata_internal_logger = logger
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "example_file", "mock_table.parquet"
    )
    destination.write([mock_parquet_table])
    output = get_df_from_connection(mssql_connection, table_name)
    output = clean_polars_df(output)
    expected_output = pl.read_parquet(mock_parquet_table)
    expected_output = clean_polars_df(expected_output)
    assert output.equals(expected_output)


@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.mssql
def test_write_mssql_multiple_files(tmp_path, mssql_connection):
    table_name_0 = f"write_mssql_multiple_files_table_0_{uuid.uuid4().hex[:8]}".replace(
        "-", "_"
    )
    table_name_1 = f"write_mssql_multiple_files_table_1_{uuid.uuid4().hex[:8]}".replace(
        "-", "_"
    )
    destination = td.MSSQLDestination(
        mssql_connection,
        [table_name_0, table_name_1],
        credentials=td.UserPasswordCredentials(MSSQL_USER, DB_PASSWORD),
        chunk_size=1,
    )
    destination._tabsdata_internal_logger = logger
    mock_parquet_table_0 = os.path.join(
        TESTING_RESOURCES_FOLDER, "example_file", "mock_table.parquet"
    )
    df = pl.DataFrame({"a": [1, 2, 3], "b": ["c", "d", "e"]})
    mock_parquet_table_1 = os.path.join(tmp_path, "mock_table_1.parquet")
    df.write_parquet(mock_parquet_table_1)
    destination.write([mock_parquet_table_0, mock_parquet_table_1])

    # Check first
    output = get_df_from_connection(mssql_connection, table_name_0)
    output = clean_polars_df(output)
    expected_output = pl.read_parquet(mock_parquet_table_0)
    expected_output = clean_polars_df(expected_output)
    assert output.equals(expected_output)

    # Check second
    output = get_df_from_connection(mssql_connection, table_name_1)
    expected_output_file = mock_parquet_table_1
    expected_output = pl.read_parquet(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.slow
@pytest.mark.performance
@pytest.mark.requires_internet
@pytest.mark.mssql
def test_stream(tmp_path, size, mssql_connection):
    lf = get_lf(size)
    table_name = f"test_stream_table_{uuid.uuid4().hex[:8]}".replace("-", "_")
    chunk_size = int(size / 3) + 1
    mssql_destination = td.MSSQLDestination(
        mssql_connection,
        table_name,
        credentials=td.UserPasswordCredentials(MSSQL_USER, DB_PASSWORD),
        chunk_size=chunk_size,
    )
    mssql_destination._tabsdata_internal_logger = logger
    mssql_destination.stream(str(tmp_path), lf)
    created_table = get_df_from_connection(mssql_connection, table_name)
    assert created_table.equals(lf.collect())


@pytest.mark.slow
@pytest.mark.requires_internet
@pytest.mark.mssql
def test_stream_append(tmp_path, size, mssql_connection):
    lf = get_lf(size)
    table_name = f"test_stream_table_append_{uuid.uuid4().hex[:8]}".replace("-", "_")
    chunk_size = int(size / 3) + 1
    mssql_destination = td.MSSQLDestination(
        mssql_connection,
        table_name,
        credentials=td.UserPasswordCredentials(MSSQL_USER, DB_PASSWORD),
        if_table_exists="append",
        chunk_size=chunk_size,
    )
    mssql_destination._tabsdata_internal_logger = logger
    for i in range(3):
        mssql_destination.stream(str(tmp_path), lf)
        created_table = get_df_from_connection(mssql_connection, table_name)
        assert created_table.height == (i + 1) * size


@pytest.mark.slow
@pytest.mark.requires_internet
@pytest.mark.mssql
def test_stream_replace(tmp_path, size, mssql_connection):
    lf = get_lf(size)
    table_name = f"test_stream_table_replace_{uuid.uuid4().hex[:8]}".replace("-", "_")
    chunk_size = int(size / 3) + 1
    mssql_destination = td.MSSQLDestination(
        mssql_connection,
        table_name,
        credentials=td.UserPasswordCredentials(MSSQL_USER, DB_PASSWORD),
        if_table_exists="replace",
        chunk_size=chunk_size,
    )
    mssql_destination._tabsdata_internal_logger = logger
    for _ in range(3):
        mssql_destination.stream(str(tmp_path), lf)
        created_table = get_df_from_connection(mssql_connection, table_name)
        assert created_table.height == size


@pytest.mark.slow
@pytest.mark.requires_internet
@pytest.mark.mssql
def test_stream_multiple_lf(tmp_path, size, mssql_connection):
    lf = get_lf(size)
    table_name_1 = f"test_stream_multiple_lf_table_1_{uuid.uuid4().hex[:8]}".replace(
        "-", "_"
    )
    table_name_2 = f"test_stream_multiple_lf_table_2_{uuid.uuid4().hex[:8]}".replace(
        "-", "_"
    )
    chunk_size = int(size / 3) + 1
    mssql_destination = td.MSSQLDestination(
        mssql_connection,
        [table_name_1, table_name_2],
        credentials=td.UserPasswordCredentials(MSSQL_USER, DB_PASSWORD),
        chunk_size=chunk_size,
    )
    mssql_destination._tabsdata_internal_logger = logger

    mssql_destination.stream(str(tmp_path), lf, lf)
    created_table_1 = get_df_from_connection(mssql_connection, table_name_1)
    assert created_table_1.equals(lf.collect())

    created_table_2 = get_df_from_connection(mssql_connection, table_name_2)
    assert created_table_2.equals(lf.collect())


@pytest.mark.slow
@pytest.mark.requires_internet
@pytest.mark.mssql
def test_stream_different_len_raises_error(size, tmp_path, mssql_connection):
    lf = get_lf(size)
    chunk_size = int(size / 3) + 1
    mssql_destination = td.MSSQLDestination(
        mssql_connection,
        ["table1", "table2"],
        credentials=td.UserPasswordCredentials(MSSQL_USER, DB_PASSWORD),
        chunk_size=chunk_size,
    )
    mssql_destination._tabsdata_internal_logger = logger
    with pytest.raises(ValueError):
        mssql_destination.stream(str(tmp_path), lf)

    with pytest.raises(ValueError):
        mssql_destination.stream(str(tmp_path), lf, lf, lf)


@pytest.mark.slow
@pytest.mark.requires_internet
@pytest.mark.mssql
def test_single_element_table_list(tmp_path, size, mssql_connection):
    lf = get_lf(size)
    table_name = f"test_single_element_table_list_table_{uuid.uuid4().hex[:8]}".replace(
        "-", "_"
    )
    mssql_destination = td.MSSQLDestination(
        mssql_connection,
        [table_name],
        credentials=td.UserPasswordCredentials(MSSQL_USER, DB_PASSWORD),
        chunk_size=size + 10,
    )
    mssql_destination._tabsdata_internal_logger = logger

    mssql_destination.stream(str(tmp_path), lf)
    created_table = get_df_from_connection(mssql_connection, table_name)
    print("-" * 500)
    print(f"Created table: {created_table}")
    print(f"Frame: {lf.collect()}")
    print("-" * 500)
    assert created_table.equals(lf.collect())


@pytest.mark.mssql
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tabsserver
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_mssql(tmp_path, mssql_connection, mssql_version):
    from tests_tabsdata_mssql.testing_resources.test_output_mssql.example import (
        output_mssql,
    )

    logs_folder = os.path.join(
        LOCAL_DEV_FOLDER, f"{inspect.currentframe().f_code.co_name}_{mssql_version}"
    )
    table_name = f"output_mssql_table_{uuid.uuid4().hex[:8]}".replace("-", "_")
    destination = td.MSSQLDestination(
        mssql_connection,
        table_name,
        credentials=td.UserPasswordCredentials(MSSQL_USER, DB_PASSWORD),
    )
    output_mssql_copy = copy.deepcopy(output_mssql)
    output_mssql_copy.output = destination
    context_archive = create_bundle_archive(
        output_mssql_copy,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_mssql", "mock_table.parquet"
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
        temp_cwd=True,
    )
    assert result == 0
    assert os.path.exists(os.path.join(response_folder, RESPONSE_FILE_NAME))
    output = get_df_from_connection(mssql_connection, table_name)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_mssql",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.mssql
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tabsserver
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_multiple_outputs_mssql(tmp_path, mssql_connection, mssql_version):
    from tests_tabsdata_mssql.testing_resources.test_multiple_outputs_mssql.example import (
        multiple_outputs_mssql,
    )

    logs_folder = os.path.join(
        LOCAL_DEV_FOLDER, f"{inspect.currentframe().f_code.co_name}_{mssql_version}"
    )
    table_name_0 = f"multiple_outputs_mssql_table_0_{uuid.uuid4().hex[:8]}".replace(
        "-", "_"
    )
    table_name_1 = f"multiple_outputs_mssql_table_1_{uuid.uuid4().hex[:8]}".replace(
        "-", "_"
    )
    destination = td.MSSQLDestination(
        mssql_connection,
        [table_name_0, table_name_1],
        credentials=td.UserPasswordCredentials(MSSQL_USER, DB_PASSWORD),
    )
    multiple_outputs_mssql_copy = copy.deepcopy(multiple_outputs_mssql)
    multiple_outputs_mssql_copy.output = destination
    context_archive = create_bundle_archive(
        multiple_outputs_mssql_copy,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_multiple_outputs_mssql",
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
        temp_cwd=True,
    )
    assert result == 0
    assert os.path.exists(os.path.join(response_folder, RESPONSE_FILE_NAME))

    # Check first result
    output = get_df_from_connection(mssql_connection, table_name_0)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_multiple_outputs_mssql",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    # Check second result
    output = get_df_from_connection(mssql_connection, table_name_1)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_multiple_outputs_mssql",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.mssql
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tabsserver
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_mssql_with_none(tmp_path, mssql_connection, mssql_version):
    from tests_tabsdata_mssql.testing_resources.test_output_mssql_none.example import (
        output_mssql_none,
    )

    logs_folder = os.path.join(
        LOCAL_DEV_FOLDER, f"{inspect.currentframe().f_code.co_name}_{mssql_version}"
    )
    table_name = f"output_mssql_table_{uuid.uuid4().hex[:8]}".replace("-", "_")
    destination = td.MSSQLDestination(
        mssql_connection,
        table_name,
        credentials=td.UserPasswordCredentials(MSSQL_USER, DB_PASSWORD),
    )
    output_mssql_none_copy = copy.deepcopy(output_mssql_none)
    output_mssql_none_copy.output = destination
    context_archive = create_bundle_archive(
        output_mssql_none_copy,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_mssql_none", "mock_table.parquet"
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
        temp_cwd=True,
    )
    assert result == 0
    assert os.path.exists(os.path.join(response_folder, RESPONSE_FILE_NAME))
    with pytest.raises(Exception):
        get_df_from_connection(mssql_connection, table_name)
