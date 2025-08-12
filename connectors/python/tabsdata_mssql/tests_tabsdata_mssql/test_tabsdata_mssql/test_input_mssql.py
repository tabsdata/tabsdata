#
# Copyright 2025 Tabs Data Inc.
#

import copy
import inspect
import logging
import os
from io import StringIO
from unittest import mock

import polars as pl

# noinspection PyPackageRequirements
import pytest

# noinspection PyPackageRequirements
from tests_tabsdata_mssql.conftest import (
    DB_PASSWORD,
    INVOICE_HEADER_DF,
    INVOICE_ITEM_DF,
    MSSQL_USER,
    TESTING_RESOURCES_FOLDER,
)
from tests_tabsdata_mssql.testing_resources.test_input_mssql.example import input_mssql
from tests_tabsdata_mssql.testing_resources.test_input_mssql_initial_values.example import (
    input_mssql_initial_values,
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
    read_json_and_clean,
    write_v2_yaml_file,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ROOT_PROJECT_DIR = ROOT_FOLDER
RESPONSE_FOLDER = "response_folder"

LOCAL_DEV_FOLDER = TDLOCAL_FOLDER

FAKE_CONNECTION_PARAMETERS = "fake_connection_parameters"


@pytest.mark.mssql
def test_mssql_class_parameters():
    source = td.MSSQLSource(FAKE_CONNECTION_PARAMETERS, "fake query")
    assert source.connection_string == FAKE_CONNECTION_PARAMETERS + ";"
    assert source.query == ["fake query"]
    assert source.chunk_size == 1000
    assert source.credentials is None
    assert source.server is None
    assert source.driver is None
    assert source.initial_values == {}

    source = td.MSSQLSource(
        FAKE_CONNECTION_PARAMETERS,
        ["query1", "query2"],
        chunk_size=2,
        initial_values={"key": "value"},
    )
    assert source.connection_string == FAKE_CONNECTION_PARAMETERS + ";"
    assert source.query == ["query1", "query2"]
    assert source.chunk_size == 2
    assert source.credentials is None
    assert source.server is None
    assert source.driver is None
    assert source.initial_values == {"key": "value"}


@pytest.mark.mssql
def test_mssql_wrong_query_type():
    with pytest.raises(TypeError):
        # noinspection PyTypeChecker
        td.MSSQLSource(FAKE_CONNECTION_PARAMETERS, 42)


@pytest.mark.mssql
def test_mssql_wrong_query_list_type():
    with pytest.raises(TypeError):
        # noinspection PyTypeChecker
        td.MSSQLSource(FAKE_CONNECTION_PARAMETERS, [42])


@pytest.mark.mssql
@pytest.mark.requires_internet
@pytest.mark.slow
def test_chunk(tmp_path, mssql_connection):
    source = td.MSSQLSource(
        mssql_connection,
        "SELECT * FROM INVOICE_HEADER",
        credentials=td.UserPasswordCredentials(MSSQL_USER, DB_PASSWORD),
        chunk_size=1,
    )
    source._tabsdata_internal_logger = logger
    [result] = source.chunk(str(tmp_path))
    assert os.path.isfile(result)
    output = pl.read_parquet(result)
    output = clean_polars_df(output)
    assert output.equals(INVOICE_HEADER_DF)


@pytest.mark.mssql
@pytest.mark.requires_internet
@pytest.mark.slow
def test_chunk_multiple_queries(tmp_path, mssql_connection):
    source = td.MSSQLSource(
        mssql_connection,
        ["SELECT * FROM INVOICE_HEADER", "SELECT * FROM INVOICE_ITEM"],
        credentials=td.UserPasswordCredentials(MSSQL_USER, DB_PASSWORD),
        chunk_size=1,
    )
    source._tabsdata_internal_logger = logger
    [result1, result2] = source.chunk(str(tmp_path))
    assert os.path.isfile(result1)
    output = pl.read_parquet(result1)
    output = clean_polars_df(output)
    assert output.equals(INVOICE_HEADER_DF)

    assert os.path.isfile(result2)
    output = pl.read_parquet(result2)
    output = clean_polars_df(output)
    assert output.equals(INVOICE_ITEM_DF)


@pytest.mark.mssql
@pytest.mark.requires_internet
@pytest.mark.slow
def test_chunk_multiple_queries_with_initial_values(tmp_path, mssql_connection):
    source = td.MSSQLSource(
        mssql_connection,
        [
            "SELECT * FROM INVOICE_HEADER where id > :number",
            "SELECT * FROM INVOICE_ITEM where id > :number",
        ],
        credentials=td.UserPasswordCredentials(MSSQL_USER, DB_PASSWORD),
        chunk_size=1,
        initial_values={"number": 2},
    )
    source._tabsdata_internal_logger = logger
    [result1, result2] = source.chunk(str(tmp_path))
    assert os.path.isfile(result1)
    output = pl.read_parquet(result1)
    output = clean_polars_df(output)
    assert output.height == INVOICE_HEADER_DF.height - 2

    assert os.path.isfile(result2)
    output = pl.read_parquet(result2)
    output = clean_polars_df(output)
    assert output.height == INVOICE_ITEM_DF.height - 2


@pytest.mark.mssql
@pytest.mark.requires_internet
@pytest.mark.slow
def test_chunk_initial_values_no_result(tmp_path, mssql_connection):
    source = td.MSSQLSource(
        mssql_connection,
        [
            "SELECT * FROM INVOICE_HEADER where id > :number",
        ],
        credentials=td.UserPasswordCredentials(MSSQL_USER, DB_PASSWORD),
        chunk_size=1,
        initial_values={"number": 100},
    )
    source._tabsdata_internal_logger = logger

    [result] = source.chunk(str(tmp_path))
    assert os.path.isfile(result)
    output = pl.read_parquet(result)
    output = clean_polars_df(output)
    assert output.is_empty()


@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.mssql
@pytest.mark.tabsserver
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_input_mssql(mssql_connection, tmp_path, mssql_version):
    logs_folder = os.path.join(
        LOCAL_DEV_FOLDER, f"{inspect.currentframe().f_code.co_name}_{mssql_version}"
    )
    input_mssql_modified = copy.deepcopy(input_mssql)
    source = td.MSSQLSource(
        mssql_connection,
        query=[
            "select * from INVOICE_HEADER",
            "select * from INVOICE_ITEM",
        ],
        credentials=td.UserPasswordCredentials(MSSQL_USER, DB_PASSWORD),
    )
    input_mssql_modified.input = source
    context_archive = create_bundle_archive(
        input_mssql_modified, local_packages=LOCAL_PACKAGES_LIST, save_location=tmp_path
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file1 = os.path.join(tmp_path, "output1.parquet")
    output_file2 = os.path.join(tmp_path, "output2.parquet")
    path_to_output_initial_values = os.path.join(tmp_path, "initial_values.parquet")
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
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
        "test_input_mssql",
        "expected_result1.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    assert os.path.isfile(output_file2)
    output = pl.read_parquet(output_file2)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_mssql",
        "expected_result2.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)
    assert not os.path.isfile(path_to_output_initial_values)


@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.mssql
@pytest.mark.tabsserver
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_input_mssql_initial_values(mssql_connection, tmp_path, mssql_version):
    logs_folder = os.path.join(
        LOCAL_DEV_FOLDER, f"{inspect.currentframe().f_code.co_name}_{mssql_version}"
    )
    input_mssql_initial_values_nothing_stored = copy.deepcopy(
        input_mssql_initial_values
    )
    source = td.MSSQLSource(
        mssql_connection,
        [
            "SELECT * FROM INVOICE_HEADER where id > :number",
            "SELECT * from INVOICE_ITEM where id > :number",
        ],
        credentials=td.UserPasswordCredentials(MSSQL_USER, DB_PASSWORD),
        initial_values={"number": 2},
    )
    input_mssql_initial_values_nothing_stored.input = source
    context_archive = create_bundle_archive(
        input_mssql_initial_values_nothing_stored,
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
    write_v2_yaml_file(
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
        "test_input_mssql_initial_values",
        "expected_result1.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    assert os.path.isfile(output_file2)
    output = pl.read_parquet(output_file2)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_mssql_initial_values",
        "expected_result2.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)
    assert os.path.isfile(path_to_output_initial_values)
    output_initial_values = pl.read_parquet(path_to_output_initial_values)
    assert output_initial_values.equals(pl.DataFrame({"number": 3}))


@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.mssql
@pytest.mark.tabsserver
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_input_mssql_initial_values_stored_number_0(
    mssql_connection, tmp_path, mssql_version
):
    logs_folder = os.path.join(
        LOCAL_DEV_FOLDER, f"{inspect.currentframe().f_code.co_name}_{mssql_version}"
    )
    input_mssql_initial_values_stored_0 = copy.deepcopy(input_mssql_initial_values)
    source = td.MSSQLSource(
        mssql_connection,
        [
            "SELECT * FROM INVOICE_HEADER where id > :number",
            "SELECT * from INVOICE_ITEM where id > :number",
        ],
        credentials=td.UserPasswordCredentials(MSSQL_USER, DB_PASSWORD),
        initial_values={"number": 2},
    )
    input_mssql_initial_values_stored_0.input = source
    context_archive = create_bundle_archive(
        input_mssql_initial_values_stored_0,
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
        "test_input_mssql_initial_values",
        "mock_number_0.parquet",
    )
    path_to_output_initial_values = os.path.join(tmp_path, "initial_values.parquet")
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
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
        "test_input_mssql_initial_values",
        "expected_result1_number_0.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    assert os.path.isfile(output_file2)
    output = pl.read_parquet(output_file2)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_mssql_initial_values",
        "expected_result2_number_0.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)
    assert os.path.isfile(path_to_output_initial_values)
    output_initial_values = pl.read_parquet(path_to_output_initial_values)
    assert output_initial_values.equals(pl.DataFrame({"number": 3}))


@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.mssql
@pytest.mark.tabsserver
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_input_mssql_initial_values_stored_number_2(
    mssql_connection, tmp_path, mssql_version
):
    logs_folder = os.path.join(
        LOCAL_DEV_FOLDER, f"{inspect.currentframe().f_code.co_name}_{mssql_version}"
    )
    input_mssql_initial_values_stored_2 = copy.deepcopy(input_mssql_initial_values)
    source = td.MSSQLSource(
        mssql_connection,
        [
            "SELECT * FROM INVOICE_HEADER where id > :number",
            "SELECT * from INVOICE_ITEM where id > :number",
        ],
        credentials=td.UserPasswordCredentials(MSSQL_USER, DB_PASSWORD),
        initial_values={"number": 2},
    )
    input_mssql_initial_values_stored_2.input = source
    context_archive = create_bundle_archive(
        input_mssql_initial_values_stored_2,
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
        "test_input_mssql_initial_values",
        "mock_number_2.parquet",
    )
    path_to_output_initial_values = os.path.join(tmp_path, "initial_values.parquet")
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
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
        "test_input_mssql_initial_values",
        "expected_result1.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    assert os.path.isfile(output_file2)
    output = pl.read_parquet(output_file2)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_mssql_initial_values",
        "expected_result2.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)
    assert os.path.isfile(path_to_output_initial_values)
    output_initial_values = pl.read_parquet(path_to_output_initial_values)
    assert output_initial_values.equals(pl.DataFrame({"number": 3}))
