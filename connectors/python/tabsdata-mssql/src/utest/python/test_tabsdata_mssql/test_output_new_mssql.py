#
# Copyright 2026 Tabs Data Inc.
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

import tabsdata as td
from tabsdata._tabsserver.function.response_utils import RESPONSE_FILE_NAME
from tabsdata._tabsserver.invoker import REQUEST_FILE_NAME
from tabsdata._tabsserver.invoker import invoke as tabsserver_main
from tabsdata._utils.bundle_utils import create_bundle_archive
from test_tabsdata.bootest import ROOT_FOLDER, TDLOCAL_FOLDER
from test_tabsdata.common.constants import PYTEST_DEFAULT_ENVIRONMENT_PREFIX
from test_tabsdata.conftest import (
    FUNCTION_DATA_FOLDER,
    LOCAL_PACKAGES_LIST,
    clean_polars_df,
    get_lf,
    read_json_and_clean,
    write_v2_function_yaml_file,
)
from test_tabsdata.fntest import tdfnrun
from test_tabsdata_mssql.conftest import (
    DB_PASSWORD,
    TESTING_RESOURCES_FOLDER,
    MSSQLConfig,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ROOT_PROJECT_DIR = ROOT_FOLDER
RESPONSE_FOLDER = "response_folder"

LOCAL_DEV_FOLDER = TDLOCAL_FOLDER

FAKE_CONNECTION_PARAMETERS = "fake_connection_parameters"

from test_tabsdata_mssql.test_multiple_outputs_new_mssql.example import (
    testing_tabsdata_function as multiple_outputs_new_mssql,
)
from test_tabsdata_mssql.test_output_new_mssql_basic.example import (
    testing_tabsdata_function as output_new_mssql_basic,
)


def get_df_from_connection(mssql_connection: MSSQLConfig, table: str):
    """
    Helper function to get a DataFrame from a connection string and table name.
    """
    connection_string = f"{mssql_connection.connection_string}UID={mssql_connection.credentials.user.secret_value};PWD={mssql_connection.credentials.password.secret_value};"
    connection_url = URL.create(
        "mssql+pyodbc", query={"odbc_connect": connection_string}
    )
    engine = create_engine(connection_url)
    query = f"SELECT * FROM {table}"
    df = pd.read_sql_query(query, engine)
    engine.dispose()
    return clean_polars_df(pl.from_pandas(df))


@pytest.mark.mssql
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.parametrize("chunk_size", [10000, 5])
@pytest.mark.parametrize("transactional", [True, False])
def test_output_new_mssql(mssql_connection, mssql_version, chunk_size, transactional):
    destination_tables = [
        f"output_new_mssql_table_{uuid.uuid4().hex[:8]}".replace("-", "_")
    ]
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_mssql_basic", "mock_table.parquet"
    )
    fnrun = tdfnrun(
        function=output_new_mssql_basic(
            connection_string=mssql_connection.connection_string,
            credentials=mssql_connection.credentials,
            destination_tables=destination_tables,
            chunk_size=chunk_size,
            transactional=transactional,
        ),
        input_tables=[mock_parquet_table],
    )

    assert fnrun.status == 0

    output = get_df_from_connection(mssql_connection, destination_tables[0])
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_mssql_basic",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.mssql
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_mssql_replace(mssql_connection, mssql_version):
    destination_tables = [
        f"output_new_mssql_table_{uuid.uuid4().hex[:8]}".replace("-", "_")
    ]
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_mssql_basic", "mock_table.parquet"
    )
    fnrun = tdfnrun(
        function=output_new_mssql_basic(
            connection_string=mssql_connection.connection_string,
            credentials=mssql_connection.credentials,
            destination_tables=destination_tables,
        ),
        input_tables=[mock_parquet_table],
    )

    assert fnrun.status == 0

    output = get_df_from_connection(mssql_connection, destination_tables[0])
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_mssql_basic",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    fnrun = tdfnrun(
        function=output_new_mssql_basic(
            connection_string=mssql_connection.connection_string,
            credentials=mssql_connection.credentials,
            destination_tables=destination_tables,
            if_table_exists="replace",
        ),
        input_tables=[mock_parquet_table],
    )

    assert fnrun.status == 0

    output = get_df_from_connection(mssql_connection, destination_tables[0])
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_mssql_basic",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.mssql
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.parametrize("chunk_size", [10000, 5])
@pytest.mark.parametrize("transactional", [True, False])
def test_multiple_outputs_new_mssql(
    mssql_connection, mssql_version, chunk_size, transactional
):
    destination_tables = [
        "multiple_outputs_new_mssql_table_0_"
        + f"{uuid.uuid4().hex[:8]}".replace("-", "_"),
        "multiple_outputs_new_mssql_table_1_"
        + f"{uuid.uuid4().hex[:8]}".replace("-", "_"),
    ]
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_multiple_outputs_new_mssql",
        "mock_table.parquet",
    )
    fnrun = tdfnrun(
        function=multiple_outputs_new_mssql(
            connection_string=mssql_connection.connection_string,
            credentials=mssql_connection.credentials,
            destination_tables=destination_tables,
            chunk_size=chunk_size,
            transactional=transactional,
        ),
        input_tables=[mock_parquet_table],
    )

    # Check first result
    output = get_df_from_connection(mssql_connection, destination_tables[0])
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_multiple_outputs_new_mssql",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    # Check second result
    output = get_df_from_connection(mssql_connection, destination_tables[1])
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_multiple_outputs_new_mssql",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)
