#
# Copyright 2025 Tabs Data Inc.
#

import inspect
import logging
import os
import uuid
from io import StringIO
from unittest import mock

import polars as pl

# noinspection PyPackageRequirements
import pytest

# noinspection PyPackageRequirements
from databricks.sql import ServerOperationError
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
from tests_tabsdata_databricks.conftest import (
    DATABRICKS_CATALOG,
    DATABRICKS_HOST,
    DATABRICKS_SCHEMA,
    DATABRICKS_TOKEN,
    DATABRICKS_VOLUME,
    DATABRICKS_WAREHOUSE_NAME,
    TESTING_RESOURCES_FOLDER,
)
from tests_tabsdata_databricks.testing_resources.test_multiple_outputs_databricks.example import (
    multiple_outputs_databricks,
)
from tests_tabsdata_databricks.testing_resources.test_output_databricks.example import (
    output_databricks,
)
from tests_tabsdata_databricks.testing_resources.test_output_databricks_list_none.example import (
    output_databricks_list_none,
)
from tests_tabsdata_databricks.testing_resources.test_output_databricks_none.example import (
    output_databricks_none,
)

import tabsdata as td
from tabsdata.secret import DirectSecret, EnvironmentSecret
from tabsdata.tabsserver.function.response_utils import RESPONSE_FILE_NAME
from tabsdata.tabsserver.invoker import REQUEST_FILE_NAME
from tabsdata.tabsserver.invoker import invoke as tabsserver_main
from tabsdata.utils.bundle_utils import create_bundle_archive

# noinspection PyProtectedMember
from tabsdata_databricks.connector import _table_fqn_4sql

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ROOT_PROJECT_DIR = ROOT_FOLDER
RESPONSE_FOLDER = "response_folder"

LOCAL_DEV_FOLDER = TDLOCAL_FOLDER


DATABRICKS_BUDGET_SAFETY_TIMEOUT = 120


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_class_initialization_default_options():
    tables = ["catalog.schema.table1", "catalog.schema.table2"]
    output = td.DatabricksDestination(
        DATABRICKS_HOST,
        DATABRICKS_TOKEN,
        tables,
        DATABRICKS_VOLUME,
        warehouse=DATABRICKS_WAREHOUSE_NAME,
    )
    assert output.host_url == DATABRICKS_HOST
    assert output.token == DirectSecret(DATABRICKS_TOKEN)
    assert output.tables == tables
    assert output.volume == DATABRICKS_VOLUME
    assert output.warehouse == DATABRICKS_WAREHOUSE_NAME
    assert output.warehouse_id is None
    assert output.catalog is None
    assert output.schema is None
    assert output.schema_strategy == "update"
    assert output.if_table_exists == "append"


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_class_initialization_single_table():
    table = "catalog.schema.table1"
    output = td.DatabricksDestination(
        DATABRICKS_HOST,
        DATABRICKS_TOKEN,
        table,
        DATABRICKS_VOLUME,
        warehouse=DATABRICKS_WAREHOUSE_NAME,
    )
    assert output.tables == [table]


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_class_initialization_single_table_no_catalog_fails():
    table = "schema.table1"
    with pytest.raises(ValueError):
        td.DatabricksDestination(
            DATABRICKS_HOST,
            DATABRICKS_TOKEN,
            table,
            DATABRICKS_VOLUME,
            warehouse=DATABRICKS_WAREHOUSE_NAME,
        )


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_class_initialization_single_table_no_schema_fails():
    table = "table1"
    with pytest.raises(ValueError):
        td.DatabricksDestination(
            DATABRICKS_HOST,
            DATABRICKS_TOKEN,
            table,
            DATABRICKS_VOLUME,
            warehouse=DATABRICKS_WAREHOUSE_NAME,
        )


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_class_initialization_tables_no_catalog_fails():
    tables = ["catalog.schema.table1", "catalog.schema.table2", "schema.table3"]
    with pytest.raises(ValueError):
        td.DatabricksDestination(
            DATABRICKS_HOST,
            DATABRICKS_TOKEN,
            tables,
            DATABRICKS_VOLUME,
            warehouse=DATABRICKS_WAREHOUSE_NAME,
        )


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_class_initialization_tables_no_schema_fails():
    tables = ["catalog.schema.table1", "catalog.schema.table2", "table3"]
    with pytest.raises(ValueError):
        td.DatabricksDestination(
            DATABRICKS_HOST,
            DATABRICKS_TOKEN,
            tables,
            DATABRICKS_VOLUME,
            warehouse=DATABRICKS_WAREHOUSE_NAME,
        )


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_class_initialization_tables_catalog():
    tables = ["catalog.schema.table1", "catalog.schema.table2", "schema.table3"]
    expected_tables = [
        "catalog.schema.table1",
        "catalog.schema.table2",
        f"{DATABRICKS_CATALOG}.schema.table3",
    ]
    output = td.DatabricksDestination(
        DATABRICKS_HOST,
        DATABRICKS_TOKEN,
        tables,
        DATABRICKS_VOLUME,
        warehouse=DATABRICKS_WAREHOUSE_NAME,
        catalog=DATABRICKS_CATALOG,
    )
    assert output.tables == expected_tables


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_class_initialization_tables_catalog_and_schema():
    tables = ["catalog.schema.table1", "catalog.schema.table2", "table3"]
    expected_tables = [
        "catalog.schema.table1",
        "catalog.schema.table2",
        f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.table3",
    ]
    output = td.DatabricksDestination(
        DATABRICKS_HOST,
        DATABRICKS_TOKEN,
        tables,
        DATABRICKS_VOLUME,
        warehouse=DATABRICKS_WAREHOUSE_NAME,
        catalog=DATABRICKS_CATALOG,
        schema=DATABRICKS_SCHEMA,
    )
    assert output.tables == expected_tables


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_class_initialization_all_options():
    tables = ["table1", "table2"]
    output = td.DatabricksDestination(
        DATABRICKS_HOST,
        EnvironmentSecret("DATABRICKS_TOKEN"),
        tables,
        DATABRICKS_VOLUME,
        catalog=DATABRICKS_CATALOG,
        schema=DATABRICKS_SCHEMA,
        schema_strategy="strict",
        if_table_exists="replace",
        warehouse_id="fake_id",
    )
    assert output.host_url == DATABRICKS_HOST
    assert output.token == EnvironmentSecret("DATABRICKS_TOKEN")
    assert output.tables == [
        f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{table}" for table in tables
    ]
    assert output.volume == DATABRICKS_VOLUME
    assert output.warehouse is None
    assert output.warehouse_id == "fake_id"
    assert output.catalog == DATABRICKS_CATALOG
    assert output.schema == DATABRICKS_SCHEMA
    assert output.schema_strategy == "strict"
    assert output.if_table_exists == "replace"


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_class_initialization_support_options():
    tables = ["catalog.schema.table1", "catalog.schema.table2"]
    output = td.DatabricksDestination(
        DATABRICKS_HOST,
        DATABRICKS_TOKEN,
        tables,
        DATABRICKS_VOLUME,
        warehouse=DATABRICKS_WAREHOUSE_NAME,
        support_append_create_table={"key": "value"},
    )
    assert output.kwargs == {"support_append_create_table": {"key": "value"}}
    assert output._support_append_create_table == {"key": "value"}


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_no_warehouse():
    tables = ["catalog.schema.table1", "catalog.schema.table2"]
    with pytest.raises(ValueError):
        td.DatabricksDestination(
            DATABRICKS_HOST,
            DATABRICKS_TOKEN,
            tables,
            DATABRICKS_VOLUME,
        )


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_both_warehouse():
    tables = ["catalog.schema.table1", "catalog.schema.table2"]
    with pytest.raises(ValueError):
        td.DatabricksDestination(
            DATABRICKS_HOST,
            DATABRICKS_TOKEN,
            tables,
            DATABRICKS_VOLUME,
            warehouse=DATABRICKS_WAREHOUSE_NAME,
            warehouse_id="fake_id",
        )


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_databricks_chunk(tmp_path):
    databricks_destination = td.DatabricksDestination(
        DATABRICKS_HOST,
        DATABRICKS_TOKEN,
        "table",
        DATABRICKS_VOLUME,
        warehouse=DATABRICKS_WAREHOUSE_NAME,
        catalog=DATABRICKS_CATALOG,
        schema=DATABRICKS_SCHEMA,
    )
    df1 = pl.LazyFrame({"a": [1, 2, 3], "b": ["c", "d", "e"]})
    df2 = pl.LazyFrame({"c": [4, 5, 6], "d": ["hi", "hello", "bye"]})
    resulting_files = databricks_destination.chunk(str(tmp_path), df1, None, df2)
    assert str(tmp_path) in resulting_files[0]
    assert resulting_files[1] is None
    assert str(tmp_path) in resulting_files[2]
    result1 = pl.read_parquet(resulting_files[0])
    result2 = pl.read_parquet(resulting_files[2])
    assert df1.collect().equals(result1)
    assert df2.collect().equals(result2)


@pytest.mark.slow
@pytest.mark.performance
@pytest.mark.requires_internet
@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_stream(tmp_path, size, databricks_client, sql_conn):
    lf = get_lf(size)
    table_name = (
        f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.test_stream_table_{uuid.uuid4()}"
    )
    databricks_destination = td.DatabricksDestination(
        DATABRICKS_HOST,
        DATABRICKS_TOKEN,
        table_name,
        DATABRICKS_VOLUME,
        warehouse=DATABRICKS_WAREHOUSE_NAME,
        catalog="fake_catalog",  # This should not affect execution as table is fully
        # qualified
        schema="fake_schema",  # This should not affect execution as table is fully
        # qualified
    )
    try:
        databricks_destination.stream(str(tmp_path), lf)

        table_name = _table_fqn_4sql(table_name)

        query = f"SELECT * FROM {table_name}"
        created_table = pl.read_database(query, sql_conn)
        assert created_table.drop("_rescued_data").equals(lf.collect())
    finally:
        # noinspection PyBroadException
        try:
            databricks_client.tables.delete(full_name=table_name)
        except Exception:
            pass


@pytest.mark.slow
@pytest.mark.requires_internet
@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_stream_append(tmp_path, size, databricks_client, sql_conn):
    lf = get_lf(size)
    table_name = (
        f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.test_stream_table_append"
        f"nd_{uuid.uuid4()}"
    )
    databricks_destination = td.DatabricksDestination(
        DATABRICKS_HOST,
        DATABRICKS_TOKEN,
        table_name,
        DATABRICKS_VOLUME,
        warehouse=DATABRICKS_WAREHOUSE_NAME,
        catalog="fake_catalog",  # This should not affect execution as table is fully
        # qualified
        schema="fake_schema",  # This should not affect execution as table is fully
        # qualified
    )

    table_name = _table_fqn_4sql(table_name)

    try:
        for i in range(3):
            databricks_destination.stream(str(tmp_path), lf)

            query = f"SELECT * FROM {table_name}"
            created_table = pl.read_database(query, sql_conn)
            assert created_table.height == (i + 1) * size
    finally:
        # noinspection PyBroadException
        try:
            databricks_client.tables.delete(full_name=table_name)
        except Exception:
            pass


@pytest.mark.slow
@pytest.mark.requires_internet
@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_stream_replace(tmp_path, size, databricks_client, sql_conn):
    lf = get_lf(size)
    table_name = (
        f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.test_stream_table_"
        f"replace_{uuid.uuid4()}"
    )
    databricks_destination = td.DatabricksDestination(
        DATABRICKS_HOST,
        DATABRICKS_TOKEN,
        table_name,
        DATABRICKS_VOLUME,
        if_table_exists="replace",
        warehouse=DATABRICKS_WAREHOUSE_NAME,
        catalog="fake_catalog",  # This should not affect execution as table is fully
        # qualified
        schema="fake_schema",  # This should not affect execution as table is fully
        # qualified
    )

    table_name = _table_fqn_4sql(table_name)

    try:
        for _ in range(3):
            databricks_destination.stream(str(tmp_path), lf)

            query = f"SELECT * FROM {table_name}"
            created_table = pl.read_database(query, sql_conn)
            assert created_table.height == size
    finally:
        # noinspection PyBroadException
        try:
            databricks_client.tables.delete(full_name=table_name)
        except Exception:
            pass


@pytest.mark.slow
@pytest.mark.requires_internet
@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_stream_multiple_lf(tmp_path, size, databricks_client, sql_conn):
    lf = get_lf(size)
    table_name_1 = f"test_stream_multiple_lf_table_1_{uuid.uuid4()}"
    table_name_2 = f"test_stream_multiple_lf_table_2_{uuid.uuid4()}"
    databricks_destination = td.DatabricksDestination(
        DATABRICKS_HOST,
        DATABRICKS_TOKEN,
        [table_name_1, table_name_2],
        DATABRICKS_VOLUME,
        warehouse=DATABRICKS_WAREHOUSE_NAME,
        catalog=DATABRICKS_CATALOG,
        schema=DATABRICKS_SCHEMA,
    )
    full_name_1 = _table_fqn_4sql(
        f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{table_name_1}"
    )
    full_name_2 = _table_fqn_4sql(
        f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{table_name_2}"
    )
    try:
        databricks_destination.stream(str(tmp_path), lf, lf)

        query = f"SELECT * FROM {full_name_1}"
        created_table_1 = pl.read_database(query, sql_conn)
        assert created_table_1.drop("_rescued_data").equals(lf.collect())

        query = f"SELECT * FROM {full_name_1}"
        created_table_2 = pl.read_database(query, sql_conn)
        assert created_table_2.drop("_rescued_data").equals(lf.collect())
    finally:
        # noinspection PyBroadException
        try:
            databricks_client.tables.delete(full_name=full_name_1)
        except Exception:
            pass
        # noinspection PyBroadException
        try:
            databricks_client.tables.delete(full_name=full_name_2)
        except Exception:
            pass


@pytest.mark.slow
@pytest.mark.requires_internet
@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_stream_different_len_raises_error(tmp_path, databricks_client):
    size = 25000
    lf = get_lf(size)
    databricks_destination = td.DatabricksDestination(
        DATABRICKS_HOST,
        DATABRICKS_TOKEN,
        ["table1", "table2"],
        DATABRICKS_VOLUME,
        warehouse=DATABRICKS_WAREHOUSE_NAME,
        catalog=DATABRICKS_CATALOG,
        schema=DATABRICKS_SCHEMA,
    )
    try:
        with pytest.raises(ValueError):
            databricks_destination.stream(str(tmp_path), lf)

        with pytest.raises(ValueError):
            databricks_destination.stream(str(tmp_path), lf, lf, lf)
    finally:
        # noinspection PyBroadException
        try:
            databricks_client.tables.delete(
                full_name=f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.table1"
            )
        except Exception:
            pass
        # noinspection PyBroadException
        try:
            databricks_client.tables.delete(
                full_name=f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.table2"
            )
        except Exception:
            pass


@pytest.mark.slow
@pytest.mark.requires_internet
@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_single_element_table_list(tmp_path, size, databricks_client, sql_conn):
    lf = get_lf(size)
    table_name = (
        f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.test_single_element_table_list_table"
        f"_{uuid.uuid4()}"
    )
    databricks_destination = td.DatabricksDestination(
        DATABRICKS_HOST,
        DATABRICKS_TOKEN,
        [table_name],
        DATABRICKS_VOLUME,
        warehouse=DATABRICKS_WAREHOUSE_NAME,
        catalog="fake_catalog",  # This should not affect execution as table is fully
        # qualified
        schema="fake_schema",  # This should not affect execution as table is fully
        # qualified
    )

    table_name = _table_fqn_4sql(table_name)

    try:
        databricks_destination.stream(str(tmp_path), lf)

        query = f"SELECT * FROM {table_name}"
        created_table = pl.read_database(query, sql_conn)
        assert created_table.drop("_rescued_data").equals(lf.collect())
    finally:
        # noinspection PyBroadException
        try:
            databricks_client.tables.delete(full_name=table_name)
        except Exception:
            pass


@pytest.mark.slow
@pytest.mark.tabsserver
@pytest.mark.requires_internet
@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_databricks(tmp_path, databricks_client, sql_conn):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    table_name = f"test_output_databricks_table_{uuid.uuid4()}"
    output_databricks.output.tables = table_name
    context_archive = create_bundle_archive(
        output_databricks,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    full_table_name = _table_fqn_4sql(
        f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{table_name}"
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_databricks", "mock_table.parquet"
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
    try:
        assert result == 0
        assert os.path.exists(os.path.join(response_folder, RESPONSE_FILE_NAME))

        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_databricks",
            "expected_result.json",
        )
        query = f"SELECT * FROM {full_table_name}"
        output = pl.read_database(query, sql_conn)
        expected_output = read_json_and_clean(expected_output_file)
        assert expected_output.equals(clean_polars_df(output.drop("_rescued_data")))
    finally:
        # noinspection PyBroadException
        try:
            databricks_client.tables.delete(full_name=full_table_name)
        except Exception:
            pass


@pytest.mark.slow
@pytest.mark.tabsserver
@pytest.mark.requires_internet
@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_multiple_outputs_databricks(tmp_path, databricks_client, sql_conn):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    table_name_1 = (
        f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}."
        "test_multiple_outputs_databricks_table_1"
        f"_{uuid.uuid4()}"
    )
    table_name_2 = (
        f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}."
        "test_multiple_outputs_databricks_table_2"
        f"_{uuid.uuid4()}"
    )
    multiple_outputs_databricks.output.tables = [table_name_1, table_name_2]
    context_archive = create_bundle_archive(
        multiple_outputs_databricks,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_multiple_outputs_databricks",
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

    table_name_1 = _table_fqn_4sql(table_name_1)
    table_name_2 = _table_fqn_4sql(table_name_2)

    try:
        assert result == 0
        assert os.path.exists(os.path.join(response_folder, RESPONSE_FILE_NAME))

        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_multiple_outputs_databricks",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)

        query = f"SELECT * FROM {table_name_1}"
        output = pl.read_database(query, sql_conn)
        assert expected_output.equals(clean_polars_df(output.drop("_rescued_data")))

        query = f"SELECT * FROM {table_name_2}"
        output = pl.read_database(query, sql_conn)
        assert expected_output.equals(clean_polars_df(output.drop("_rescued_data")))
    finally:
        # noinspection PyBroadException
        try:
            databricks_client.tables.delete(full_name=table_name_1)
        except Exception:
            pass
        # noinspection PyBroadException
        try:
            databricks_client.tables.delete(full_name=table_name_2)
        except Exception:
            pass


@pytest.mark.slow
@pytest.mark.tabsserver
@pytest.mark.requires_internet
@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_databricks_with_none(tmp_path, databricks_client, sql_conn):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    table_name = f"test_output_databricks_table_{uuid.uuid4()}"
    output_databricks.output.tables = table_name
    context_archive = create_bundle_archive(
        output_databricks_none,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    full_table_name = _table_fqn_4sql(
        f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{table_name}"
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_databricks", "mock_table.parquet"
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
    try:
        assert result == 0
        assert os.path.exists(os.path.join(response_folder, RESPONSE_FILE_NAME))

        query = f"SELECT * FROM {full_table_name}"
        try:
            output = pl.read_database(query, sql_conn)
            assert output.is_empty()
        except ServerOperationError as e:
            if "TABLE_OR_VIEW_NOT_FOUND" in str(e) or "SQLSTATE: 42P01" in str(e):
                pass
            else:
                raise
    finally:
        # noinspection PyBroadException
        try:
            databricks_client.tables.delete(full_name=full_table_name)
        except Exception:
            pass


@pytest.mark.slow
@pytest.mark.tabsserver
@pytest.mark.requires_internet
@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_databricks_with_list_none(tmp_path, databricks_client, sql_conn):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    table_name_1 = (
        f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}."
        "test_multiple_outputs_databricks_table_1"
        f"_{uuid.uuid4()}"
    )
    table_name_2 = (
        f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}."
        "test_multiple_outputs_databricks_table_2"
        f"_{uuid.uuid4()}"
    )
    multiple_outputs_databricks.output.tables = [table_name_1, table_name_2]
    context_archive = create_bundle_archive(
        output_databricks_list_none,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_multiple_outputs_databricks",
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

    table_name_1 = _table_fqn_4sql(table_name_1)
    table_name_2 = _table_fqn_4sql(table_name_2)

    try:
        assert result == 0
        assert os.path.exists(os.path.join(response_folder, RESPONSE_FILE_NAME))

        query = f"SELECT * FROM {table_name_1}"
        try:
            output = pl.read_database(query, sql_conn)
            assert output.is_empty()
        except ServerOperationError as e:
            if "TABLE_OR_VIEW_NOT_FOUND" in str(e) or "SQLSTATE: 42P01" in str(e):
                pass
            else:
                raise

        query = f"SELECT * FROM {table_name_2}"
        try:
            output = pl.read_database(query, sql_conn)
            assert output.is_empty()
        except ServerOperationError as e:
            if "TABLE_OR_VIEW_NOT_FOUND" in str(e) or "SQLSTATE: 42P01" in str(e):
                pass
            else:
                raise
    finally:
        # noinspection PyBroadException
        try:
            databricks_client.tables.delete(full_name=table_name_1)
        except Exception:
            pass
        # noinspection PyBroadException
        try:
            databricks_client.tables.delete(full_name=table_name_2)
        except Exception:
            pass
