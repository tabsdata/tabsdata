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

import polars as pl

# noinspection PyPackageRequirements
import pytest

# noinspection PyPackageRequirements
from databricks.sql import ServerOperationError

import tabsdata as td
from tabsdata._secret import DirectSecret, EnvironmentSecret
from tabsdata._tabsserver.function.response_utils import RESPONSE_FILE_NAME
from tabsdata._tabsserver.invoker import REQUEST_FILE_NAME
from tabsdata._tabsserver.invoker import invoke as tabsserver_main
from tabsdata._utils.bundle_utils import create_bundle_archive

# noinspection PyProtectedMember
from tabsdata_databricks._connector import _table_fqn_4sql
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
from tests_tabsdata_databricks.conftest import TESTING_RESOURCES_FOLDER
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

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ROOT_PROJECT_DIR = ROOT_FOLDER
RESPONSE_FOLDER = "response_folder"

LOCAL_DEV_FOLDER = TDLOCAL_FOLDER


DATABRICKS_BUDGET_SAFETY_TIMEOUT = 600


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_class_initialization_default_options(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    tables = ["catalog.schema.table1", "catalog.schema.table2"]
    output = td.DatabricksDestination(
        host,
        token,
        tables,
        volume,
        warehouse=warehouse_name,
    )
    assert output.host_url == host
    assert output.token == DirectSecret(token)
    assert output.tables == tables
    assert output.volume == volume
    assert output.warehouse == warehouse_name
    assert output.warehouse_id is None
    assert output.catalog is None
    assert output.schema is None
    assert output.schema_strategy == "update"
    assert output.if_table_exists == "append"


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_class_initialization_single_table(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    table = "catalog.schema.table1"
    output = td.DatabricksDestination(
        host,
        token,
        table,
        volume,
        warehouse=warehouse_name,
    )
    assert output.tables == [table]


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_class_initialization_single_table_no_catalog_fails(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    table = "schema.table1"
    with pytest.raises(ValueError):
        td.DatabricksDestination(
            host,
            token,
            table,
            volume,
            warehouse=warehouse_name,
        )


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_class_initialization_single_table_no_schema_fails(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    table = "table1"
    with pytest.raises(ValueError):
        td.DatabricksDestination(
            host,
            token,
            table,
            volume,
            warehouse=warehouse_name,
        )


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_class_initialization_tables_no_catalog_fails(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    tables = ["catalog.schema.table1", "catalog.schema.table2", "schema.table3"]
    with pytest.raises(ValueError):
        td.DatabricksDestination(
            host,
            token,
            tables,
            volume,
            warehouse=warehouse_name,
        )


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_class_initialization_tables_no_schema_fails(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    tables = ["catalog.schema.table1", "catalog.schema.table2", "table3"]
    with pytest.raises(ValueError):
        td.DatabricksDestination(
            host,
            token,
            tables,
            volume,
            warehouse=warehouse_name,
        )


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_class_initialization_tables_catalog(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    tables = ["catalog.schema.table1", "catalog.schema.table2", "schema.table3"]
    expected_tables = [
        "catalog.schema.table1",
        "catalog.schema.table2",
        f"{catalog}.schema.table3",
    ]
    output = td.DatabricksDestination(
        host,
        token,
        tables,
        volume,
        warehouse=warehouse_name,
        catalog=catalog,
    )
    assert output.tables == expected_tables


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_class_initialization_tables_catalog_and_schema(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]
    tables = ["catalog.schema.table1", "catalog.schema.table2", "table3"]
    expected_tables = [
        "catalog.schema.table1",
        "catalog.schema.table2",
        f"{catalog}.{schema}.table3",
    ]
    output = td.DatabricksDestination(
        host,
        token,
        tables,
        volume,
        warehouse=warehouse_name,
        catalog=catalog,
        schema=schema,
    )
    assert output.tables == expected_tables


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_class_initialization_all_options(databricks_config):
    host = databricks_config["HOST"]
    volume = databricks_config["VOLUME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]
    tables = ["table1", "table2"]
    output = td.DatabricksDestination(
        host,
        EnvironmentSecret("token"),
        tables,
        volume,
        catalog=catalog,
        schema=schema,
        schema_strategy="strict",
        if_table_exists="replace",
        warehouse_id="fake_id",
    )
    assert output.host_url == host
    assert output.token == EnvironmentSecret("token")
    assert output.tables == [f"{catalog}.{schema}.{table}" for table in tables]
    assert output.volume == volume
    assert output.warehouse is None
    assert output.warehouse_id == "fake_id"
    assert output.catalog == catalog
    assert output.schema == schema
    assert output.schema_strategy == "strict"
    assert output.if_table_exists == "replace"


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_class_initialization_support_options(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    tables = ["catalog.schema.table1", "catalog.schema.table2"]
    output = td.DatabricksDestination(
        host,
        token,
        tables,
        volume,
        warehouse=warehouse_name,
        support_append_create_table={"key": "value"},
    )
    assert output.kwargs == {"support_append_create_table": {"key": "value"}}
    assert output._support_append_create_table == {"key": "value"}


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_no_warehouse(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    volume = databricks_config["VOLUME"]
    tables = ["catalog.schema.table1", "catalog.schema.table2"]
    with pytest.raises(ValueError):
        td.DatabricksDestination(
            host,
            token,
            tables,
            volume,
        )


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_both_warehouse(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    tables = ["catalog.schema.table1", "catalog.schema.table2"]
    with pytest.raises(ValueError):
        td.DatabricksDestination(
            host,
            token,
            tables,
            volume,
            warehouse=warehouse_name,
            warehouse_id="fake_id",
        )


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_databricks_chunk(tmp_path, databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]
    databricks_destination = td.DatabricksDestination(
        host,
        token,
        "table",
        volume,
        warehouse=warehouse_name,
        catalog=catalog,
        schema=schema,
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


@pytest.mark.databricks
@pytest.mark.performance
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_stream(tmp_path, size, databricks_client, sql_conn, databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]
    lf = get_lf(size)
    table_name = f"{catalog}.{schema}.test_stream_table_{uuid.uuid4()}"
    databricks_destination = td.DatabricksDestination(
        host,
        token,
        table_name,
        volume,
        warehouse=warehouse_name,
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


@pytest.mark.databricks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_stream_append(tmp_path, size, databricks_client, sql_conn, databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]
    lf = get_lf(size)
    table_name = f"{catalog}.{schema}.test_stream_table_appendnd_{uuid.uuid4()}"
    databricks_destination = td.DatabricksDestination(
        host,
        token,
        table_name,
        volume,
        warehouse=warehouse_name,
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


@pytest.mark.databricks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_stream_replace(tmp_path, size, databricks_client, sql_conn, databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]
    lf = get_lf(size)
    table_name = f"{catalog}.{schema}.test_stream_table_replace_{uuid.uuid4()}"
    databricks_destination = td.DatabricksDestination(
        host,
        token,
        table_name,
        volume,
        if_table_exists="replace",
        warehouse=warehouse_name,
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


@pytest.mark.databricks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_stream_multiple_lf(
    tmp_path, size, databricks_client, sql_conn, databricks_config
):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]
    lf = get_lf(size)
    table_name_1 = f"test_stream_multiple_lf_table_1_{uuid.uuid4()}"
    table_name_2 = f"test_stream_multiple_lf_table_2_{uuid.uuid4()}"
    databricks_destination = td.DatabricksDestination(
        host,
        token,
        [table_name_1, table_name_2],
        volume,
        warehouse=warehouse_name,
        catalog=catalog,
        schema=schema,
    )
    full_name_1 = _table_fqn_4sql(f"{catalog}.{schema}.{table_name_1}")
    full_name_2 = _table_fqn_4sql(f"{catalog}.{schema}.{table_name_2}")
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


@pytest.mark.databricks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_stream_different_len_raises_error(
    tmp_path, databricks_client, databricks_config
):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]
    size = 25000
    lf = get_lf(size)
    databricks_destination = td.DatabricksDestination(
        host,
        token,
        ["table1", "table2"],
        volume,
        warehouse=warehouse_name,
        catalog=catalog,
        schema=schema,
    )
    try:
        with pytest.raises(ValueError):
            databricks_destination.stream(str(tmp_path), lf)

        with pytest.raises(ValueError):
            databricks_destination.stream(str(tmp_path), lf, lf, lf)
    finally:
        # noinspection PyBroadException
        try:
            databricks_client.tables.delete(full_name=f"{catalog}.{schema}.table1")
        except Exception:
            pass
        # noinspection PyBroadException
        try:
            databricks_client.tables.delete(full_name=f"{catalog}.{schema}.table2")
        except Exception:
            pass


@pytest.mark.databricks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_single_element_table_list(
    tmp_path, size, databricks_client, sql_conn, databricks_config
):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]
    lf = get_lf(size)
    table_name = (
        f"{catalog}.{schema}.test_single_element_table_list_table_{uuid.uuid4()}"
    )
    databricks_destination = td.DatabricksDestination(
        host,
        token,
        [table_name],
        volume,
        warehouse=warehouse_name,
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


@pytest.mark.databricks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tabsserver
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_databricks(tmp_path, databricks_client, sql_conn, databricks_config):
    host = databricks_config["HOST"]
    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    table_name = f"test_output_databricks_table_{uuid.uuid4()}"
    output_databricks_copy = copy.deepcopy(output_databricks)
    destination = td.DatabricksDestination(
        host,
        td.EnvironmentSecret(databricks_config["TOKEN_ENV"]),
        table_name,
        volume,
        warehouse=warehouse_name,
        catalog=catalog,
        schema=schema,
    )
    output_databricks_copy.output = destination
    context_archive = create_bundle_archive(
        output_databricks_copy,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    full_table_name = _table_fqn_4sql(f"{catalog}.{schema}.{table_name}")

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


@pytest.mark.databricks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tabsserver
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_multiple_outputs_databricks(
    tmp_path, databricks_client, sql_conn, databricks_config
):
    host = databricks_config["HOST"]
    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    table_name_1 = (
        f"{catalog}.{schema}.test_multiple_outputs_databricks_table_1_{uuid.uuid4()}"
    )
    table_name_2 = (
        f"{catalog}.{schema}.test_multiple_outputs_databricks_table_2_{uuid.uuid4()}"
    )
    destination = td.DatabricksDestination(
        host,
        td.EnvironmentSecret(databricks_config["TOKEN_ENV"]),
        [table_name_1, table_name_2],
        volume,
        warehouse=warehouse_name,
    )
    multiple_outputs_databricks.output = destination
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


@pytest.mark.databricks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tabsserver
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_databricks_with_none(
    tmp_path, databricks_client, sql_conn, databricks_config
):
    host = databricks_config["HOST"]
    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    table_name = f"test_output_databricks_none_table_{uuid.uuid4()}"
    destination = td.DatabricksDestination(
        host,
        td.EnvironmentSecret(databricks_config["TOKEN_ENV"]),
        table_name,
        volume,
        warehouse=warehouse_name,
        catalog=catalog,
        schema=schema,
    )
    output_databricks_none.output = destination
    context_archive = create_bundle_archive(
        output_databricks_none,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    full_table_name = _table_fqn_4sql(f"{catalog}.{schema}.{table_name}")

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_databricks_none", "mock_table.parquet"
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


@pytest.mark.databricks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tabsserver
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_databricks_with_list_none(
    tmp_path, databricks_client, sql_conn, databricks_config
):
    host = databricks_config["HOST"]
    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    table_name_1 = (
        f"{catalog}.{schema}.test_output_databricks_list_none_table_1_{uuid.uuid4()}"
    )
    table_name_2 = (
        f"{catalog}.{schema}.test_output_databricks_list_none_table_2_{uuid.uuid4()}"
    )
    destination = td.DatabricksDestination(
        host,
        td.EnvironmentSecret(databricks_config["TOKEN_ENV"]),
        [table_name_1, table_name_2],
        volume,
        warehouse=warehouse_name,
    )
    output_databricks_list_none.output = destination
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
        "test_output_databricks_list_none",
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
