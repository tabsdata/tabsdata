#
# Copyright 2026 Tabs Data Inc.
#

import copy
import json
import logging
import os
import uuid

import polars as pl
import pytest

import tabsdata as td
from tabsdata._secret import DirectSecret, EnvironmentSecret
from tabsdata._tabsserver.function.named_connection_utils import (
    CONNECTIONS_FOLDER,
    TD_URI_REPOSITORY,
)
from tabsdata.connector.databricks._connector import _table_fqn_4sql
from tabsdata.exceptions import ParameterValidationError
from test_tabsdata.conftest import (
    clean_polars_df,
    read_json_and_clean,
)
from test_tabsdata.fntest import tdfnrun
from test_tabsdata_databricks.conftest import TESTING_RESOURCES_FOLDER
from test_tabsdata_databricks.test_output_databricks import (
    DATABRICKS_BUDGET_SAFETY_TIMEOUT,
)
from test_tabsdata_databricks.test_output_databricks_basic.example import (
    output_databricks,
)
from test_tabsdata_databricks.test_multiple_outputs_databricks.example import (
    multiple_outputs_databricks,
)
from test_tabsdata_databricks.test_output_databricks_none.example import (
    output_databricks_none,
)
from test_tabsdata_databricks.test_output_databricks_list_none.example import (
    output_databricks_list_none,
)
from test_tabsdata_databricks.test_output_databricks_named_conn.example import (
    testing_tabsdata_function as output_databricks_named_conn_generator,
)

# noinspection PyPackageRequirements
from databricks.sql import ServerOperationError

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

pytestmark = pytest.mark.xdist_group("databricks-plugin_integration_seq")

DATABRICKS_NAMED_CONN_NAME = "test_databricks_named"


def _write_named_conn_repository(tmp_path, name, conn_type, conn_configs, conn_mode):
    """Create a temporary repository with a single named-connection JSON."""
    repo_dir = os.path.join(str(tmp_path), "repository")
    connections_dir = os.path.join(repo_dir, CONNECTIONS_FOLDER)
    os.makedirs(connections_dir, exist_ok=True)
    cx_dict = {
        "kind": "connectionDef",
        "apiVersion": "1.0",
        "name": name,
        "conn_mode": conn_mode,
        "conn_type": conn_type,
        "conn_configs": conn_configs,
    }
    connection_file = os.path.join(connections_dir, f"{name.lower()}.json")
    with open(connection_file, "w") as f:
        json.dump(cx_dict, f)
    return repo_dir


# --- DatabricksDest unit tests ---


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_databricksdest_default_options(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    tables = ["catalog.schema.table1", "catalog.schema.table2"]
    conn = td.DatabricksConn(host_url=host, token=token)
    dest = td.DatabricksDest(
        conn=conn,
        tables=tables,
        volume=volume,
        warehouse=warehouse_name,
    )
    assert dest.conn is conn
    assert dest.tables == tables
    assert dest.volume == volume
    assert dest.warehouse == warehouse_name
    assert dest.warehouse_id is None
    assert dest.catalog is None
    assert dest.schema is None
    assert dest.schema_strategy == "update"
    assert dest.if_table_exists == "append"
    assert dest._databricks_destination is not None
    assert dest._databricks_destination.host_url == host
    assert dest._databricks_destination.token == DirectSecret(token)


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_databricksdest_single_table(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    table = "catalog.schema.table1"
    dest = td.DatabricksDest(
        conn=td.DatabricksConn(host_url=host, token=token),
        tables=table,
        volume=volume,
        warehouse=warehouse_name,
    )
    assert dest.tables == [table]


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_databricksdest_single_table_no_catalog_fails(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    with pytest.raises(ValueError):
        td.DatabricksDest(
            conn=td.DatabricksConn(host_url=host, token=token),
            tables="schema.table1",
            volume=volume,
            warehouse=warehouse_name,
        )


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_databricksdest_catalog_and_schema_qualification(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]
    tables = ["catalog.schema.table1", "catalog.schema.table2", "table3"]
    expected = [
        "catalog.schema.table1",
        "catalog.schema.table2",
        f"{catalog}.{schema}.table3",
    ]
    dest = td.DatabricksDest(
        conn=td.DatabricksConn(host_url=host, token=token),
        tables=tables,
        volume=volume,
        warehouse=warehouse_name,
        catalog=catalog,
        schema=schema,
    )
    assert dest.tables == expected


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_databricksdest_conn_setter_rebuilds_inner(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    first_conn = td.DatabricksConn(host_url=host, token=token)
    dest = td.DatabricksDest(
        conn=first_conn,
        tables="catalog.schema.t1",
        volume=volume,
        warehouse=warehouse_name,
    )
    first_inner = dest._databricks_destination
    assert first_inner is not None

    new_conn = td.DatabricksConn(host_url="https://other.databricks.com", token="t2")
    dest.conn = new_conn
    assert dest.conn is new_conn
    assert dest._databricks_destination is not first_inner
    assert dest._databricks_destination.host_url == "https://other.databricks.com"
    assert dest._databricks_destination.token == DirectSecret("t2")


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_databricksdest_conn_setter_rejects_bad_type(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    dest = td.DatabricksDest(
        conn=td.DatabricksConn(host_url=host, token=token),
        tables="catalog.schema.t1",
        volume=volume,
        warehouse=warehouse_name,
    )
    with pytest.raises(ParameterValidationError):
        # noinspection PyTypeChecker
        dest.conn = "not_a_conn"


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_databricksdest_both_warehouses_raises(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    with pytest.raises(ValueError):
        td.DatabricksDest(
            conn=td.DatabricksConn(host_url=host, token=token),
            tables="catalog.schema.t1",
            volume=volume,
            warehouse=warehouse_name,
            warehouse_id="w0",
        )


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_databricksdest_no_warehouse_raises(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    volume = databricks_config["VOLUME"]
    with pytest.raises(ValueError):
        td.DatabricksDest(
            conn=td.DatabricksConn(host_url=host, token=token),
            tables="catalog.schema.t1",
            volume=volume,
        )


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_databricksdest_invalid_if_table_exists_raises(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    # The wrapper's @_td_validate_call rejects this at the pydantic boundary
    # (IfTableExistStrategySpec is a Literal) before the inner
    # DatabricksDestination's ValueError can fire.
    with pytest.raises(ParameterValidationError):
        # noinspection PyTypeChecker
        td.DatabricksDest(
            conn=td.DatabricksConn(host_url=host, token=token),
            tables="catalog.schema.t1",
            volume=volume,
            warehouse=warehouse_name,
            if_table_exists="not_a_strategy",
        )


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_databricksdest_invalid_schema_strategy_raises(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    # The wrapper's @_td_validate_call rejects this at the pydantic boundary
    # (SchemaStrategySpec is a Literal) before the inner
    # DatabricksDestination's ValueError can fire.
    with pytest.raises(ParameterValidationError):
        # noinspection PyTypeChecker
        td.DatabricksDest(
            conn=td.DatabricksConn(host_url=host, token=token),
            tables="catalog.schema.t1",
            volume=volume,
            warehouse=warehouse_name,
            schema_strategy="not_a_strategy",
        )


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_databricksdest_passthrough_setters_update_inner(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    dest = td.DatabricksDest(
        conn=td.DatabricksConn(host_url=host, token=token),
        tables="catalog.schema.t1",
        volume=volume,
        warehouse=warehouse_name,
    )
    inner = dest._databricks_destination
    assert inner is not None

    dest.tables = ["catalog.schema.t2", "catalog.schema.t3"]
    assert dest.tables == ["catalog.schema.t2", "catalog.schema.t3"]
    assert inner.tables == ["catalog.schema.t2", "catalog.schema.t3"]

    dest.volume = "new_volume"
    assert dest.volume == "new_volume"
    assert inner.volume == "new_volume"

    dest.warehouse = None
    dest.warehouse_id = "w0"
    assert dest.warehouse is None
    assert inner.warehouse is None
    assert dest.warehouse_id == "w0"
    assert inner.warehouse_id == "w0"

    dest.if_table_exists = "replace"
    assert dest.if_table_exists == "replace"
    assert inner.if_table_exists == "replace"

    dest.schema_strategy = "strict"
    assert dest.schema_strategy == "strict"
    assert inner.schema_strategy == "strict"


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_databricksdest_passthrough_setter_validation_propagates(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    dest = td.DatabricksDest(
        conn=td.DatabricksConn(host_url=host, token=token),
        tables="catalog.schema.t1",
        volume=volume,
        warehouse=warehouse_name,
    )
    with pytest.raises(ValueError):
        # noinspection PyTypeChecker
        dest.if_table_exists = "not_a_strategy"
    with pytest.raises(ValueError):
        # noinspection PyTypeChecker
        dest.schema_strategy = "not_a_strategy"
    with pytest.raises(TypeError):
        # noinspection PyTypeChecker
        dest.tables = 1


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_databricksdest_repr(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    dest = td.DatabricksDest(
        conn=td.DatabricksConn(host_url=host, token=token),
        tables="catalog.schema.t1",
        volume=volume,
        warehouse=warehouse_name,
    )
    assert "DatabricksDest" in repr(dest)
    assert "tables=" in repr(dest)


# --- Full pipeline e2e ---


@pytest.mark.databricks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_output_databricksdest(databricks_client, sql_conn, databricks_config):
    host = databricks_config["HOST"]
    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]
    table_name = f"test_output_databricksdest_table_{uuid.uuid4()}"
    fn = copy.deepcopy(output_databricks)
    # Swap the function's destination to a DatabricksDest. This exercises
    # bundling + stream() via the new wrapper.
    destination = td.DatabricksDest(
        conn=td.DatabricksConn(
            host_url=host,
            token=td.EnvironmentSecret(databricks_config["TOKEN_ENV"]),
        ),
        tables=table_name,
        volume=volume,
        warehouse=warehouse_name,
        catalog=catalog,
        schema=schema,
    )
    fn.output = destination

    full_table_name = _table_fqn_4sql(f"{catalog}.{schema}.{table_name}")

    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_databricks_basic",
        "mock_table.parquet",
    )
    fnrun = tdfnrun(fn, input_tables=mock_parquet_table)
    try:
        assert fnrun.status == 0

        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_databricks_basic",
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


# --- NamedConn e2e ---


@pytest.mark.databricks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_output_databricks_with_named_conn(
    databricks_config, databricks_client, sql_conn, tmp_path, monkeypatch
):
    repo_dir = _write_named_conn_repository(
        tmp_path,
        DATABRICKS_NAMED_CONN_NAME,
        "DatabricksConn",
        {
            "host_url": databricks_config["HOST"],
            "token": databricks_config["TOKEN"],
        },
        "dest",
    )
    monkeypatch.setenv(TD_URI_REPOSITORY, repo_dir)

    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]
    table_name = f"test_output_databricks_named_conn_{uuid.uuid4().hex[:12]}"
    full_table_name = _table_fqn_4sql(f"{catalog}.{schema}.{table_name}")

    testing_function = output_databricks_named_conn_generator(
        conn_name=DATABRICKS_NAMED_CONN_NAME,
        table_name=table_name,
        volume=volume,
        warehouse=warehouse_name,
        catalog=catalog,
        schema=schema,
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_databricks_basic",
        "mock_table.parquet",
    )

    fnrun = tdfnrun(
        function=testing_function,
        input_tables=mock_parquet_table,
    )
    try:
        assert fnrun.status == 0
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_databricks_basic",
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
@pytest.mark.tdfnrun
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_multiple_outputs_databricksdest(
    databricks_client, sql_conn, databricks_config
):
    host = databricks_config["HOST"]
    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]
    table_name_1 = (
        f"{catalog}.{schema}.test_multiple_outputs_databricksdest_1_{uuid.uuid4()}"
    )
    table_name_2 = (
        f"{catalog}.{schema}.test_multiple_outputs_databricksdest_2_{uuid.uuid4()}"
    )
    fn = copy.deepcopy(multiple_outputs_databricks)
    fn.output = td.DatabricksDest(
        conn=td.DatabricksConn(
            host_url=host,
            token=td.EnvironmentSecret(databricks_config["TOKEN_ENV"]),
        ),
        tables=[table_name_1, table_name_2],
        volume=volume,
        warehouse=warehouse_name,
    )

    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_multiple_outputs_databricks",
        "mock_table.parquet",
    )

    table_name_1 = _table_fqn_4sql(table_name_1)
    table_name_2 = _table_fqn_4sql(table_name_2)

    fnrun = tdfnrun(fn, input_tables=mock_parquet_table)
    try:
        assert fnrun.status == 0

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
@pytest.mark.tdfnrun
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_output_databricksdest_with_none(
    databricks_client, sql_conn, databricks_config
):
    host = databricks_config["HOST"]
    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]
    table_name = f"test_output_databricksdest_none_{uuid.uuid4()}"
    fn = copy.deepcopy(output_databricks_none)
    fn.output = td.DatabricksDest(
        conn=td.DatabricksConn(
            host_url=host,
            token=td.EnvironmentSecret(databricks_config["TOKEN_ENV"]),
        ),
        tables=table_name,
        volume=volume,
        warehouse=warehouse_name,
        catalog=catalog,
        schema=schema,
    )

    full_table_name = _table_fqn_4sql(f"{catalog}.{schema}.{table_name}")

    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_databricks_none", "mock_table.parquet"
    )
    fnrun = tdfnrun(fn, input_tables=mock_parquet_table)
    try:
        assert fnrun.status == 0

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
@pytest.mark.tdfnrun
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_output_databricksdest_with_list_none(
    databricks_client, sql_conn, databricks_config
):
    host = databricks_config["HOST"]
    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]
    table_name_1 = (
        f"{catalog}.{schema}.test_output_databricksdest_list_none_1_{uuid.uuid4()}"
    )
    table_name_2 = (
        f"{catalog}.{schema}.test_output_databricksdest_list_none_2_{uuid.uuid4()}"
    )
    fn = copy.deepcopy(output_databricks_list_none)
    fn.output = td.DatabricksDest(
        conn=td.DatabricksConn(
            host_url=host,
            token=td.EnvironmentSecret(databricks_config["TOKEN_ENV"]),
        ),
        tables=[table_name_1, table_name_2],
        volume=volume,
        warehouse=warehouse_name,
    )

    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_databricks_list_none",
        "mock_table.parquet",
    )

    table_name_1 = _table_fqn_4sql(table_name_1)
    table_name_2 = _table_fqn_4sql(table_name_2)

    fnrun = tdfnrun(fn, input_tables=mock_parquet_table)
    try:
        assert fnrun.status == 0

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
