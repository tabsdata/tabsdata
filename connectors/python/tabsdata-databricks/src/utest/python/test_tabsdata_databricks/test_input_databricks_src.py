#
# Copyright 2026 Tabs Data Inc.
#

import json
import logging
import os
import uuid

import polars as pl
import pytest

import tabsdata as td
from tabsdata._io.connections.named_connection.kind_registry import kind_for
from tabsdata._secret import DirectSecret, EnvironmentSecret
from tabsdata._tabsserver.function.named_connection_utils import (
    CONNECTIONS_FOLDER,
    TD_URI_REPOSITORY,
)
from tabsdata.connector.databricks._connector import _table_fqn_4sql
from tabsdata.exceptions import ParameterValidationError, SourceConfigurationError
from test_tabsdata.conftest import MOCK_PROJECT_ID
from test_tabsdata.fntest import tdfnrun
from test_tabsdata_databricks.test_input_databricks import (
    DATABRICKS_BUDGET_SAFETY_TIMEOUT,
    append_databricks_table,
    check_databricks_table,
    delete_databricks_table,
    generate_orders_data,
    generate_persons_data,
    write_databricks_table,
)
from test_tabsdata_databricks.test_input_databricks_named_conn.example import (
    testing_tabsdata_function as input_databricks_named_conn_generator,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

pytestmark = pytest.mark.xdist_group("databricks-plugin_integration_seq")

DATABRICKS_NAMED_CONN_NAME = "test_databricks_named"


def _write_named_conn_repository(tmp_path, name, conn_type, conn_configs, conn_mode):
    """Create a temporary repository with a single named-connection JSON."""
    repo_dir = os.path.join(str(tmp_path), "repository")
    connections_dir = os.path.join(repo_dir, CONNECTIONS_FOLDER, MOCK_PROJECT_ID)
    os.makedirs(connections_dir, exist_ok=True)
    cx_dict = {
        "kind": kind_for(conn_type, conn_mode),
        "apiVersion": "1.0",
        "name": name,
        "conn_configs": conn_configs,
    }
    connection_file = os.path.join(connections_dir, f"{name.lower()}.json")
    with open(connection_file, "w") as f:
        json.dump(cx_dict, f)
    return repo_dir


# --- DatabricksConn unit tests ---


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_databricksconn_token_wrapping_string(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    conn = td.DatabricksConn(host_url=host, token=token)
    assert conn.host_url == host
    assert conn.token == DirectSecret(token)


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_databricksconn_token_wrapping_secret(databricks_config):
    host = databricks_config["HOST"]
    conn = td.DatabricksConn(host_url=host, token=EnvironmentSecret("token"))
    assert conn.token == EnvironmentSecret("token")


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_databricksconn_bad_host_raises():
    with pytest.raises(ParameterValidationError):
        # noinspection PyTypeChecker
        td.DatabricksConn(host_url=1, token="t")


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_databricksconn_bad_token_raises():
    with pytest.raises(ParameterValidationError):
        # noinspection PyTypeChecker
        td.DatabricksConn(host_url="h", token=1)


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_databricksconn_setter_bad_host_raises(databricks_config):
    conn = td.DatabricksConn(
        host_url=databricks_config["HOST"], token=databricks_config["TOKEN"]
    )
    with pytest.raises(ParameterValidationError):
        # noinspection PyTypeChecker
        conn.host_url = 1


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_databricksconn_setter_bad_token_raises(databricks_config):
    conn = td.DatabricksConn(
        host_url=databricks_config["HOST"], token=databricks_config["TOKEN"]
    )
    with pytest.raises(ParameterValidationError):
        # noinspection PyTypeChecker
        conn.token = 1


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_databricksconn_load_from_dict_plain(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    conn = td.DatabricksConn._load_from_dict({
        "host_url": host,
        "token": token,
    })
    assert conn.host_url == host
    assert conn.token == DirectSecret(token)


# --- DatabricksSrc unit tests ---


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_databrickssrc_with_real_conn_properties(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    conn = td.DatabricksConn(host_url=host, token=token)
    query = "select * from catalog.schema.t1"
    src = td.DatabricksSrc(
        conn=conn,
        queries=query,
        warehouse=warehouse_name,
    )
    assert src.conn is conn
    assert src.queries == [query]
    assert src.warehouse == warehouse_name
    assert src.warehouse_id is None
    assert src.initial_values == {}
    # Inner source is built and reflects the conn's host/token.
    assert src._databricks_source is not None
    assert src._databricks_source.host_url == host
    assert src._databricks_source.token == DirectSecret(token)


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_databrickssrc_with_query_list_and_initial_values(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    conn = td.DatabricksConn(host_url=host, token=token)
    queries = ["select * from a", "select * from b where id > :last_id"]
    src = td.DatabricksSrc(
        conn=conn,
        queries=queries,
        initial_values={"last_id": 0},
        warehouse_id="w0",
    )
    assert src.queries == queries
    assert src.initial_values == {"last_id": 0}
    assert src.warehouse is None
    assert src.warehouse_id == "w0"


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_databrickssrc_conn_setter_rebuilds_inner_source(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    first_conn = td.DatabricksConn(host_url=host, token=token)
    src = td.DatabricksSrc(
        conn=first_conn,
        queries="select 1",
        warehouse=warehouse_name,
    )
    first_inner = src._databricks_source
    assert first_inner is not None

    new_conn = td.DatabricksConn(host_url="https://other.databricks.com", token="t2")
    src.conn = new_conn
    assert src.conn is new_conn
    # Reassigning conn rebuilds the inner DatabricksSource with the new
    # host/token.
    assert src._databricks_source is not first_inner
    assert src._databricks_source.host_url == "https://other.databricks.com"
    assert src._databricks_source.token == DirectSecret("t2")


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_databrickssrc_conn_setter_rejects_bad_type(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    src = td.DatabricksSrc(
        conn=td.DatabricksConn(host_url=host, token=token),
        queries="select 1",
        warehouse=warehouse_name,
    )
    with pytest.raises(ParameterValidationError):
        # noinspection PyTypeChecker
        src.conn = "not_a_conn"


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_databrickssrc_both_warehouses_raises(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    with pytest.raises(ValueError):
        td.DatabricksSrc(
            conn=td.DatabricksConn(host_url=host, token=token),
            queries="select 1",
            warehouse=warehouse_name,
            warehouse_id="w0",
        )


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_databrickssrc_no_warehouse_raises(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    with pytest.raises(ValueError):
        td.DatabricksSrc(
            conn=td.DatabricksConn(host_url=host, token=token),
            queries="select 1",
        )


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_databrickssrc_queries_wrong_type_raises(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    # The wrapper's @_td_validate_call rejects this at the pydantic boundary
    # before the inner DatabricksSource's TypeError can fire.
    with pytest.raises(ParameterValidationError):
        # noinspection PyTypeChecker
        td.DatabricksSrc(
            conn=td.DatabricksConn(host_url=host, token=token),
            queries=1,
            warehouse=warehouse_name,
        )


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_databrickssrc_initial_values_wrong_type_raises(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    # The wrapper's @_td_validate_call rejects this at the pydantic boundary
    # before the inner DatabricksSource's SourceConfigurationError can fire.
    with pytest.raises(ParameterValidationError):
        td.DatabricksSrc(
            conn=td.DatabricksConn(host_url=host, token=token),
            queries="select 1",
            initial_values=[1, 2, 3],
            warehouse=warehouse_name,
        )


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_databrickssrc_passthrough_setters_update_inner_source(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    src = td.DatabricksSrc(
        conn=td.DatabricksConn(host_url=host, token=token),
        queries="select 1",
        warehouse=warehouse_name,
    )
    inner = src._databricks_source
    assert inner is not None

    src.queries = ["select 2", "select 3"]
    assert src.queries == ["select 2", "select 3"]
    assert inner.queries == ["select 2", "select 3"]

    src.initial_values = {"k": "v"}
    assert src.initial_values == {"k": "v"}
    assert inner.initial_values == {"k": "v"}

    src.warehouse = None
    src.warehouse_id = "w0"
    assert src.warehouse is None
    assert inner.warehouse is None
    assert src.warehouse_id == "w0"
    assert inner.warehouse_id == "w0"


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_databrickssrc_passthrough_setter_validation_propagates(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    src = td.DatabricksSrc(
        conn=td.DatabricksConn(host_url=host, token=token),
        queries="select 1",
        warehouse=warehouse_name,
    )
    with pytest.raises(TypeError):
        # noinspection PyTypeChecker
        src.queries = 1
    with pytest.raises(SourceConfigurationError):
        # noinspection PyTypeChecker
        src.initial_values = [1, 2, 3]


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_databrickssrc_repr(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    src = td.DatabricksSrc(
        conn=td.DatabricksConn(host_url=host, token=token),
        queries="select 1",
        warehouse=warehouse_name,
    )
    assert "DatabricksSrc" in repr(src)
    assert "queries=" in repr(src)


# --- Full pipeline e2e ---


@pytest.mark.databricks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_databrickssrc_full_table_single_tdfnrun(
    tmp_path,
    databricks_client,
    databricks_config,
):
    host = databricks_config["HOST"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]
    df = generate_persons_data()
    table_name = (
        f"{catalog}.{schema}.test_databrickssrc_full_table_single_tdfnrun_"
        f"{uuid.uuid4().hex[:12]}"
    )
    table_sql = _table_fqn_4sql(table_name)
    try:
        write_databricks_table(databricks_config, tmp_path, table_name, df)

        @td.publisher(
            name="test_databrickssrc_full_table_single_tdfnrun",
            source=td.DatabricksSrc(
                conn=td.DatabricksConn(
                    host_url=host,
                    token=td.EnvironmentSecret(databricks_config["TOKEN_ENV"]),
                ),
                queries=f"select * from {table_sql}",
                warehouse=warehouse_name,
            ),
            tables="output",
        )
        def input_databricks(data: td.TableFrame) -> td.TableFrame:
            return data

        fnrun = tdfnrun(function=input_databricks)
        assert fnrun.status == 0

        output = fnrun.output
        assert len(output) == 1

        result_df = output[0].as_pl_df(with_sys_columns=False)
        check_databricks_table(result_df, df)
    finally:
        delete_databricks_table(databricks_client, table_name)


# --- NamedConn e2e ---


@pytest.mark.databricks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_input_databricks_with_named_conn(
    databricks_config, databricks_client, tmp_path, monkeypatch
):
    repo_dir = _write_named_conn_repository(
        tmp_path,
        DATABRICKS_NAMED_CONN_NAME,
        "DatabricksConn",
        {
            "host_url": databricks_config["HOST"],
            "token": databricks_config["TOKEN"],
        },
        "src",
    )
    monkeypatch.setenv(TD_URI_REPOSITORY, repo_dir)

    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]
    df = generate_persons_data()
    table_name = (
        f"{catalog}.{schema}.test_input_databricks_named_conn_"
        f"{uuid.uuid4().hex[:12]}"
    )
    table_sql = _table_fqn_4sql(table_name)

    try:
        write_databricks_table(databricks_config, tmp_path, table_name, df)

        testing_function = input_databricks_named_conn_generator(
            conn_name=DATABRICKS_NAMED_CONN_NAME,
            query=f"select * from {table_sql}",
            warehouse=warehouse_name,
        )

        fnrun = tdfnrun(function=testing_function)
        assert fnrun.status == 0

        output = fnrun.output
        assert len(output) == 1
        result_df = output[0].as_pl_df(with_sys_columns=False)
        check_databricks_table(result_df, df)
    finally:
        delete_databricks_table(databricks_client, table_name)


@pytest.mark.databricks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_databrickssrc_full_table_multiple_tdfnrun(
    tmp_path,
    databricks_client,
    databricks_config,
):
    host = databricks_config["HOST"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]
    df1 = generate_persons_data()
    df2 = generate_orders_data()
    uid = uuid.uuid4().hex[:12]
    table_name_1 = f"{catalog}.{schema}.test_databrickssrc_full_multi_1_{uid}"
    table_name_2 = f"{catalog}.{schema}.test_databrickssrc_full_multi_2_{uid}"
    table_sql_1 = _table_fqn_4sql(table_name_1)
    table_sql_2 = _table_fqn_4sql(table_name_2)
    try:
        write_databricks_table(databricks_config, tmp_path, table_name_1, df1)
        write_databricks_table(databricks_config, tmp_path, table_name_2, df2)

        @td.publisher(
            name="test_databrickssrc_full_table_multiple_tdfnrun",
            source=td.DatabricksSrc(
                conn=td.DatabricksConn(
                    host_url=host,
                    token=td.EnvironmentSecret(databricks_config["TOKEN_ENV"]),
                ),
                queries=[
                    f"select * from {table_sql_1}",
                    f"select * from {table_sql_2}",
                ],
                warehouse=warehouse_name,
            ),
            tables=["output1", "output2"],
        )
        def input_databricks_multi(
            data1: td.TableFrame, data2: td.TableFrame
        ) -> tuple[td.TableFrame, td.TableFrame]:
            return data1, data2

        fnrun = tdfnrun(function=input_databricks_multi)
        assert fnrun.status == 0

        output = fnrun.output
        assert len(output) == 2

        check_databricks_table(output[0].as_pl_df(with_sys_columns=False), df1)
        check_databricks_table(output[1].as_pl_df(with_sys_columns=False), df2)
    finally:
        delete_databricks_table(databricks_client, table_name_1)
        delete_databricks_table(databricks_client, table_name_2)


@pytest.mark.databricks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_databrickssrc_partial_table_single_tdfnrun(
    tmp_path,
    databricks_client,
    databricks_config,
):
    host = databricks_config["HOST"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]
    df = generate_persons_data()
    table_name = (
        f"{catalog}.{schema}.test_databrickssrc_partial_single_"
        f"{uuid.uuid4().hex[:12]}"
    )
    table_sql = _table_fqn_4sql(table_name)
    try:
        write_databricks_table(databricks_config, tmp_path, table_name, df)

        @td.publisher(
            name="test_databrickssrc_partial_table_single_tdfnrun",
            source=td.DatabricksSrc(
                conn=td.DatabricksConn(
                    host_url=host,
                    token=td.EnvironmentSecret(databricks_config["TOKEN_ENV"]),
                ),
                queries=f"select * from {table_sql} where id < 1024",
                warehouse=warehouse_name,
            ),
            tables="output",
        )
        def input_databricks_partial(data: td.TableFrame) -> td.TableFrame:
            return data

        fnrun = tdfnrun(function=input_databricks_partial)
        assert fnrun.status == 0

        output = fnrun.output
        assert len(output) == 1

        result_df = output[0].as_pl_df(with_sys_columns=False)
        expected = df.filter(pl.col("id") < 1024)
        check_databricks_table(result_df, expected)
    finally:
        delete_databricks_table(databricks_client, table_name)


@pytest.mark.databricks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_databrickssrc_partial_table_multiple_tdfnrun(
    tmp_path,
    databricks_client,
    databricks_config,
):
    host = databricks_config["HOST"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]
    df1 = generate_persons_data()
    df2 = generate_orders_data()
    uid = uuid.uuid4().hex[:12]
    table_name_1 = (
        f"{catalog}.{schema}.test_databrickssrc_partial_multi_1_{uid}"
    )
    table_name_2 = (
        f"{catalog}.{schema}.test_databrickssrc_partial_multi_2_{uid}"
    )
    table_sql_1 = _table_fqn_4sql(table_name_1)
    table_sql_2 = _table_fqn_4sql(table_name_2)
    try:
        write_databricks_table(databricks_config, tmp_path, table_name_1, df1)
        write_databricks_table(databricks_config, tmp_path, table_name_2, df2)

        @td.publisher(
            name="test_databrickssrc_partial_table_multiple_tdfnrun",
            source=td.DatabricksSrc(
                conn=td.DatabricksConn(
                    host_url=host,
                    token=td.EnvironmentSecret(databricks_config["TOKEN_ENV"]),
                ),
                queries=[
                    f"select * from {table_sql_1} where id < 1024",
                    f"select * from {table_sql_2} where id >= 1024",
                ],
                warehouse=warehouse_name,
            ),
            tables=["output1", "output2"],
        )
        def input_databricks_partial_multi(
            data1: td.TableFrame, data2: td.TableFrame
        ) -> tuple[td.TableFrame, td.TableFrame]:
            return data1, data2

        fnrun = tdfnrun(function=input_databricks_partial_multi)
        assert fnrun.status == 0

        output = fnrun.output
        assert len(output) == 2

        check_databricks_table(
            output[0].as_pl_df(with_sys_columns=False),
            df1.filter(pl.col("id") < 1024),
        )
        check_databricks_table(
            output[1].as_pl_df(with_sys_columns=False),
            df2.filter(pl.col("id") >= 1024),
        )
    finally:
        delete_databricks_table(databricks_client, table_name_1)
        delete_databricks_table(databricks_client, table_name_2)


@pytest.mark.databricks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_databrickssrc_initial_values_tdfnrun(
    tmp_path,
    databricks_client,
    databricks_config,
):
    host = databricks_config["HOST"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]
    df = generate_persons_data()
    table_name = (
        f"{catalog}.{schema}.test_databrickssrc_iv_tdfnrun_"
        f"{uuid.uuid4().hex[:12]}"
    )
    table_sql = _table_fqn_4sql(table_name)
    try:
        write_databricks_table(databricks_config, tmp_path, table_name, df)

        @td.publisher(
            name="test_databrickssrc_initial_values_tdfnrun",
            source=td.DatabricksSrc(
                conn=td.DatabricksConn(
                    host_url=host,
                    token=td.EnvironmentSecret(databricks_config["TOKEN_ENV"]),
                ),
                queries=f"select * from {table_sql} where id >= :min_id",
                initial_values={"min_id": 1024},
                warehouse=warehouse_name,
            ),
            tables="output",
        )
        def input_databricks_iv(data: td.TableFrame) -> tuple[td.TableFrame, dict]:
            return data, {"min_id": 1024}

        fnrun = tdfnrun(function=input_databricks_iv)
        assert fnrun.status == 0

        output = fnrun.output
        assert len(output) == 1

        result_df = output[0].as_pl_df(with_sys_columns=False)
        expected = df.filter(pl.col("id") >= 1024)
        check_databricks_table(result_df, expected)
    finally:
        delete_databricks_table(databricks_client, table_name)


@pytest.mark.databricks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_databrickssrc_incremental_ingestion(
    tmp_path,
    databricks_client,
    databricks_config,
):
    batch_size = 512
    rounds = 4

    host = databricks_config["HOST"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]
    persons_data = generate_persons_data(batch_size * rounds)
    batches = [
        persons_data.filter(
            (pl.col("id") >= i * batch_size)
            & (pl.col("id") < (i + 1) * batch_size)
        )
        for i in range(rounds)
    ]
    uid = uuid.uuid4().hex[:12]
    table_name = f"{catalog}.{schema}.test_databrickssrc_incremental_{uid}"
    table_sql = _table_fqn_4sql(table_name)
    try:
        write_databricks_table(databricks_config, tmp_path, table_name, batches[0])
        current_offset = -1
        for round_idx in range(rounds):
            if round_idx > 0:
                append_databricks_table(
                    databricks_config, tmp_path, table_name, batches[round_idx]
                )
            expected_batch = batches[round_idx]
            min_id_for_query = current_offset

            @td.publisher(
                name=f"test_databrickssrc_incremental_{round_idx}",
                source=td.DatabricksSrc(
                    conn=td.DatabricksConn(
                        host_url=host,
                        token=td.EnvironmentSecret(databricks_config["TOKEN_ENV"]),
                    ),
                    queries=f"select * from {table_sql} where id > :min_id",
                    initial_values={"min_id": min_id_for_query},
                    warehouse=warehouse_name,
                ),
                tables="output",
            )
            def incremental_read(
                data: td.TableFrame,
            ) -> tuple[td.TableFrame, dict]:
                return data, {"min_id": min_id_for_query}

            fnrun = tdfnrun(function=incremental_read)
            assert fnrun.status == 0, (
                f"Round {round_idx} failed with status {fnrun.status}"
            )

            output = fnrun.output
            assert len(output) == 1

            result_df = output[0].as_pl_df(with_sys_columns=False)
            check_databricks_table(result_df, expected_batch)

            current_offset = expected_batch["id"].max()

    finally:
        delete_databricks_table(databricks_client, table_name)
