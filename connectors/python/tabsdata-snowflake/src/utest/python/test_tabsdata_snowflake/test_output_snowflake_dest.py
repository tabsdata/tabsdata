#
# Copyright 2026 Tabs Data Inc.
#

import copy
import json
import logging
import os
import uuid

import polars as pl

# noinspection PyPackageRequirements
import pytest

import tabsdata as td
from tabsdata._secret import DirectSecret, EnvironmentSecret
from tabsdata._tabsserver.function.named_connection_utils import (
    CONNECTIONS_FOLDER,
    TD_URI_REPOSITORY,
)
from tabsdata.exceptions import ParameterValidationError
from test_tabsdata.conftest import (
    clean_polars_df,
    read_json_and_clean,
)
from test_tabsdata.fntest import tdfnrun
from test_tabsdata_snowflake.conftest import TESTING_RESOURCES_FOLDER
from test_tabsdata_snowflake.test_output_snowflake_basic.example import (
    output_snowflake,
)
from test_tabsdata_snowflake.test_multiple_outputs_snowflake.example import (
    multiple_outputs_snowflake,
)
from test_tabsdata_snowflake.test_output_snowflake_none.example import (
    output_snowflake_none,
)
from test_tabsdata_snowflake.test_output_snowflake_list_none.example import (
    output_snowflake_list_none,
)
from test_tabsdata_snowflake.test_output_snowflake_named_conn.example import (
    testing_tabsdata_function as output_snowflake_named_conn_generator,
)

# noinspection PyPackageRequirements
from snowflake.connector.errors import ProgrammingError

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

SNOWFLAKE_NAMED_CONN_NAME = "test_snowflake_named"


def _write_named_conn_repository(tmp_path, name, conn_type, conn_configs):
    """Create a temporary repository with a single named-connection JSON."""
    repo_dir = os.path.join(str(tmp_path), "repository")
    connections_dir = os.path.join(repo_dir, CONNECTIONS_FOLDER)
    os.makedirs(connections_dir, exist_ok=True)
    cx_dict = {
        "name": name,
        "type": conn_type,
        "conn_configs": conn_configs,
    }
    connection_file = os.path.join(connections_dir, f"{name.lower()}.json")
    with open(connection_file, "w") as f:
        json.dump(cx_dict, f)
    return repo_dir


def _fake_conn() -> td.SnowflakeConn:
    return td.SnowflakeConn(
        account="a",
        user="u",
        password="p",
        role="r",
        database="d",
        schema="s",
        warehouse="w",
    )


# --- SnowflakeConn unit tests ---


@pytest.mark.snowflake
@pytest.mark.unit
def test_snowflakeconn_with_strings():
    conn = _fake_conn()
    assert conn.account == DirectSecret("a")
    assert conn.user == DirectSecret("u")
    assert conn.password == DirectSecret("p")
    assert conn.role == DirectSecret("r")
    assert conn.database == DirectSecret("d")
    assert conn.schema == DirectSecret("s")
    assert conn.warehouse == DirectSecret("w")


@pytest.mark.snowflake
@pytest.mark.unit
def test_snowflakeconn_with_secrets():
    conn = td.SnowflakeConn(
        account=EnvironmentSecret("ACC"),
        user=EnvironmentSecret("USR"),
        password=EnvironmentSecret("PWD"),
        role=EnvironmentSecret("ROLE"),
        database=EnvironmentSecret("DB"),
        schema=EnvironmentSecret("SCH"),
        warehouse=EnvironmentSecret("WH"),
    )
    assert conn.account == EnvironmentSecret("ACC")
    assert conn.user == EnvironmentSecret("USR")
    assert conn.password == EnvironmentSecret("PWD")
    assert conn.role == EnvironmentSecret("ROLE")
    assert conn.database == EnvironmentSecret("DB")
    assert conn.schema == EnvironmentSecret("SCH")
    assert conn.warehouse == EnvironmentSecret("WH")


@pytest.mark.snowflake
@pytest.mark.unit
def test_snowflakeconn_bad_account_raises():
    with pytest.raises(ParameterValidationError):
        # noinspection PyTypeChecker
        td.SnowflakeConn(
            account=1,
            user="u",
            password="p",
            role="r",
            database="d",
            schema="s",
            warehouse="w",
        )


@pytest.mark.snowflake
@pytest.mark.unit
def test_snowflakeconn_setter_validation_raises():
    conn = _fake_conn()
    with pytest.raises(ParameterValidationError):
        # noinspection PyTypeChecker
        conn.password = 1


@pytest.mark.snowflake
@pytest.mark.unit
def test_snowflakeconn_load_from_dict_plain():
    conn = td.SnowflakeConn._load_from_dict(
        {
            "account": "a",
            "user": "u",
            "password": "p",
            "role": "r",
            "database": "d",
            "schema": "s",
            "warehouse": "w",
        }
    )
    assert conn.account == DirectSecret("a")
    assert conn.user == DirectSecret("u")
    assert conn.password == DirectSecret("p")
    assert conn.role == DirectSecret("r")
    assert conn.database == DirectSecret("d")
    assert conn.schema == DirectSecret("s")
    assert conn.warehouse == DirectSecret("w")


@pytest.mark.snowflake
@pytest.mark.unit
def test_snowflakeconn_load_from_dict_with_nested_secrets():
    conn = td.SnowflakeConn._load_from_dict(
        {
            "account": "a",
            "user": {"environment-secret": {"environment_variable_name": "USR"}},
            "password": "p",
            "role": "r",
            "database": "d",
            "schema": "s",
            "warehouse": "w",
        }
    )
    assert conn.user == EnvironmentSecret("USR")
    assert conn.account == DirectSecret("a")


# --- SnowflakeDest unit tests ---


@pytest.mark.snowflake
@pytest.mark.unit
def test_snowflakedest_default_options():
    conn = _fake_conn()
    dest = td.SnowflakeDest(conn=conn, destination_table="table")
    assert dest.conn is conn
    assert dest.destination_table == ["table"]
    assert dest.if_table_exists == "append"
    assert dest.stage is None
    # Inner destination is built and its connection_parameters dict mirrors
    # the conn's fields.
    assert dest._snowflake_destination is not None
    inner_params = dest._snowflake_destination.connection_parameters
    assert inner_params["account"] == DirectSecret("a")
    assert inner_params["user"] == DirectSecret("u")
    assert inner_params["password"] == DirectSecret("p")
    assert inner_params["role"] == DirectSecret("r")
    assert inner_params["database"] == DirectSecret("d")
    assert inner_params["schema"] == DirectSecret("s")
    assert inner_params["warehouse"] == DirectSecret("w")


@pytest.mark.snowflake
@pytest.mark.unit
def test_snowflakedest_with_all_options():
    dest = td.SnowflakeDest(
        conn=_fake_conn(),
        destination_table=["t1", "t2"],
        if_table_exists="replace",
        stage="my_stage",
        support_purge="FALSE",
    )
    assert dest.destination_table == ["t1", "t2"]
    assert dest.if_table_exists == "replace"
    assert dest.stage == "my_stage"
    # Support kwarg threaded through to the inner destination.
    assert dest._snowflake_destination._support_purge == "FALSE"


@pytest.mark.snowflake
@pytest.mark.unit
def test_snowflakedest_conn_setter_rebuilds_inner():
    first_conn = _fake_conn()
    dest = td.SnowflakeDest(conn=first_conn, destination_table="table")
    first_inner = dest._snowflake_destination
    assert first_inner is not None

    new_conn = td.SnowflakeConn(
        account="a2",
        user="u2",
        password="p2",
        role="r2",
        database="d2",
        schema="s2",
        warehouse="w2",
    )
    dest.conn = new_conn
    assert dest.conn is new_conn
    assert dest._snowflake_destination is not first_inner
    new_params = dest._snowflake_destination.connection_parameters
    assert new_params["account"] == DirectSecret("a2")
    assert new_params["warehouse"] == DirectSecret("w2")


@pytest.mark.snowflake
@pytest.mark.unit
def test_snowflakedest_conn_setter_rejects_bad_type():
    dest = td.SnowflakeDest(conn=_fake_conn(), destination_table="table")
    with pytest.raises(ParameterValidationError):
        # noinspection PyTypeChecker
        dest.conn = "not_a_conn"


@pytest.mark.snowflake
@pytest.mark.unit
def test_snowflakedest_destination_table_wrong_type_raises():
    # The wrapper's @_td_validate_call rejects at the pydantic boundary
    # before the inner SnowflakeDestination's TypeError can fire.
    with pytest.raises(ParameterValidationError):
        # noinspection PyTypeChecker
        td.SnowflakeDest(conn=_fake_conn(), destination_table=42)


@pytest.mark.snowflake
@pytest.mark.unit
def test_snowflakedest_invalid_if_table_exists_raises():
    # IfTableExistStrategySpec is a Literal, so pydantic rejects at the
    # wrapper boundary before the inner ValueError can fire.
    with pytest.raises(ParameterValidationError):
        # noinspection PyTypeChecker
        td.SnowflakeDest(
            conn=_fake_conn(),
            destination_table="table",
            if_table_exists="nope",
        )


@pytest.mark.snowflake
@pytest.mark.unit
def test_snowflakedest_passthrough_setters_update_inner():
    dest = td.SnowflakeDest(conn=_fake_conn(), destination_table="table")
    inner = dest._snowflake_destination
    assert inner is not None

    dest.destination_table = ["a", "b"]
    assert dest.destination_table == ["a", "b"]
    assert inner.destination_table == ["a", "b"]

    dest.if_table_exists = "replace"
    assert dest.if_table_exists == "replace"
    assert inner.if_table_exists == "replace"

    dest.stage = "my_stage"
    assert dest.stage == "my_stage"
    assert inner.stage == "my_stage"


@pytest.mark.snowflake
@pytest.mark.unit
def test_snowflakedest_passthrough_setter_validation_propagates():
    dest = td.SnowflakeDest(conn=_fake_conn(), destination_table="table")
    # The passthrough setters have no @_td_validate_call, so they forward
    # to the inner destination's setters and surface the inner errors.
    with pytest.raises(ValueError):
        dest.if_table_exists = "nope"
    with pytest.raises(TypeError):
        # noinspection PyTypeChecker
        dest.destination_table = 42


@pytest.mark.snowflake
@pytest.mark.unit
def test_snowflakedest_repr():
    dest = td.SnowflakeDest(conn=_fake_conn(), destination_table="table")
    assert "SnowflakeDest" in repr(dest)
    assert "destination_table=" in repr(dest)


# --- Live write() ---


def _snowflakeconn_from_config(snowflake_config) -> td.SnowflakeConn:
    params = snowflake_config["CONNECTION_PARAMETERS"]
    return td.SnowflakeConn(
        account=params["account"],
        user=params["user"],
        password=params["password"],
        role=params["role"],
        database=params["database"],
        schema=params["schema"],
        warehouse=params["warehouse"],
    )


@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.snowflake
def test_snowflakedest_write(tmp_path, snowflake_connection, snowflake_config):
    table_name = f"snowflakedest_write_table_{uuid.uuid4()}".replace("-", "_")
    dest = td.SnowflakeDest(
        conn=_snowflakeconn_from_config(snowflake_config),
        destination_table=table_name,
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "example_file", "mock_table.parquet"
    )
    try:
        dest._snowflake_destination.write([mock_parquet_table])
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
        cursor = snowflake_connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        cursor.close()


# --- Full pipeline e2e ---


@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.snowflake
@pytest.mark.tdfnrun
def test_output_snowflakedest(snowflake_connection, snowflake_config):
    table_name = f"output_snowflakedest_table_{uuid.uuid4()}".replace("-", "_")
    fn = copy.deepcopy(output_snowflake)
    fn.output = td.SnowflakeDest(
        conn=_snowflakeconn_from_config(snowflake_config),
        destination_table=table_name,
    )

    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_snowflake_basic", "mock_table.parquet"
    )
    fnrun = tdfnrun(fn, input_tables=mock_parquet_table)
    try:
        assert fnrun.status == 0
        cursor = snowflake_connection.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        results = cursor.fetchall()
        output = pl.DataFrame(
            results, ["Duration", "Pulse", "Maxpulse", "Calories"], orient="row"
        )
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_snowflake_basic",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        cursor = snowflake_connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        cursor.close()


# --- NamedConn e2e ---


@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.snowflake
@pytest.mark.tdfnrun
def test_output_snowflake_with_named_conn(
    snowflake_config, snowflake_connection, tmp_path, monkeypatch
):
    repo_dir = _write_named_conn_repository(
        tmp_path,
        SNOWFLAKE_NAMED_CONN_NAME,
        "SnowflakeConn",
        {
            "account": os.environ.get(snowflake_config["ACCOUNT_ENV"]),
            "user": os.environ.get(snowflake_config["USER_ENV"]),
            "password": os.environ.get(snowflake_config["PAT_ENV"]),
            "role": os.environ.get(snowflake_config["ROLE_ENV"]),
            "database": os.environ.get(snowflake_config["DATABASE_ENV"]),
            "schema": os.environ.get(snowflake_config["SCHEMA_ENV"]),
            "warehouse": os.environ.get(snowflake_config["WAREHOUSE_ENV"]),
        },
    )
    monkeypatch.setenv(TD_URI_REPOSITORY, repo_dir)

    table_name = f"namedconn_table_{uuid.uuid4()}".replace("-", "_")
    testing_function = output_snowflake_named_conn_generator(
        conn_name=SNOWFLAKE_NAMED_CONN_NAME,
        table_name=table_name,
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_snowflake_basic", "mock_table.parquet"
    )

    fnrun = tdfnrun(
        function=testing_function,
        input_tables=mock_parquet_table,
    )
    try:
        assert fnrun.status == 0
        cursor = snowflake_connection.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        results = cursor.fetchall()
        output = pl.DataFrame(
            results, ["Duration", "Pulse", "Maxpulse", "Calories"], orient="row"
        )
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_snowflake_basic",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        cursor = snowflake_connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        cursor.close()


@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.snowflake
@pytest.mark.tdfnrun
def test_multiple_outputs_snowflakedest(snowflake_connection, snowflake_config):
    table_name_0 = f"multiple_outputs_snowflakedest_0_{uuid.uuid4()}".replace("-", "_")
    table_name_1 = f"multiple_outputs_snowflakedest_1_{uuid.uuid4()}".replace("-", "_")
    fn = copy.deepcopy(multiple_outputs_snowflake)
    fn.output = td.SnowflakeDest(
        conn=_snowflakeconn_from_config(snowflake_config),
        destination_table=[table_name_0, table_name_1],
    )

    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_multiple_outputs_snowflake",
        "mock_table.parquet",
    )
    fnrun = tdfnrun(fn, input_tables=mock_parquet_table)
    try:
        assert fnrun.status == 0

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

        cursor = snowflake_connection.cursor()
        cursor.execute(f"SELECT * FROM {table_name_1}")
        results = cursor.fetchall()
        output = pl.DataFrame(
            results, ["Duration", "Pulse", "Maxpulse", "Calories"], orient="row"
        )
        output = clean_polars_df(output)
        assert output.equals(expected_output)
    finally:
        # noinspection PyBroadException
        try:
            cursor = snowflake_connection.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {table_name_0}")
            cursor.close()
        except Exception:
            pass
        # noinspection PyBroadException
        try:
            cursor = snowflake_connection.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {table_name_1}")
            cursor.close()
        except Exception:
            pass


@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.snowflake
@pytest.mark.tdfnrun
def test_output_snowflakedest_with_none(snowflake_connection, snowflake_config):
    table_name = f"output_snowflakedest_none_{uuid.uuid4()}".replace("-", "_")
    fn = copy.deepcopy(output_snowflake_none)
    fn.output = td.SnowflakeDest(
        conn=_snowflakeconn_from_config(snowflake_config),
        destination_table=table_name,
    )

    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_snowflake_none", "mock_table.parquet"
    )
    fnrun = tdfnrun(fn, input_tables=mock_parquet_table)
    try:
        assert fnrun.status == 0
        cursor = snowflake_connection.cursor()
        try:
            cursor.execute(f"SELECT * FROM {table_name}")
            result = cursor.fetchall()
            assert len(result) == 0
        except ProgrammingError as e:
            if "002003" in str(e) and "42S02" in str(e):
                pass
            else:
                raise
    finally:
        cursor = snowflake_connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        cursor.close()


@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.snowflake
@pytest.mark.tdfnrun
def test_output_snowflakedest_with_list_none(snowflake_connection, snowflake_config):
    table_name_0 = f"output_snowflakedest_list_none_0_{uuid.uuid4()}".replace("-", "_")
    table_name_1 = f"output_snowflakedest_list_none_1_{uuid.uuid4()}".replace("-", "_")
    fn = copy.deepcopy(output_snowflake_list_none)
    fn.output = td.SnowflakeDest(
        conn=_snowflakeconn_from_config(snowflake_config),
        destination_table=[table_name_0, table_name_1],
    )

    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_snowflake_list_none",
        "mock_table.parquet",
    )
    fnrun = tdfnrun(fn, input_tables=mock_parquet_table)
    try:
        assert fnrun.status == 0

        cursor = snowflake_connection.cursor()
        try:
            cursor.execute(f"SELECT * FROM {table_name_0}")
            result = cursor.fetchall()
            assert len(result) == 0
        except ProgrammingError as e:
            if "002003" in str(e) and "42S02" in str(e):
                pass
            else:
                raise

        cursor = snowflake_connection.cursor()
        try:
            cursor.execute(f"SELECT * FROM {table_name_1}")
            result = cursor.fetchall()
            assert len(result) == 0
        except ProgrammingError as e:
            if "002003" in str(e) and "42S02" in str(e):
                pass
            else:
                raise
    finally:
        # noinspection PyBroadException
        try:
            cursor = snowflake_connection.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {table_name_0}")
            cursor.close()
        except Exception:
            pass
        # noinspection PyBroadException
        try:
            cursor = snowflake_connection.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {table_name_1}")
            cursor.close()
        except Exception:
            pass
