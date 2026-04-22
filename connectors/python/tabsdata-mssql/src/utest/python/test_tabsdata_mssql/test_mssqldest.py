#
# Copyright 2026 Tabs Data Inc.
#

import pytest
from pydantic import ValidationError

from tabsdata.connector.mssql.connection.mssql_connection import MSSQLConn
from tabsdata.connector.mssql.output.mssql_output import MSSQLDest
from tabsdata.exceptions import (
    DestinationConfigurationError,
    ErrorCode,
    ParameterValidationError,
)


@pytest.fixture
def mssql_conn():
    return MSSQLConn(
        connection_string_or_uri="mssql://user:pass@localhost:1433/db",
        credentials=None,
    )


@pytest.mark.mssql
def test_mssql_dest_properties(mssql_conn):
    dest = MSSQLDest(
        conn=mssql_conn,
        destination_tables="my_table",
        if_table_exists="append",
        transactional=True,
        chunk_size=50000,
        loader="polars_sqlalchemy",
    )
    assert dest.conn == mssql_conn
    assert dest.destination_tables == ["my_table"]
    assert dest.if_table_exists == "append"
    assert dest.transactional is True
    assert dest.chunk_size == 50000
    assert dest.loader == "polars_sqlalchemy"


@pytest.mark.mssql
def test_mssql_dest_multiple_tables(mssql_conn):
    tables = ["table1", "table2"]
    dest = MSSQLDest(
        conn=mssql_conn,
        destination_tables=tables,
        if_table_exists="replace",
        transactional=False,
        chunk_size=1000,
        loader="polars_sqlalchemy",
    )
    assert dest.destination_tables == tables
    assert dest.if_table_exists == "replace"
    assert dest.transactional is False
    assert dest.chunk_size == 1000


@pytest.mark.mssql
def test_mssql_dest_repr(mssql_conn):
    dest = MSSQLDest(
        conn=mssql_conn,
        destination_tables="my_table",
        if_table_exists="append",
        transactional=True,
        chunk_size=100000,
        loader="polars_sqlalchemy",
    )
    r = repr(dest)
    assert "MSSQLDest" in r
    assert "my_table" in r
    assert "polars_sqlalchemy" in r


@pytest.mark.mssql
def test_mssql_dest_invalid_table_name(mssql_conn):
    with pytest.raises(DestinationConfigurationError) as exc:
        MSSQLDest(
            conn=mssql_conn,
            destination_tables="   ",
            if_table_exists="append",
            transactional=True,
            chunk_size=100000,
            loader="polars_sqlalchemy",
        )
    assert exc.value.error_code == ErrorCode.DECE54


@pytest.mark.mssql
def test_mssql_dest_invalid_chunk_size(mssql_conn):
    with pytest.raises(ParameterValidationError):
        MSSQLDest(
            conn=mssql_conn,
            destination_tables="my_table",
            if_table_exists="append",
            transactional=True,
            chunk_size=-1,  # Invalid: not PositiveInt
            loader="polars_sqlalchemy",
        )


@pytest.mark.mssql
def test_mssql_dest_invalid_loader(mssql_conn):
    with pytest.raises(ParameterValidationError):
        MSSQLDest(
            conn=mssql_conn,
            destination_tables="my_table",
            if_table_exists="append",
            transactional=True,
            chunk_size=100000,
            loader="not_a_valid_loader",
        )


@pytest.mark.mssql
def test_mssql_dest_invalid_if_table_exists(mssql_conn):
    with pytest.raises(ParameterValidationError):
        MSSQLDest(
            conn=mssql_conn,
            destination_tables="my_table",
            if_table_exists="not_a_valid_strategy",  # Invalid value
            transactional=True,
            chunk_size=100000,
            loader="polars_sqlalchemy",
        )


@pytest.mark.mssql
def test_mssql_dest_invalid_transactional(mssql_conn):
    with pytest.raises(ParameterValidationError):
        MSSQLDest(
            conn=mssql_conn,
            destination_tables="my_table",
            if_table_exists="append",
            transactional="not_a_bool",  # Invalid: should be bool
            chunk_size=100000,
            loader="polars_sqlalchemy",
        )
