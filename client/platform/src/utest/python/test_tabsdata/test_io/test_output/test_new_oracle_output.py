#
# Copyright 2026 Tabs Data Inc.
#

import pytest

from tabsdata._io.connections.oracle_connection import OracleConn
from tabsdata._io.outputs.sql.oracle_output import OracleDest
from tabsdata.exceptions import (
    DestinationConfigurationError,
    ErrorCode,
    ParameterValidationError,
)


@pytest.fixture
def oracle_conn():
    return OracleConn(
        uri="oracle://user:pass@localhost:1521/db",
        credentials=None,
    )


def test_oracle_dest_properties(oracle_conn):
    dest = OracleDest(
        conn=oracle_conn,
        destination_tables="my_table",
        if_table_exists="append",
        transactional=True,
        chunk_size=50000,
        loader="polars_sqlalchemy",
    )
    assert dest.conn == oracle_conn
    assert dest.destination_tables == ["my_table"]
    assert dest.if_table_exists == "append"
    assert dest.transactional is True
    assert dest.chunk_size == 50000
    assert dest.loader == "polars_sqlalchemy"


def test_oracle_dest_multiple_tables(oracle_conn):
    tables = ["table1", "table2"]
    dest = OracleDest(
        conn=oracle_conn,
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


def test_oracle_dest_repr(oracle_conn):
    dest = OracleDest(
        conn=oracle_conn,
        destination_tables="my_table",
        if_table_exists="append",
        transactional=True,
        chunk_size=100000,
        loader="polars_sqlalchemy",
    )
    r = repr(dest)
    assert "OracleDest" in r
    assert "my_table" in r
    assert "polars_sqlalchemy" in r


def test_oracle_dest_invalid_table_name(oracle_conn):
    with pytest.raises(DestinationConfigurationError) as exc:
        OracleDest(
            conn=oracle_conn,
            destination_tables="   ",
            if_table_exists="append",
            transactional=True,
            chunk_size=100000,
            loader="polars_sqlalchemy",
        )
    assert exc.value.error_code == ErrorCode.DECE54


def test_oracle_dest_invalid_chunk_size(oracle_conn):
    with pytest.raises(ParameterValidationError):
        OracleDest(
            conn=oracle_conn,
            destination_tables="my_table",
            if_table_exists="append",
            transactional=True,
            chunk_size=-1,  # Invalid: not PositiveInt
            loader="polars_sqlalchemy",
        )


def test_oracle_dest_invalid_loader(oracle_conn):
    with pytest.raises(ParameterValidationError):
        OracleDest(
            conn=oracle_conn,
            destination_tables="my_table",
            if_table_exists="append",
            transactional=True,
            chunk_size=100000,
            loader="not_a_valid_loader",
        )


def test_oracle_dest_invalid_if_table_exists(oracle_conn):
    with pytest.raises(ParameterValidationError):
        OracleDest(
            conn=oracle_conn,
            destination_tables="my_table",
            if_table_exists="not_a_valid_strategy",  # Invalid value
            transactional=True,
            chunk_size=100000,
            loader="polars_sqlalchemy",
        )


def test_oracle_dest_invalid_transactional(oracle_conn):
    with pytest.raises(ParameterValidationError):
        OracleDest(
            conn=oracle_conn,
            destination_tables="my_table",
            if_table_exists="append",
            transactional="not_a_bool",  # Invalid: should be bool
            chunk_size=100000,
            loader="polars_sqlalchemy",
        )
