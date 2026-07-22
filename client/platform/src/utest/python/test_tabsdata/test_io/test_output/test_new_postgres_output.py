#
# Copyright 2026 Tabsdata Inc.
#

import polars as pl
import pytest

from tabsdata._credentials import UserPasswordCredentials
from tabsdata._io.connections.postgres_connection import PostgresConn
from tabsdata._io.outputs.sql.postgres_output import PostgresDest
from tabsdata.exceptions import (
    DestinationConfigurationError,
    ErrorCode,
    ParameterValidationError,
)


def test_postgres_dest_str_table_defaults():
    conn = PostgresConn("postgresql://host:5432/db")
    dest = PostgresDest(conn, "table1")
    assert dest.conn == conn
    assert dest.destination_tables == ["table1"]
    assert dest.if_table_exists == "append"
    assert dest.transactional is True
    assert dest.chunk_size == 100000
    assert dest.loader == "polars_sqlalchemy"


def test_postgres_dest_list_tables_all_params():
    conn = PostgresConn("postgresql://host:5432/db")
    dest = PostgresDest(
        conn,
        ["table1", "table2"],
        if_table_exists="replace",
        transactional=False,
        chunk_size=5000,
        loader="polars_sqlalchemy",
    )
    assert dest.destination_tables == ["table1", "table2"]
    assert dest.if_table_exists == "replace"
    assert dest.transactional is False
    assert dest.chunk_size == 5000
    assert dest.loader == "polars_sqlalchemy"


def test_postgres_dest_if_table_exists_enum():
    conn = PostgresConn("postgresql://host:5432/db")
    dest = PostgresDest(conn, "table1", if_table_exists="replace")
    assert dest.if_table_exists == "replace"


def test_postgres_dest_loader_enum():
    conn = PostgresConn("postgresql://host:5432/db")
    dest = PostgresDest(conn, "table1", loader="polars_sqlalchemy")
    assert dest.loader == "polars_sqlalchemy"


def test_postgres_dest_normalize_destination_tables():
    conn = PostgresConn("postgresql://host:5432/db")
    dest = PostgresDest(conn, "table1")
    assert dest.destination_tables == ["table1"]


def test_postgres_dest_chunk_size_validation():
    conn = PostgresConn("postgresql://host:5432/db")
    # Valid chunk size
    dest = PostgresDest(conn, "table1", chunk_size=1)
    assert dest.chunk_size == 1
    # Invalid chunk size (should raise ParameterValidationError)
    with pytest.raises(ParameterValidationError):
        PostgresDest(conn, "table1", chunk_size=0)
    with pytest.raises(ParameterValidationError):
        PostgresDest(conn, "table1", chunk_size=-10)
    with pytest.raises(ParameterValidationError) as e:
        PostgresDest(conn, "table1", chunk_size="a")
    assert e.value.error_code == ErrorCode.DECE107


def test_postgres_dest_invalid_loader_raises():
    conn = PostgresConn("postgresql://host:5432/db")
    with pytest.raises(ParameterValidationError) as e:
        PostgresDest(conn, "table1", loader="not_a_loader")
    assert e.value.error_code == ErrorCode.DECE107


def test_postgres_dest_invalid_if_table_exists_raises():
    conn = PostgresConn("postgresql://host:5432/db")
    with pytest.raises(ParameterValidationError) as e:
        PostgresDest(conn, "table1", if_table_exists="not_a_strategy")
    assert e.value.error_code == ErrorCode.DECE107


def test_postgres_dest_transactional_param():
    conn = PostgresConn("postgresql://host:5432/db")
    dest = PostgresDest(conn, "table1", transactional=False)
    assert dest.transactional is False
    dest2 = PostgresDest(conn, "table1", transactional=True)
    assert dest2.transactional is True


def test_postgres_dest_invalid_transactional_values():
    conn = PostgresConn("postgresql://host:5432/db")
    with pytest.raises(ParameterValidationError) as e:
        PostgresDest(conn, "table1", transactional="yeah")
    assert e.value.error_code == ErrorCode.DECE107
    with pytest.raises(ParameterValidationError) as e:
        PostgresDest(conn, "table1", transactional=4)
    assert e.value.error_code == ErrorCode.DECE107


def test_postgres_dest_repr_and_properties():
    conn = PostgresConn("postgresql://host:5432/db")
    dest = PostgresDest(conn, ["table1"])
    assert "PostgresDest" in repr(dest)
    assert dest.conn == conn
    assert dest.destination_tables == ["table1"]
    assert dest.if_table_exists == "append"
    assert dest.transactional is True
    assert dest.chunk_size == 100000
    assert dest.loader == "polars_sqlalchemy"


def test_postgres_dest_multiple_tables_and_results_length():
    conn = PostgresConn("postgresql://host:5432/db")
    dest = PostgresDest(conn, ["table1", "table2"])
    assert dest.destination_tables == ["table1", "table2"]


def test_postgres_dest_with_different_conn():
    credentials = UserPasswordCredentials("user", "pass")
    conn = PostgresConn("postgresql://host:5432/db", credentials=credentials)
    dest = PostgresDest(conn, "table1")
    assert dest.conn.credentials == credentials


def test_postgres_dest_with_various_table_types():
    conn = PostgresConn("postgresql://host:5432/db")
    # Single string
    dest1 = PostgresDest(conn, "table1")
    assert dest1.destination_tables == ["table1"]
    # List of strings
    dest2 = PostgresDest(conn, ["table1", "table2"])
    assert dest2.destination_tables == ["table1", "table2"]


def test_postgres_dest_invalid_destination_tables_type():
    conn = PostgresConn("postgresql://host:5432/db")
    with pytest.raises(ParameterValidationError):
        PostgresDest(conn, 12345)
    with pytest.raises(ParameterValidationError):
        PostgresDest(conn, None)
    with pytest.raises(ParameterValidationError) as e:
        PostgresDest(conn, [None, "table2"])
    assert e.value.error_code == ErrorCode.DECE107


def test_postgres_dest_empty_destination_table_raises():
    conn = PostgresConn("postgresql://host:5432/db")
    destination_table = ""
    with pytest.raises(DestinationConfigurationError) as e:
        PostgresDest(conn, destination_table)
    assert e.value.error_code == ErrorCode.DECE54


def test_stream_len_mismatch():
    conn = PostgresConn("postgresql://host:5432/db")
    destination_tables = ["table1", "table2"]
    dest = PostgresDest(conn, destination_tables)

    with pytest.raises(ValueError) as e:
        dest.stream(".", [pl.LazyFrame()])
