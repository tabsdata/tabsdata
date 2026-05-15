from __future__ import annotations

import pytest

from tabsdatak.api import NamedConn, Secret
from tabsdatak.error import TabsdataException
from tabsdatak.conn.common.error import ConnInitException
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.mssql import MSSQLConn, MSSQLDest, MSSQLSrc
# noinspection PyProtectedMember
from tabsdatak.conn.mssql.error import (
    MSSQLValidateException,
    MssqlErrorCode,
)


pytestmark = pytest.mark.mssql


def _conn() -> MSSQLConn:
    return MSSQLConn(
        uri="mssql://mssql.example.com:1433/analytics",
        credentials=UserPassword(
            user=Secret.lit("etl"),
            password=Secret.lit("hunter2"),
        ),
        conn_cfg={"mssql.timeout": 30},
    )


# ---------------------------------------------------------------------------
# MSSQLConn
# ---------------------------------------------------------------------------


def test_mssql_conn_round_trip():
    c = _conn()
    loaded = MSSQLConn.from_json(c.to_json())
    assert loaded == c
    assert loaded.uri == "mssql://mssql.example.com:1433/analytics"
    assert loaded.conn_cfg == {"mssql.timeout": 30}


def test_mssql_conn_accepts_odbc_connection_string_and_round_trips():
    # The ``uri`` field accepts an ODBC ``KEY=value;…`` string as the
    # alternative to ``mssql://`` URLs (handled by ``build_mssql_engine``
    # via ``odbc_connect=<encoded>``). Verify it round-trips through
    # JSON unchanged on a Conn, a Src, and a Dest.
    odbc_uri = (
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=db.example.com,1433;"
        "Database=analytics;"
        "TrustServerCertificate=yes"
    )
    conn = MSSQLConn(
        uri=odbc_uri,
        credentials=UserPassword(
            user=Secret.lit("etl"),
            password=Secret.lit("hunter2"),
        ),
    )
    loaded = MSSQLConn.from_json(conn.to_json())
    assert loaded == conn
    assert loaded.uri == odbc_uri

    src = MSSQLSrc(conn=conn, queries=["SELECT 1"])
    assert MSSQLSrc.from_json(src.to_json()) == src

    dest = MSSQLDest(conn=conn, tables=["dbo.users"])
    assert MSSQLDest.from_json(dest.to_json()) == dest


def test_mssql_conn_rejects_bad_uri():
    # The ``uri`` field is a Union of ``str | Secret | Env``, each
    # carrying the same regex. A bad value fails *every* branch of the
    # Union -- pydantic surfaces whichever raised; assert on the
    # ``TabsdataException`` umbrella rather than the specific code.
    with pytest.raises(TabsdataException):
        MSSQLConn(
            uri="ftp://host/db",
            credentials=UserPassword(
                user=Secret.lit("u"),
                password=Secret.lit("p"),
            ),
        )


# ---------------------------------------------------------------------------
# MSSQLSrc
# ---------------------------------------------------------------------------


def test_mssql_src_full_round_trip():
    src = MSSQLSrc(
        conn=_conn(),
        queries=["SELECT * FROM users", "SELECT * FROM orders"],
        transactional=False,
        schema_overrides=[None, None],
        query_params={"x": 1},
        src_cfg={"chunkSize": 1024},
    )
    loaded = MSSQLSrc.from_json(src.to_json())
    assert loaded == src
    assert loaded.queries == ["SELECT * FROM users", "SELECT * FROM orders"]
    assert loaded.transactional is False
    assert loaded.schema_overrides == [None, None]
    assert loaded.query_params == {"x": 1}
    assert loaded.src_cfg == {"chunkSize": 1024}


def test_mssql_src_round_trip_with_named_conn():
    src = MSSQLSrc(conn=NamedConn(name="prod_mssql"), queries=["SELECT 1"])
    loaded = MSSQLSrc.from_json(src.to_json())
    assert loaded == src
    assert isinstance(loaded.conn, NamedConn)
    assert loaded.conn.name == "prod_mssql"


def test_mssql_src_validate_rejects_schema_overrides_length_mismatch():
    with pytest.raises(MSSQLValidateException) as ei:
        MSSQLSrc(
            conn=_conn(),
            queries=["SELECT 1", "SELECT 2"],
            schema_overrides=[None],
        )
    assert ei.value.api_error is MssqlErrorCode.MSSQL_3


def test_mssql_src_validate_accepts_matching_schema_overrides_length():
    src = MSSQLSrc(
        conn=_conn(),
        queries=["SELECT 1", "SELECT 2"],
        schema_overrides=[None, None],
    )
    assert len(src.schema_overrides) == len(src.queries)


# ---------------------------------------------------------------------------
# MSSQLDest
# ---------------------------------------------------------------------------


def test_mssql_dest_full_round_trip():
    dest = MSSQLDest(
        conn=_conn(),
        tables=["dbo.users", "dbo.orders"],
        if_table_exists="replace",
        transactional=False,
        dest_cfg={"chunkSize": 2048},
    )
    loaded = MSSQLDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.tables == ["dbo.users", "dbo.orders"]
    assert loaded.if_table_exists == "replace"
    assert loaded.transactional is False
    assert loaded.dest_cfg == {"chunkSize": 2048}


def test_mssql_dest_round_trip_with_named_conn():
    dest = MSSQLDest(conn=NamedConn(name="prod_mssql"), tables=["dbo.users"])
    loaded = MSSQLDest.from_json(dest.to_json())
    assert loaded == dest
    assert isinstance(loaded.conn, NamedConn)
    assert loaded.conn.name == "prod_mssql"


def test_mssql_dest_rejects_bad_table_name():
    with pytest.raises(ConnInitException) as ei:
        MSSQLDest(
            conn=_conn(),
            tables=["bad-name"],
        )
    assert ei.value.api_error in {MssqlErrorCode.MSSQL_4, MssqlErrorCode.MSSQL_16}


def test_mssql_dest_defaults_if_table_exists_to_append():
    dest = MSSQLDest(conn=_conn(), tables=["dbo.users"])
    assert dest.if_table_exists == "append"
    assert dest.transactional is True
    assert dest.dest_cfg is None
