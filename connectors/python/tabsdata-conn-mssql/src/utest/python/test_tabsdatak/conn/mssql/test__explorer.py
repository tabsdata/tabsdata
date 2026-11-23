#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.mssql import MSSQLDestConn, MSSQLSrcConn

# noinspection PyProtectedMember
from tabsdatak.conn.mssql._explorer import MSSQLServerExplorer
from test_tabsdatak.conn.common.sql._testing import driver_keywords

pytestmark = pytest.mark.mssql

CONN_CFG = {"timeout": 5, "autocommit": True}
ODBC = (
    "DRIVER={ODBC Driver 18 for SQL Server};SERVER=mssql.example.com,1433;"
    "DATABASE=analytics"
)


def _conn(uri: str, conn_cfg: dict, conn_type=MSSQLSrcConn):
    return conn_type(
        uri=uri,
        credentials=UserPassword(user=Secret("etl"), password=Secret("hunter2")),
        conn_cfg=conn_cfg,
    )


@pytest.mark.parametrize(
    "conn_type", [MSSQLSrcConn, MSSQLDestConn], ids=["source", "destination"]
)
def test_the_explorer_hands_conn_cfg_to_pyodbc(conn_type):
    conn = _conn("mssql://mssql.example.com:1433/analytics", CONN_CFG, conn_type)
    assert driver_keywords(MSSQLServerExplorer(), conn).items() >= CONN_CFG.items()


def test_an_odbc_string_adds_the_credentials_to_conn_cfg():
    kwargs = MSSQLServerExplorer().engine_kwargs(_conn(ODBC, CONN_CFG))
    assert kwargs == {"connect_args": CONN_CFG | {"UID": "etl", "PWD": "hunter2"}}


def test_an_odbc_string_keeps_credentials_set_in_conn_cfg():
    conn = _conn(ODBC, {"UID": "other", "PWD": "secret"})
    kwargs = MSSQLServerExplorer().engine_kwargs(conn)
    assert kwargs == {"connect_args": {"UID": "other", "PWD": "secret"}}
