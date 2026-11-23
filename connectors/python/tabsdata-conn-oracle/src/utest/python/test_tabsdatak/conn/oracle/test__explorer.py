#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.oracle import OracleDestConn, OracleSrcConn

# noinspection PyProtectedMember
from tabsdatak.conn.oracle._explorer import OracleConnectionExplorer
from test_tabsdatak.conn.common.sql._testing import driver_keywords

pytestmark = pytest.mark.oracle

CONN_CFG = {"tcp_connect_timeout": 5, "expire_time": 2}


def _conn(conn_type=OracleSrcConn):
    return conn_type(
        uri="oracle://oracle.example.com:1521/orcl",
        credentials=UserPassword(user=Secret("etl"), password=Secret("hunter2")),
        conn_cfg=CONN_CFG,
    )


@pytest.mark.parametrize(
    "conn_type", [OracleSrcConn, OracleDestConn], ids=["source", "destination"]
)
def test_the_explorer_hands_conn_cfg_to_oracledb(conn_type):
    keywords = driver_keywords(OracleConnectionExplorer(), _conn(conn_type))
    assert keywords.items() >= CONN_CFG.items()


def test_the_explorer_still_reflects_every_tablespace():
    kwargs = OracleConnectionExplorer().engine_kwargs(_conn())
    assert kwargs == {"connect_args": CONN_CFG, "exclude_tablespaces": ()}
