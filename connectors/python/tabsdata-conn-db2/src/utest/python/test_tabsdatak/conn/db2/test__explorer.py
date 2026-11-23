#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.db2 import Db2CdcStreamSrcConn, Db2DestConn, Db2SrcConn

# noinspection PyProtectedMember
from tabsdatak.conn.db2._explorer import Db2CdcConnectionExplorer, Db2ConnectionExplorer
from test_tabsdatak.conn.common.sql._testing import driver_keywords

pytestmark = pytest.mark.db2

CONN_CFG = {"SECURITY": "SSL", "CONNECTTIMEOUT": 5}


@pytest.mark.parametrize(
    ("explorer", "conn_type"),
    [
        (Db2ConnectionExplorer(), Db2SrcConn),
        (Db2ConnectionExplorer(), Db2DestConn),
        (Db2CdcConnectionExplorer(), Db2CdcStreamSrcConn),
    ],
    ids=["source", "destination", "cdc-stream"],
)
def test_the_explorer_hands_conn_cfg_to_ibm_db(explorer, conn_type):
    conn = conn_type(
        uri="db2://db2.example.com:50000/SAMPLE",
        credentials=UserPassword(user=Secret("etl"), password=Secret("hunter2")),
        conn_cfg=CONN_CFG,
    )
    assert driver_keywords(explorer, conn).items() >= CONN_CFG.items()
