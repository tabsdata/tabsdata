#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.postgres import (
    PostgresCdcStreamSrcConn,
    PostgresDestConn,
    PostgresSrcConn,
)

# noinspection PyProtectedMember
from tabsdatak.conn.postgres._explorer import (
    PostgresCdcConnectionExplorer,
    PostgresConnectionExplorer,
)
from test_tabsdatak.conn.common.sql._testing import driver_keywords

pytestmark = pytest.mark.postgres

CONN_CFG = {
    "application_name": "tabsdata-etl",
    "sslmode": "require",
    "connect_timeout": 5,
}


@pytest.mark.parametrize(
    ("explorer", "conn_type"),
    [
        (PostgresConnectionExplorer(), PostgresSrcConn),
        (PostgresConnectionExplorer(), PostgresDestConn),
        (PostgresCdcConnectionExplorer(), PostgresCdcStreamSrcConn),
    ],
    ids=["source", "destination", "cdc-stream"],
)
def test_the_explorer_hands_conn_cfg_to_psycopg2(explorer, conn_type):
    conn = conn_type(
        uri="postgresql://postgres.example.com:5432/analytics",
        credentials=UserPassword(user=Secret("etl"), password=Secret("hunter2")),
        conn_cfg=CONN_CFG,
    )
    assert driver_keywords(explorer, conn).items() >= CONN_CFG.items()
