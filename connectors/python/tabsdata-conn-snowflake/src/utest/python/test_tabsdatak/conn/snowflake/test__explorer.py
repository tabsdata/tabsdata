#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.snowflake import SnowflakeDestConn

# noinspection PyProtectedMember
from tabsdatak.conn.snowflake._explorer import SnowflakeConnectionExplorer
from test_tabsdatak.conn.common.sql._testing import driver_keywords

pytestmark = pytest.mark.snowflake

CONN_CFG = {"login_timeout": 5, "application": "tabsdata-etl"}


def test_the_explorer_hands_conn_cfg_to_the_snowflake_connector():
    conn = SnowflakeDestConn(
        account="ACME-MAIN",
        credentials=UserPassword(user=Secret("etl"), password=Secret("hunter2")),
        database="ANALYTICS",
        conn_cfg=CONN_CFG,
    )
    keywords = driver_keywords(SnowflakeConnectionExplorer(), conn)
    assert keywords.items() >= CONN_CFG.items()
