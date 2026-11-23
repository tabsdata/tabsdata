#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import AwsAccessSecretKey, UserPassword
from tabsdatak.conn.druid import DruidDestConn, S3Location

# noinspection PyProtectedMember
from tabsdatak.conn.druid._explorer import DruidConnectionExplorer
from test_tabsdatak.conn.common.sql._testing import driver_keywords

pytestmark = pytest.mark.druid

CONN_CFG = {"ssl_verify_cert": False}


def test_the_explorer_hands_conn_cfg_to_pydruid():
    conn = DruidDestConn(
        overlord_url="http://druid.example.com:8081",
        staging=S3Location(
            bucket="staging-bucket",
            region="us-east-1",
            credentials=AwsAccessSecretKey(
                access_key_id=Secret("AKIA..."),
                secret_access_key=Secret("secret"),
            ),
            base_path="/staging",
        ),
        credentials=UserPassword(user=Secret("etl"), password=Secret("hunter2")),
        conn_cfg=CONN_CFG,
    )
    keywords = driver_keywords(DruidConnectionExplorer(), conn)
    assert keywords.items() >= CONN_CFG.items()
