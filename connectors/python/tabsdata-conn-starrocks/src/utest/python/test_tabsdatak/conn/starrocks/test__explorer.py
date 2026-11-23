#
# Copyright 2026 Tabsdata Inc.
#

"""Driver-free unit tests for ``StarRocksConnectionExplorer``.

The missing-table recognition is asserted against the error pymysql raises,
the driver every StarRocks connection goes through.
"""

from __future__ import annotations

import pytest
from sqlalchemy.exc import DBAPIError, ProgrammingError

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import AwsAccessSecretKey, UserPassword
from tabsdatak.conn.starrocks import S3Location, StarRocksDestConn

# noinspection PyProtectedMember
from tabsdatak.conn.starrocks._explorer import StarRocksConnectionExplorer
from test_tabsdatak.conn.common.sql._testing import driver_keywords

pytestmark = pytest.mark.starrocks

STATEMENT = "SHOW CREATE TABLE `analytics`.`gone`"

PYMYSQL_GONE = (
    1064,
    "Getting analyzing error. Detail message: Table gone is not found.",
)


def _wrapped(error: type[DBAPIError], args: tuple) -> DBAPIError:
    return error(STATEMENT, {}, Exception(*args))


def test_missing_table_error_recognises_a_dropped_table():
    explorer = StarRocksConnectionExplorer()
    error = _wrapped(ProgrammingError, PYMYSQL_GONE)
    assert explorer.is_missing_table_error(error) is True


@pytest.mark.parametrize(
    "args",
    [
        (1064, "Getting analyzing error. Detail message: Unexpected input '('."),
        (1045, "Access denied for user 'root'"),
        (),
    ],
    ids=["other_analyzer_error", "other_code", "no_args"],
)
def test_missing_table_error_rejects_every_other_failure(args):
    explorer = StarRocksConnectionExplorer()
    assert explorer.is_missing_table_error(_wrapped(ProgrammingError, args)) is False


def test_the_explorer_hands_conn_cfg_to_pymysql():
    conn_cfg = {"connect_timeout": 5, "charset": "utf8mb4"}
    conn = StarRocksDestConn(
        host="sr.example.com",
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
        conn_cfg=conn_cfg,
    )
    keywords = driver_keywords(StarRocksConnectionExplorer(), conn)
    assert keywords.items() >= conn_cfg.items()
