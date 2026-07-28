#
# Copyright 2026 Tabsdata Inc.
#

"""Driver-free unit tests for ``StarRocksConnectionExplorer``.

The missing-table recognition is asserted against the error both MySQL
protocol drivers raise -- pymysql (what the explorer connects with) and
mysql-connector (what the write path uses) spell the message differently
but both lead with StarRocks' analyzer code.
"""

from __future__ import annotations

import pytest
from sqlalchemy.exc import DatabaseError, DBAPIError, ProgrammingError

# noinspection PyProtectedMember
from tabsdatak.conn.starrocks._explorer import StarRocksConnectionExplorer

pytestmark = pytest.mark.starrocks

STATEMENT = "SHOW CREATE TABLE `analytics`.`gone`"

PYMYSQL_GONE = (
    1064,
    "Getting analyzing error. Detail message: Table gone is not found.",
)
CONNECTOR_GONE = (
    1064,
    "1064 (HY000): Getting analyzing error. Detail message: Table gone is not found.",
    "HY000",
)


def _wrapped(error: type[DBAPIError], args: tuple) -> DBAPIError:
    return error(STATEMENT, {}, Exception(*args))


@pytest.mark.parametrize(
    "error, args",
    [
        (ProgrammingError, PYMYSQL_GONE),
        (DatabaseError, CONNECTOR_GONE),
    ],
    ids=["pymysql", "mysqlconnector"],
)
def test_missing_table_error_recognises_a_dropped_table(error, args):
    explorer = StarRocksConnectionExplorer()
    assert explorer.is_missing_table_error(_wrapped(error, args)) is True


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
