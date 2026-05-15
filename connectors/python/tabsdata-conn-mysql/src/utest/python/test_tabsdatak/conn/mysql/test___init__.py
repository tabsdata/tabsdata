from __future__ import annotations

import pytest

from tabsdatak.api import NamedConn, Secret
from tabsdatak.error import TabsdataException
from tabsdatak.conn.common.error import ConnInitException
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.mysql import MySQLConn, MySQLDest, MySQLSrc
# noinspection PyProtectedMember
from tabsdatak.conn.mysql.error import (
    MySQLValidateException,
    MysqlErrorCode,
)


pytestmark = pytest.mark.mysql


def _conn() -> MySQLConn:
    return MySQLConn(
        uri="mysql://mysql.example.com:3306/analytics",
        credentials=UserPassword(
            user=Secret.lit("etl"),
            password=Secret.lit("hunter2"),
        ),
        conn_cfg={"mysql.timeout": 30},
    )


# ---------------------------------------------------------------------------
# MySQLConn
# ---------------------------------------------------------------------------


def test_mysql_conn_round_trip():
    c = _conn()
    loaded = MySQLConn.from_json(c.to_json())
    assert loaded == c
    assert loaded.uri == "mysql://mysql.example.com:3306/analytics"
    assert loaded.conn_cfg == {"mysql.timeout": 30}


def test_mysql_conn_rejects_bad_uri():
    # The ``uri`` field is a Union of ``str | Secret | Env``, each
    # carrying the same regex. A bad value fails *every* branch of the
    # Union -- pydantic surfaces whichever raised; assert on the
    # ``TabsdataException`` umbrella rather than the specific code.
    with pytest.raises(TabsdataException):
        MySQLConn(
            uri="ftp://host/db",
            credentials=UserPassword(
                user=Secret.lit("u"),
                password=Secret.lit("p"),
            ),
        )


# ---------------------------------------------------------------------------
# MySQLSrc
# ---------------------------------------------------------------------------


def test_mysql_src_full_round_trip():
    src = MySQLSrc(
        conn=_conn(),
        queries=["SELECT * FROM users", "SELECT * FROM orders"],
        transactional=False,
        schema_overrides=[None, None],
        query_params={"x": 1},
        src_cfg={"chunkSize": 1024},
    )
    loaded = MySQLSrc.from_json(src.to_json())
    assert loaded == src
    assert loaded.queries == ["SELECT * FROM users", "SELECT * FROM orders"]
    assert loaded.transactional is False
    assert loaded.schema_overrides == [None, None]
    assert loaded.query_params == {"x": 1}
    assert loaded.src_cfg == {"chunkSize": 1024}


def test_mysql_src_round_trip_with_named_conn():
    src = MySQLSrc(conn=NamedConn(name="prod_mysql"), queries=["SELECT 1"])
    loaded = MySQLSrc.from_json(src.to_json())
    assert loaded == src
    assert isinstance(loaded.conn, NamedConn)
    assert loaded.conn.name == "prod_mysql"


def test_mysql_src_validate_rejects_schema_overrides_length_mismatch():
    with pytest.raises(MySQLValidateException) as ei:
        MySQLSrc(
            conn=_conn(),
            queries=["SELECT 1", "SELECT 2"],
            schema_overrides=[None],
        )
    assert ei.value.api_error is MysqlErrorCode.MYSQL_3


def test_mysql_src_validate_accepts_matching_schema_overrides_length():
    src = MySQLSrc(
        conn=_conn(),
        queries=["SELECT 1", "SELECT 2"],
        schema_overrides=[None, None],
    )
    assert len(src.schema_overrides) == len(src.queries)


# ---------------------------------------------------------------------------
# MySQLDest
# ---------------------------------------------------------------------------


def test_mysql_dest_full_round_trip():
    dest = MySQLDest(
        conn=_conn(),
        tables=["analytics.users", "analytics.orders"],
        if_table_exists="replace",
        transactional=False,
        dest_cfg={"chunkSize": 2048},
    )
    loaded = MySQLDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.tables == ["analytics.users", "analytics.orders"]
    assert loaded.if_table_exists == "replace"
    assert loaded.transactional is False
    assert loaded.dest_cfg == {"chunkSize": 2048}


def test_mysql_dest_round_trip_with_named_conn():
    dest = MySQLDest(conn=NamedConn(name="prod_mysql"), tables=["analytics.users"])
    loaded = MySQLDest.from_json(dest.to_json())
    assert loaded == dest
    assert isinstance(loaded.conn, NamedConn)
    assert loaded.conn.name == "prod_mysql"


def test_mysql_dest_rejects_bad_table_name():
    with pytest.raises(ConnInitException) as ei:
        MySQLDest(
            conn=_conn(),
            tables=["bad-name"],
        )
    assert ei.value.api_error in {MysqlErrorCode.MYSQL_4, MysqlErrorCode.MYSQL_15}


def test_mysql_dest_defaults_if_table_exists_to_append():
    dest = MySQLDest(conn=_conn(), tables=["analytics.users"])
    assert dest.if_table_exists == "append"
    assert dest.transactional is True
    assert dest.dest_cfg is None
