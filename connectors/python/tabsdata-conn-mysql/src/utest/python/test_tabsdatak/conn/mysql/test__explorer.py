#
# Copyright 2026 Tabsdata Inc.
#

"""``MySQLConnectionExplorer`` builds its engine URL through the same
scheme/driver normalization as the read/write path, so check-connection
and discovery exercise the DBAPI a run will actually use."""

from __future__ import annotations

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.mysql import MySQLSrcConn

# noinspection PyProtectedMember
from tabsdatak.conn.mysql._explorer import MySQLConnectionExplorer

pytestmark = pytest.mark.mysql


def _conn(uri: str, **kwargs) -> MySQLSrcConn:
    return MySQLSrcConn(
        uri=uri,
        credentials=UserPassword(user=Secret("etl"), password=Secret("hunter2")),
        **kwargs,
    )


def test_make_url_injects_mysqlconnector_driver():
    url = MySQLConnectionExplorer().make_url(
        _conn("mysql://mysql.example.com:3306/analytics?charset=utf8mb4")
    )
    assert url.drivername == "mysql+mysqlconnector"
    assert url.username == "etl"
    assert url.password == "hunter2"
    assert url.host == "mysql.example.com"
    assert url.port == 3306
    assert url.database == "analytics"
    assert dict(url.query) == {"charset": "utf8mb4"}


def test_make_url_folds_mariadb_scheme():
    url = MySQLConnectionExplorer().make_url(_conn("mariadb://host/db"))
    assert url.drivername == "mysql+mysqlconnector"


def test_make_url_preserves_explicit_driver():
    url = MySQLConnectionExplorer().make_url(_conn("mysql+pymysql://host/db"))
    assert url.drivername == "mysql+pymysql"


def test_engine_kwargs_wrap_conn_cfg_as_connect_args():
    conn = _conn("mysql://host/db", conn_cfg={"connect_timeout": 5})
    assert MySQLConnectionExplorer().engine_kwargs(conn) == {
        "connect_args": {"connect_timeout": 5}
    }


def test_engine_kwargs_without_conn_cfg_pass_empty_connect_args():
    assert MySQLConnectionExplorer().engine_kwargs(_conn("mysql://host/db")) == {
        "connect_args": {}
    }
