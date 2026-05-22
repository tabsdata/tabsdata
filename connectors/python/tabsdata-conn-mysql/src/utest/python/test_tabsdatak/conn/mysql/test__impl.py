from __future__ import annotations

import pytest
from sqlalchemy.engine import URL

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword
# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine import constants as const
# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine.reader import SqlReadCfg
# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine.writer import SqlWriteCfg
from tabsdatak.conn.mysql import MySQLDestConn, MySQLSrcConn
# noinspection PyProtectedMember
from tabsdatak.conn.mysql import _impl as impl


pytestmark = pytest.mark.mysql


# ---------------------------------------------------------------------------
# build_mysql_engine -- driver injection + credential set
#
# The mysql DBAPI may not be installed in the test env, so we intercept
# ``create_engine`` and verify the URL that the impl built.
# ---------------------------------------------------------------------------


class _CapturedURL:
    """Stand-in for a sqlalchemy Engine with the URL we captured."""

    def __init__(self, url, connect_args):
        self.url = url
        self.connect_args = connect_args

    def dispose(self) -> None:
        pass


def _credentials() -> UserPassword:
    return UserPassword(
        user=Secret.lit("etl"),
        password=Secret.lit("hunter2"),
    )


def _src_conn() -> MySQLSrcConn:
    return MySQLSrcConn(
        uri="mysql://mysql.example.com:3306/analytics",
        credentials=_credentials(),
        conn_cfg={"connect_timeout": 5},
    )


def _dest_conn() -> MySQLDestConn:
    return MySQLDestConn(
        uri="mysql://mysql.example.com:3306/analytics",
        credentials=_credentials(),
        conn_cfg={"connect_timeout": 5},
    )


@pytest.fixture
def captured(monkeypatch):
    """Monkey-patch ``impl.create_engine`` to capture call args."""
    box: dict = {}

    def fake_create_engine(url, *, connect_args):
        box["url"] = url
        box["connect_args"] = connect_args
        return _CapturedURL(url, connect_args)

    monkeypatch.setattr(impl, "create_engine", fake_create_engine)
    return box


def test_build_mysql_engine_injects_mysqlconnector_driver_when_missing(captured):
    impl.build_mysql_engine(
        "mysql://host:3306/db",
        _credentials(),
        connect_args=None,
    )
    url: URL = captured["url"]
    assert url.drivername == "mysql+mysqlconnector"
    assert url.username == "etl"
    assert url.password == "hunter2"
    assert url.host == "host"
    assert url.port == 3306


def test_build_mysql_engine_preserves_explicit_driver(captured):
    impl.build_mysql_engine(
        "mysql+pymysql://host:3306/db",
        _credentials(),
        connect_args=None,
    )
    assert captured["url"].drivername == "mysql+pymysql"


def test_build_mysql_engine_accepts_mariadb_scheme(captured):
    # No mariadb default driver -> stays as-is.
    impl.build_mysql_engine(
        "mariadb://host/db",
        _credentials(),
        connect_args=None,
    )
    assert captured["url"].drivername == "mariadb"


def test_build_mysql_engine_accepts_secret_uri(captured):
    impl.build_mysql_engine(
        Secret.lit("mysql://host:3306/db"),
        _credentials(),
        connect_args=None,
    )
    assert captured["url"].drivername == "mysql+mysqlconnector"


def test_build_mysql_engine_passes_connect_args(captured):
    impl.build_mysql_engine(
        "mysql://host/db",
        _credentials(),
        connect_args={"charset": "utf8mb4"},
    )
    assert captured["connect_args"] == {"charset": "utf8mb4"}


def test_build_mysql_engine_with_no_connect_args_passes_empty_dict(captured):
    impl.build_mysql_engine(
        "mysql://host/db",
        _credentials(),
        connect_args=None,
    )
    assert captured["connect_args"] == {}


# ---------------------------------------------------------------------------
# Adapter dataclasses
# ---------------------------------------------------------------------------


def test_mysql_read_cfg_subclasses_sql_read_cfg():
    assert issubclass(impl.MySQLReadCfg, SqlReadCfg)


def test_mysql_read_cfg_creates_engine_from_conn(captured):
    cfg = impl.MySQLReadCfg(
        conn=_src_conn(),
        queries=["SELECT 1"],
        transactional=True,
        schema_overrides=None,
        query_params=None,
        cfg=None,
    )
    cfg.create_sql_alchemy_engine()
    assert captured["url"].drivername == "mysql+mysqlconnector"
    # The conn_cfg dict should be the connect_args forwarded:
    assert captured["connect_args"] == {"connect_timeout": 5}


def test_mysql_write_cfg_subclasses_sql_write_cfg():
    assert issubclass(impl.MySQLWriteCfg, SqlWriteCfg)


def test_mysql_write_cfg_creates_engine_from_conn(captured):
    cfg = impl.MySQLWriteCfg(
        conn=_dest_conn(),
        target_tables=["t"],
        if_table_exists="append",
        transactional=True,
        cfg=None,
    )
    cfg.create_sql_alchemy_engine()
    assert captured["url"].drivername == "mysql+mysqlconnector"
    assert captured["connect_args"] == {"connect_timeout": 5}


def test_mysql_read_cfg_no_conn_cfg_forwards_empty_connect_args(captured):
    conn = MySQLSrcConn(
        uri="mysql://host/db",
        credentials=_credentials(),
    )
    cfg = impl.MySQLReadCfg(
        conn=conn,
        queries=["SELECT 1"],
        transactional=True,
        schema_overrides=None,
        query_params=None,
        cfg=None,
    )
    cfg.create_sql_alchemy_engine()
    assert captured["connect_args"] == {}


# ---------------------------------------------------------------------------
# Connector defaults
# ---------------------------------------------------------------------------


def test_read_defaults_pick_polars_with_pandas_allowed():
    d = impl.MYSQL_READ_DEFAULTS
    assert d[const.READ_ENGINE_KEY] == const.POLARS_SQL_ALCHEMY_READ_ENGINE
    assert const.POLARS_SQL_ALCHEMY_READ_ENGINE in d[const.AVAIL_READ_ENGINES_KEY]
    assert const.PANDAS_SQL_ALCHEMY_READ_ENGINE in d[const.AVAIL_READ_ENGINES_KEY]


def test_write_defaults_pick_polars_only():
    d = impl.MYSQL_WRITE_DEFAULTS
    assert d[const.WRITE_ENGINE_KEY] == const.POLARS_SQL_ALCHEMY_WRITE_ENGINE
    assert d[const.AVAIL_WRITE_ENGINES_KEY] == [
        const.POLARS_SQL_ALCHEMY_WRITE_ENGINE,
    ]
