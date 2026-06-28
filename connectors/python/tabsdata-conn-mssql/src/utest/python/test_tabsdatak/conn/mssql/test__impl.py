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
from tabsdatak.conn.mssql import MSSQLDestConn, MSSQLSrcConn
# noinspection PyProtectedMember
from tabsdatak.conn.mssql import _impl as impl


pytestmark = pytest.mark.mssql


# ---------------------------------------------------------------------------
# build_mssql_engine -- driver injection + credential set
#
# The mssql DBAPI may not be installed in the test env, so we intercept
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
        user=Secret("etl"),
        password=Secret("hunter2"),
    )


def _src_conn() -> MSSQLSrcConn:
    return MSSQLSrcConn(
        uri="mssql://mssql.example.com:1433/analytics",
        credentials=_credentials(),
        conn_cfg={"connect_timeout": 5},
    )


def _dest_conn() -> MSSQLDestConn:
    return MSSQLDestConn(
        uri="mssql://mssql.example.com:1433/analytics",
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


def test_build_mssql_engine_injects_pyodbc_driver_when_missing(captured):
    impl.build_mssql_engine(
        "mssql://host:1433/db",
        _credentials(),
        connect_args=None,
    )
    url: URL = captured["url"]
    assert url.drivername == "mssql+pyodbc"
    assert url.username == "etl"
    assert url.password == "hunter2"
    assert url.host == "host"
    assert url.port == 1433


def test_build_mssql_engine_preserves_explicit_driver(captured):
    impl.build_mssql_engine(
        "mssql+pymssql://host:1433/db",
        _credentials(),
        connect_args=None,
    )
    assert captured["url"].drivername == "mssql+pymssql"


def test_build_mssql_engine_accepts_secret_uri(captured):
    impl.build_mssql_engine(
        Secret("mssql://host:1433/db"),
        _credentials(),
        connect_args=None,
    )
    assert captured["url"].drivername == "mssql+pyodbc"


def test_build_mssql_engine_passes_connect_args(captured):
    impl.build_mssql_engine(
        "mssql://host/db",
        _credentials(),
        connect_args={"timeout": 10},
    )
    assert captured["connect_args"] == {"timeout": 10}


def test_build_mssql_engine_with_no_connect_args_passes_empty_dict(captured):
    impl.build_mssql_engine(
        "mssql://host/db",
        _credentials(),
        connect_args=None,
    )
    assert captured["connect_args"] == {}


# ---------------------------------------------------------------------------
# ODBC connection-string path
#
# When the ``uri`` field carries an ODBC ``KEY=value;…`` string (no
# ``://``), ``build_mssql_engine`` wraps it as
# ``mssql+pyodbc:///?odbc_connect=<urlencoded>`` and forwards user /
# password through ``connect_args`` (``UID`` / ``PWD``) rather than
# baking them into the URL host. Verify the discriminator and the
# resulting URL / connect_args.
# ---------------------------------------------------------------------------


_ODBC_URI = (
    "DRIVER=ODBC Driver 18 for SQL Server;"
    "SERVER=db.example.com,1433;"
    "Database=analytics;"
    "TrustServerCertificate=yes"
)


def test_is_odbc_connection_string_distinguishes_url_from_odbc():
    assert impl._is_odbc_connection_string(_ODBC_URI) is True
    assert impl._is_odbc_connection_string("mssql://host/db") is False
    assert impl._is_odbc_connection_string("mssql+pyodbc://host/db") is False


def test_build_mssql_engine_wraps_odbc_string_via_odbc_connect(captured):
    impl.build_mssql_engine(
        _ODBC_URI,
        _credentials(),
        connect_args=None,
    )
    url: URL = captured["url"]
    assert url.drivername == "mssql+pyodbc"
    # No host/credentials on the URL — they ride on ``odbc_connect``
    # (and ``UID`` / ``PWD`` on connect_args).
    assert url.host in (None, "")
    assert url.username in (None, "")
    odbc_connect = url.query.get("odbc_connect")
    assert odbc_connect is not None
    import urllib.parse
    assert urllib.parse.unquote_plus(odbc_connect) == _ODBC_URI


def test_build_mssql_engine_odbc_injects_uid_pwd_into_connect_args(captured):
    impl.build_mssql_engine(
        _ODBC_URI,
        _credentials(),
        connect_args=None,
    )
    assert captured["connect_args"]["UID"] == "etl"
    assert captured["connect_args"]["PWD"] == "hunter2"


def test_build_mssql_engine_odbc_caller_uid_pwd_take_precedence(captured):
    # The impl uses ``setdefault`` for UID / PWD, so explicit
    # ``connect_args`` win over the credentials-derived defaults.
    impl.build_mssql_engine(
        _ODBC_URI,
        _credentials(),
        connect_args={"UID": "override-user", "PWD": "override-pass"},
    )
    assert captured["connect_args"]["UID"] == "override-user"
    assert captured["connect_args"]["PWD"] == "override-pass"


def test_build_mssql_engine_odbc_accepts_secret_uri(captured):
    impl.build_mssql_engine(
        Secret(_ODBC_URI),
        _credentials(),
        connect_args=None,
    )
    url: URL = captured["url"]
    assert url.drivername == "mssql+pyodbc"
    assert url.query.get("odbc_connect") is not None


# ---------------------------------------------------------------------------
# Adapter dataclasses
# ---------------------------------------------------------------------------


def test_mssql_read_cfg_subclasses_sql_read_cfg():
    assert issubclass(impl.MSSQLReadCfg, SqlReadCfg)


def test_mssql_read_cfg_creates_engine_from_conn(captured):
    cfg = impl.MSSQLReadCfg(
        conn=_src_conn(),
        queries=["SELECT 1"],
        transactional=True,
        schema_overrides=None,
        initial_values=None,
        cfg=None,
    )
    cfg.create_sql_alchemy_engine()
    assert captured["url"].drivername == "mssql+pyodbc"
    # The conn_cfg dict should be the connect_args forwarded:
    assert captured["connect_args"] == {"connect_timeout": 5}


def test_mssql_write_cfg_subclasses_sql_write_cfg():
    assert issubclass(impl.MSSQLWriteCfg, SqlWriteCfg)


def test_mssql_write_cfg_creates_engine_from_conn(captured):
    cfg = impl.MSSQLWriteCfg(
        conn=_dest_conn(),
        target_tables=["t"],
        if_table_exists="append",
        transactional=True,
        cfg=None,
    )
    cfg.create_sql_alchemy_engine()
    assert captured["url"].drivername == "mssql+pyodbc"
    assert captured["connect_args"] == {"connect_timeout": 5}


def test_mssql_read_cfg_no_conn_cfg_forwards_empty_connect_args(captured):
    conn = MSSQLSrcConn(
        uri="mssql://host/db",
        credentials=_credentials(),
    )
    cfg = impl.MSSQLReadCfg(
        conn=conn,
        queries=["SELECT 1"],
        transactional=True,
        schema_overrides=None,
        initial_values=None,
        cfg=None,
    )
    cfg.create_sql_alchemy_engine()
    assert captured["connect_args"] == {}


# ---------------------------------------------------------------------------
# Connector defaults
# ---------------------------------------------------------------------------


def test_read_defaults_pick_polars_with_pandas_allowed():
    d = impl.MSSQL_READ_DEFAULTS
    assert d[const.READ_ENGINE_KEY] == const.POLARS_SQL_ALCHEMY_READ_ENGINE
    assert const.POLARS_SQL_ALCHEMY_READ_ENGINE in d[const.AVAIL_READ_ENGINES_KEY]
    assert const.PANDAS_SQL_ALCHEMY_READ_ENGINE in d[const.AVAIL_READ_ENGINES_KEY]


def test_write_defaults_pick_polars_only():
    d = impl.MSSQL_WRITE_DEFAULTS
    assert d[const.WRITE_ENGINE_KEY] == const.POLARS_SQL_ALCHEMY_WRITE_ENGINE
    assert d[const.AVAIL_WRITE_ENGINES_KEY] == [
        const.POLARS_SQL_ALCHEMY_WRITE_ENGINE,
    ]
