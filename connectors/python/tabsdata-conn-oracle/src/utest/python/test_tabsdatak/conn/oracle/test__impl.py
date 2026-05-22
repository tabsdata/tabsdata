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
from tabsdatak.conn.oracle import OracleDestConn, OracleSrcConn
# noinspection PyProtectedMember
from tabsdatak.conn.oracle import _impl as impl


pytestmark = pytest.mark.oracle


# ---------------------------------------------------------------------------
# build_oracle_engine -- driver injection + credential set
#
# The oracle DBAPI may not be installed in the test env, so we intercept
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


def _src_conn() -> OracleSrcConn:
    return OracleSrcConn(
        uri="oracle://oracle.example.com:1521/orcl",
        credentials=_credentials(),
        conn_cfg={"connect_timeout": 5},
    )


def _dest_conn() -> OracleDestConn:
    return OracleDestConn(
        uri="oracle://oracle.example.com:1521/orcl",
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


def test_build_oracle_engine_injects_oracledb_driver_when_missing(captured):
    impl.build_oracle_engine(
        "oracle://host:1521/orcl",
        _credentials(),
        connect_args=None,
    )
    url: URL = captured["url"]
    assert url.drivername == "oracle+oracledb"
    assert url.username == "etl"
    assert url.password == "hunter2"
    assert url.host == "host"
    assert url.port == 1521


def test_build_oracle_engine_preserves_explicit_driver(captured):
    impl.build_oracle_engine(
        "oracle+oracledb://host:1521/orcl",
        _credentials(),
        connect_args=None,
    )
    assert captured["url"].drivername == "oracle+oracledb"


def test_build_oracle_engine_accepts_secret_uri(captured):
    impl.build_oracle_engine(
        Secret.lit("oracle://host:1521/orcl"),
        _credentials(),
        connect_args=None,
    )
    assert captured["url"].drivername == "oracle+oracledb"


def test_build_oracle_engine_passes_connect_args(captured):
    impl.build_oracle_engine(
        "oracle://host/orcl",
        _credentials(),
        connect_args={"events": True},
    )
    assert captured["connect_args"] == {"events": True}


def test_build_oracle_engine_with_no_connect_args_passes_empty_dict(captured):
    impl.build_oracle_engine(
        "oracle://host/orcl",
        _credentials(),
        connect_args=None,
    )
    assert captured["connect_args"] == {}


# ---------------------------------------------------------------------------
# Adapter dataclasses
# ---------------------------------------------------------------------------


def test_oracle_read_cfg_subclasses_sql_read_cfg():
    assert issubclass(impl.OracleReadCfg, SqlReadCfg)


def test_oracle_read_cfg_creates_engine_from_conn(captured):
    cfg = impl.OracleReadCfg(
        conn=_src_conn(),
        queries=["SELECT 1 FROM DUAL"],
        transactional=True,
        schema_overrides=None,
        query_params=None,
        cfg=None,
    )
    cfg.create_sql_alchemy_engine()
    assert captured["url"].drivername == "oracle+oracledb"
    # The conn_cfg dict should be the connect_args forwarded:
    assert captured["connect_args"] == {"connect_timeout": 5}


def test_oracle_write_cfg_subclasses_sql_write_cfg():
    assert issubclass(impl.OracleWriteCfg, SqlWriteCfg)


def test_oracle_write_cfg_creates_engine_from_conn(captured):
    cfg = impl.OracleWriteCfg(
        conn=_dest_conn(),
        target_tables=["t"],
        if_table_exists="append",
        transactional=True,
        cfg=None,
    )
    cfg.create_sql_alchemy_engine()
    assert captured["url"].drivername == "oracle+oracledb"
    assert captured["connect_args"] == {"connect_timeout": 5}


def test_oracle_read_cfg_no_conn_cfg_forwards_empty_connect_args(captured):
    conn = OracleSrcConn(
        uri="oracle://host/orcl",
        credentials=_credentials(),
    )
    cfg = impl.OracleReadCfg(
        conn=conn,
        queries=["SELECT 1 FROM DUAL"],
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
    d = impl.ORACLE_READ_DEFAULTS
    assert d[const.READ_ENGINE_KEY] == const.POLARS_SQL_ALCHEMY_READ_ENGINE
    assert const.POLARS_SQL_ALCHEMY_READ_ENGINE in d[const.AVAIL_READ_ENGINES_KEY]
    assert const.PANDAS_SQL_ALCHEMY_READ_ENGINE in d[const.AVAIL_READ_ENGINES_KEY]


def test_write_defaults_pick_polars_only():
    d = impl.ORACLE_WRITE_DEFAULTS
    assert d[const.WRITE_ENGINE_KEY] == const.POLARS_SQL_ALCHEMY_WRITE_ENGINE
    assert d[const.AVAIL_WRITE_ENGINES_KEY] == [
        const.POLARS_SQL_ALCHEMY_WRITE_ENGINE,
    ]
