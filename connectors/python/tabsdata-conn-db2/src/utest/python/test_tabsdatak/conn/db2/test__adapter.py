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
from tabsdatak.conn.db2 import Db2Conn
# noinspection PyProtectedMember
from tabsdatak.conn.db2 import _adapter as adapter


pytestmark = pytest.mark.db2


# ---------------------------------------------------------------------------
# build_db2_engine -- driver injection + credential set
#
# The ibm_db DBAPI may not be installed in the test env, so we intercept
# ``create_engine`` and verify the URL that the adapter built.
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


def _conn() -> Db2Conn:
    return Db2Conn(
        uri="db2://db2.example.com:50000/SAMPLE",
        credentials=_credentials(),
        conn_cfg={"connect_timeout": 5},
    )


@pytest.fixture
def captured(monkeypatch):
    """Monkey-patch ``adapter.create_engine`` to capture call args."""
    box: dict = {}

    def fake_create_engine(url, *, connect_args):
        box["url"] = url
        box["connect_args"] = connect_args
        return _CapturedURL(url, connect_args)

    monkeypatch.setattr(adapter, "create_engine", fake_create_engine)
    return box


def test_build_db2_engine_injects_ibm_db_sa_driver_when_missing(captured):
    adapter.build_db2_engine(
        "db2://host:50000/db",
        _credentials(),
        connect_args=None,
    )
    url: URL = captured["url"]
    assert url.drivername == "db2+ibm_db_sa"
    assert url.username == "etl"
    assert url.password == "hunter2"
    assert url.host == "host"
    assert url.port == 50000


def test_build_db2_engine_preserves_explicit_driver(captured):
    adapter.build_db2_engine(
        "db2+ibm_db://host:50000/db",
        _credentials(),
        connect_args=None,
    )
    assert captured["url"].drivername == "db2+ibm_db"


def test_build_db2_engine_accepts_ibm_db_sa_scheme(captured):
    # No alias for ibm_db_sa -> stays as-is.
    adapter.build_db2_engine(
        "ibm_db_sa://host/db",
        _credentials(),
        connect_args=None,
    )
    assert captured["url"].drivername == "ibm_db_sa"


def test_build_db2_engine_accepts_secret_uri(captured):
    adapter.build_db2_engine(
        Secret.lit("db2://host:50000/db"),
        _credentials(),
        connect_args=None,
    )
    assert captured["url"].drivername == "db2+ibm_db_sa"


def test_build_db2_engine_passes_connect_args(captured):
    adapter.build_db2_engine(
        "db2://host/db",
        _credentials(),
        connect_args={"timeout": 30},
    )
    assert captured["connect_args"] == {"timeout": 30}


def test_build_db2_engine_with_no_connect_args_passes_empty_dict(captured):
    adapter.build_db2_engine(
        "db2://host/db",
        _credentials(),
        connect_args=None,
    )
    assert captured["connect_args"] == {}


# ---------------------------------------------------------------------------
# Adapter dataclasses
# ---------------------------------------------------------------------------


def test_db2_read_cfg_subclasses_sql_read_cfg():
    assert issubclass(adapter.Db2ReadCfg, SqlReadCfg)


def test_db2_read_cfg_creates_engine_from_conn(captured):
    cfg = adapter.Db2ReadCfg(
        conn=_conn(),
        queries=["SELECT 1 FROM SYSIBM.SYSDUMMY1"],
        transactional=True,
        schema_overrides=None,
        query_params=None,
        cfg=None,
    )
    cfg.create_sql_alchemy_engine()
    assert captured["url"].drivername == "db2+ibm_db_sa"
    # The conn_cfg dict should be the connect_args forwarded:
    assert captured["connect_args"] == {"connect_timeout": 5}


def test_db2_write_cfg_subclasses_sql_write_cfg():
    assert issubclass(adapter.Db2WriteCfg, SqlWriteCfg)


def test_db2_write_cfg_creates_engine_from_conn(captured):
    cfg = adapter.Db2WriteCfg(
        conn=_conn(),
        target_tables=["t"],
        if_table_exists="append",
        transactional=True,
        cfg=None,
    )
    cfg.create_sql_alchemy_engine()
    assert captured["url"].drivername == "db2+ibm_db_sa"
    assert captured["connect_args"] == {"connect_timeout": 5}


def test_db2_read_cfg_no_conn_cfg_forwards_empty_connect_args(captured):
    conn = Db2Conn(
        uri="db2://host/db",
        credentials=_credentials(),
    )
    cfg = adapter.Db2ReadCfg(
        conn=conn,
        queries=["SELECT 1 FROM SYSIBM.SYSDUMMY1"],
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
    d = adapter.DB2_READ_DEFAULTS
    assert d[const.READ_ENGINE_KEY] == const.POLARS_SQL_ALCHEMY_READ_ENGINE
    assert const.POLARS_SQL_ALCHEMY_READ_ENGINE in d[const.AVAIL_READ_ENGINES_KEY]
    assert const.PANDAS_SQL_ALCHEMY_READ_ENGINE in d[const.AVAIL_READ_ENGINES_KEY]


def test_write_defaults_pick_polars_only():
    d = adapter.DB2_WRITE_DEFAULTS
    assert d[const.WRITE_ENGINE_KEY] == const.POLARS_SQL_ALCHEMY_WRITE_ENGINE
    assert d[const.AVAIL_WRITE_ENGINES_KEY] == [
        const.POLARS_SQL_ALCHEMY_WRITE_ENGINE,
    ]
