#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest
from sqlalchemy.engine import URL

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword
# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine import constants as const
# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine import sqlalchemy_utils
# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine.reader import SqlReadCfg
# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine.writer import SqlWriteCfg
from tabsdatak.conn.postgres import PostgresDestConn, PostgresSrcConn
# noinspection PyProtectedMember
from tabsdatak.conn.postgres import _impl as impl


pytestmark = pytest.mark.postgres


# ---------------------------------------------------------------------------
# build_postgres_engine -- driver injection, scheme aliasing, credential set
#
# The postgres DBAPI may not be installed in the test env, so we intercept
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


def _src_conn() -> PostgresSrcConn:
    return PostgresSrcConn(
        uri="postgresql://postgres.example.com:5432/analytics",
        credentials=_credentials(),
        conn_cfg={"connect_timeout": 5},
    )


def _dest_conn() -> PostgresDestConn:
    return PostgresDestConn(
        uri="postgresql://postgres.example.com:5432/analytics",
        credentials=_credentials(),
        conn_cfg={"connect_timeout": 5},
    )


@pytest.fixture
def captured(monkeypatch):
    """Monkey-patch the shared engine proxy to capture call args."""
    box: dict = {}

    def fake_create_engine(url, *, connect_args):
        box["url"] = url
        box["connect_args"] = connect_args
        return _CapturedURL(url, connect_args)

    monkeypatch.setattr(sqlalchemy_utils, "create_engine", fake_create_engine)
    return box


def test_build_postgres_engine_injects_psycopg2_driver_when_missing(captured):
    impl.build_postgres_engine(
        "postgresql://host:5432/db",
        _credentials(),
        connect_args=None,
    )
    url: URL = captured["url"]
    assert url.drivername == "postgresql+psycopg2"
    assert url.username == "etl"
    assert url.password == "hunter2"
    assert url.host == "host"
    assert url.port == 5432


def test_build_postgres_engine_preserves_explicit_driver(captured):
    impl.build_postgres_engine(
        "postgresql+psycopg2://host:5432/db",
        _credentials(),
        connect_args=None,
    )
    assert captured["url"].drivername == "postgresql+psycopg2"


def test_build_postgres_engine_accepts_postgres_scheme_alias(captured):
    # ``postgres://`` is rewritten to canonical ``postgresql://``;
    # the default driver is then injected.
    impl.build_postgres_engine(
        "postgres://host/db",
        _credentials(),
        connect_args=None,
    )
    assert captured["url"].drivername == "postgresql+psycopg2"


def test_build_postgres_engine_preserves_driver_with_postgres_alias(captured):
    # Driver-qualified ``postgres+psycopg2://`` is also accepted.
    impl.build_postgres_engine(
        "postgres+psycopg2://host/db",
        _credentials(),
        connect_args=None,
    )
    assert captured["url"].drivername == "postgresql+psycopg2"


def test_build_postgres_engine_accepts_secret_uri(captured):
    impl.build_postgres_engine(
        Secret("postgresql://host:5432/db"),
        _credentials(),
        connect_args=None,
    )
    assert captured["url"].drivername == "postgresql+psycopg2"


def test_build_postgres_engine_passes_connect_args(captured):
    impl.build_postgres_engine(
        "postgresql://host/db",
        _credentials(),
        connect_args={"sslmode": "require"},
    )
    assert captured["connect_args"] == {"sslmode": "require"}


def test_build_postgres_engine_with_no_connect_args_passes_empty_dict(captured):
    impl.build_postgres_engine(
        "postgresql://host/db",
        _credentials(),
        connect_args=None,
    )
    assert captured["connect_args"] == {}


# ---------------------------------------------------------------------------
# Adapter dataclasses
# ---------------------------------------------------------------------------


def test_postgres_read_cfg_subclasses_sql_read_cfg():
    assert issubclass(impl.PostgresReadCfg, SqlReadCfg)


def test_postgres_read_cfg_creates_engine_from_conn(captured):
    cfg = impl.PostgresReadCfg(
        conn=_src_conn(),
        queries=["SELECT 1"],
        transactional=True,
        schema_overrides=None,
        initial_values=None,
        cfg=None,
    )
    cfg.create_sql_alchemy_engine()
    assert captured["url"].drivername == "postgresql+psycopg2"
    # The conn_cfg dict should be the connect_args forwarded:
    assert captured["connect_args"] == {"connect_timeout": 5}


def test_postgres_write_cfg_subclasses_sql_write_cfg():
    assert issubclass(impl.PostgresWriteCfg, SqlWriteCfg)


def test_postgres_write_cfg_creates_engine_from_conn(captured):
    cfg = impl.PostgresWriteCfg(
        conn=_dest_conn(),
        target_tables=["t"],
        if_table_exists="append",
        transactional=True,
        cfg=None,
    )
    cfg.create_sql_alchemy_engine()
    assert captured["url"].drivername == "postgresql+psycopg2"
    assert captured["connect_args"] == {"connect_timeout": 5}


def test_postgres_read_cfg_no_conn_cfg_forwards_empty_connect_args(captured):
    conn = PostgresSrcConn(
        uri="postgresql://host/db",
        credentials=_credentials(),
    )
    cfg = impl.PostgresReadCfg(
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
    d = impl.POSTGRES_READ_DEFAULTS
    assert d[const.READ_ENGINE_KEY] == const.POLARS_SQL_ALCHEMY_READ_ENGINE
    assert const.POLARS_SQL_ALCHEMY_READ_ENGINE in d[const.AVAIL_READ_ENGINES_KEY]
    assert const.PANDAS_SQL_ALCHEMY_READ_ENGINE in d[const.AVAIL_READ_ENGINES_KEY]


def test_write_defaults_pick_polars_only():
    d = impl.POSTGRES_WRITE_DEFAULTS
    assert d[const.WRITE_ENGINE_KEY] == const.POLARS_SQL_ALCHEMY_WRITE_ENGINE
    assert d[const.AVAIL_WRITE_ENGINES_KEY] == [
        const.POLARS_SQL_ALCHEMY_WRITE_ENGINE,
    ]
