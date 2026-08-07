#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import make_url
from sqlalchemy.engine import Engine

# noinspection PyProtectedMember
import tabsdatak.conn.common.sql._engine as _engine

# Submodule imports register ``constants`` / ``sqlalchemy_utils`` as
# attributes of the ``_engine`` namespace package so the qualified
# call-sites below resolve.
# noinspection PyProtectedMember
import tabsdatak.conn.common.sql._engine.constants  # noqa: F401

# noinspection PyProtectedMember
import tabsdatak.conn.common.sql._engine.sqlalchemy_utils  # noqa: F401

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine.reader import SqlReadCfg

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine.writer import SqlWriteCfg

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import CfgSpec, UserPassword, _convert
from tabsdatak.conn.mysql import MySQLDestConn, MySQLSrcConn, UriSpec

# noinspection PyProtectedMember
from tabsdatak.conn.mysql._uri import normalize_mysql_uri

# --- Conn helpers ------------------------------------------------


def build_mysql_engine(
    uri: UriSpec,
    credentials: UserPassword,
    connect_args: CfgSpec | None,
) -> Engine:
    uri = _convert(uri, str)
    url = make_url(normalize_mysql_uri(uri))

    user = _convert(credentials.user, str)
    password = _convert(credentials.password, str)
    url = url.set(username=user, password=password)

    return _engine.sqlalchemy_utils.create_engine(
        url, connect_args=dict(connect_args or {})
    )


# --- Src: SqlReadCfg adapter + defaults ---------------------------


@dataclass(kw_only=True)
class MySQLReadCfg(SqlReadCfg[MySQLSrcConn]):
    def create_sql_alchemy_engine(self) -> Engine:
        return build_mysql_engine(
            self.conn.uri,
            self.conn.credentials,
            self.conn.conn_cfg if self.conn.conn_cfg else None,
        )


MYSQL_READ_DEFAULTS: dict[str, Any] = {
    _engine.constants.READ_ENGINE_KEY: _engine.constants.POLARS_SQL_ALCHEMY_READ_ENGINE,
    _engine.constants.AVAIL_READ_ENGINES_KEY: [
        _engine.constants.POLARS_SQL_ALCHEMY_READ_ENGINE,
        _engine.constants.PANDAS_SQL_ALCHEMY_READ_ENGINE,
    ],
}


# --- Dest: SqlWriteCfg adapter + defaults -------------------------


@dataclass(kw_only=True)
class MySQLWriteCfg(SqlWriteCfg[MySQLDestConn]):
    def create_sql_alchemy_engine(self) -> Engine:
        return build_mysql_engine(
            self.conn.uri,
            self.conn.credentials,
            self.conn.conn_cfg if self.conn.conn_cfg else None,
        )


MYSQL_WRITE_DEFAULTS: dict[str, Any] = {
    _engine.constants.WRITE_ENGINE_KEY: (
        _engine.constants.POLARS_SQL_ALCHEMY_WRITE_ENGINE
    ),
    _engine.constants.AVAIL_WRITE_ENGINES_KEY: [
        _engine.constants.POLARS_SQL_ALCHEMY_WRITE_ENGINE,
    ],
}
