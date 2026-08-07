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
from tabsdatak.conn.db2 import Db2DestConn, Db2SrcConn, UriSpec

# --- Db2 scheme / driver tables -----------------------------------

_SCHEME_ALIASES: dict[str, str] = {}

_DRIVERS: dict[str, str] = {"db2": "ibm_db_sa"}


# --- Conn helpers ------------------------------------------------


def build_db2_engine(
    uri: UriSpec,
    credentials: UserPassword,
    connect_args: CfgSpec | None,
) -> Engine:
    uri = _convert(uri, str)
    uri = _engine.sqlalchemy_utils.normalize_scheme(uri, aliases=_SCHEME_ALIASES)
    uri = _engine.sqlalchemy_utils.add_driver_to_uri(uri, drivers=_DRIVERS)
    url = make_url(uri)

    user = _convert(credentials.user, str)
    password = _convert(credentials.password, str)
    url = url.set(username=user, password=password)

    return _engine.sqlalchemy_utils.create_engine(
        url, connect_args=dict(connect_args or {})
    )


# --- Src: SqlReadCfg adapter + defaults ---------------------------


@dataclass(kw_only=True)
class Db2ReadCfg(SqlReadCfg[Db2SrcConn]):
    def create_sql_alchemy_engine(self) -> Engine:
        return build_db2_engine(
            self.conn.uri,
            self.conn.credentials,
            self.conn.conn_cfg if self.conn.conn_cfg else None,
        )


DB2_READ_DEFAULTS: dict[str, Any] = {
    _engine.constants.READ_ENGINE_KEY: _engine.constants.POLARS_SQL_ALCHEMY_READ_ENGINE,
    _engine.constants.AVAIL_READ_ENGINES_KEY: [
        _engine.constants.POLARS_SQL_ALCHEMY_READ_ENGINE,
        _engine.constants.PANDAS_SQL_ALCHEMY_READ_ENGINE,
    ],
}


# --- Dest: SqlWriteCfg adapter + defaults -------------------------


@dataclass(kw_only=True)
class Db2WriteCfg(SqlWriteCfg[Db2DestConn]):
    def create_sql_alchemy_engine(self) -> Engine:
        return build_db2_engine(
            self.conn.uri,
            self.conn.credentials,
            self.conn.conn_cfg if self.conn.conn_cfg else None,
        )


DB2_WRITE_DEFAULTS: dict[str, Any] = {
    _engine.constants.WRITE_ENGINE_KEY: (
        _engine.constants.POLARS_SQL_ALCHEMY_WRITE_ENGINE
    ),
    _engine.constants.AVAIL_WRITE_ENGINES_KEY: [
        _engine.constants.POLARS_SQL_ALCHEMY_WRITE_ENGINE,
    ],
}
