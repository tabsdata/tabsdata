from __future__ import annotations

import urllib.parse
from dataclasses import dataclass
from typing import Any

from sqlalchemy import create_engine, make_url
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
from tabsdatak.conn.mssql import MSSQLDestConn, MSSQLSrcConn, UriSpec

# --- MSSQL scheme / driver tables --------------------------------

_SCHEME_ALIASES: dict[str, str] = {}

_DRIVERS: dict[str, str] = {"mssql": "pyodbc"}


# --- Conn helpers ------------------------------------------------


def _is_odbc_connection_string(value: str) -> bool:
    """ODBC connection strings have no scheme separator. The
    validator already restricts the input to one of the two shapes,
    so this is just the URI/ODBC discriminator."""
    return "://" not in value


def build_mssql_engine(
    uri: UriSpec,
    credentials: UserPassword,
    connect_args: CfgSpec | None,
) -> Engine:
    uri = _convert(uri, str)

    user = _convert(credentials.user, str)
    password = _convert(credentials.password, str)
    args: dict[str, Any] = dict(connect_args or {})

    if _is_odbc_connection_string(uri):
        # Wrap the ODBC string as ``mssql+pyodbc:///?odbc_connect=...``.
        # User / password are passed via connect_args (``UID`` / ``PWD``)
        # rather than the URL because pyodbc reads the connect string
        # from the query parameter verbatim.
        encoded = urllib.parse.quote_plus(uri)
        url = make_url(f"mssql+pyodbc:///?odbc_connect={encoded}")
        args.setdefault("UID", user)
        args.setdefault("PWD", password)
        return create_engine(url, connect_args=args)

    uri = _engine.sqlalchemy_utils.normalize_scheme(uri, aliases=_SCHEME_ALIASES)
    uri = _engine.sqlalchemy_utils.add_driver_to_uri(uri, drivers=_DRIVERS)
    url = make_url(uri)
    url = url.set(username=user, password=password)
    return create_engine(url, connect_args=args)


# --- Src: SqlReadCfg adapter + defaults ---------------------------


@dataclass(kw_only=True)
class MSSQLReadCfg(SqlReadCfg[MSSQLSrcConn]):
    def create_sql_alchemy_engine(self) -> Engine:
        return build_mssql_engine(
            self.conn.uri,
            self.conn.credentials,
            self.conn.conn_cfg if self.conn.conn_cfg else None,
        )


MSSQL_READ_DEFAULTS: dict[str, Any] = {
    _engine.constants.READ_ENGINE_KEY: _engine.constants.POLARS_SQL_ALCHEMY_READ_ENGINE,
    _engine.constants.AVAIL_READ_ENGINES_KEY: [
        _engine.constants.POLARS_SQL_ALCHEMY_READ_ENGINE,
        _engine.constants.PANDAS_SQL_ALCHEMY_READ_ENGINE,
    ],
}


# --- Dest: SqlWriteCfg adapter + defaults -------------------------


@dataclass(kw_only=True)
class MSSQLWriteCfg(SqlWriteCfg[MSSQLDestConn]):
    def create_sql_alchemy_engine(self) -> Engine:
        return build_mssql_engine(
            self.conn.uri,
            self.conn.credentials,
            self.conn.conn_cfg if self.conn.conn_cfg else None,
        )


MSSQL_WRITE_DEFAULTS: dict[str, Any] = {
    _engine.constants.WRITE_ENGINE_KEY: (
        _engine.constants.POLARS_SQL_ALCHEMY_WRITE_ENGINE
    ),
    _engine.constants.AVAIL_WRITE_ENGINES_KEY: [
        _engine.constants.POLARS_SQL_ALCHEMY_WRITE_ENGINE,
    ],
}
