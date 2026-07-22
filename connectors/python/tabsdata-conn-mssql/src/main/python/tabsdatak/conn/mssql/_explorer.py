#
#  Copyright 2026 Tabsdata Inc.
#

"""SQL Server metadata explorer."""

from typing import TypeAlias
from urllib.parse import parse_qsl, quote_plus, urlparse

from sqlalchemy import engine, make_url as sa_make_url

from tabsdatak.conn.common.sql._explorer import SqlAlchemyConnectionExplorer
from tabsdatak.conn.mssql import MSSQLDestConn, MSSQLSrcConn

# Either SQL Server connection flavour -- source or destination.
MSSQLConn: TypeAlias = MSSQLSrcConn | MSSQLDestConn


def _is_odbc_connection_string(uri: str) -> bool:
    # An ODBC connection string has no scheme separator; a ``mssql://`` URI
    # does. (Mirrors the runtime ``build_mssql_engine``; the conn validator
    # accepts both shapes.)
    return "://" not in uri


class MSSQLServerConnectionExplorer(SqlAlchemyConnectionExplorer):
    """SQL Server metadata via pyodbc.

    Drives the shared :class:`SqlAlchemyConnectionExplorer`
    introspection surface (``check_connect`` / ``metadata`` / ``data``)
    against an :class:`MSSQLSrcConn` or :class:`MSSQLDestConn`.
    """

    def system_schemas(self) -> frozenset[str]:
        return frozenset({
            "sys",
            "INFORMATION_SCHEMA",
            "db_owner",
            "db_accessadmin",
            "db_securityadmin",
            "db_ddladmin",
            "db_backupoperator",
            "db_datareader",
            "db_datawriter",
            "db_denydatareader",
            "db_denydatawriter",
            "guest",
        })

    def make_url(self, conn: MSSQLConn) -> engine.URL:
        # `uri` is a str or a `Secret`; `_uri()` yields the plain str
        # form either way (mirrors the runtime `build_mssql_engine`).
        uri = conn._uri()
        if _is_odbc_connection_string(uri):
            # A raw ODBC connection string can't be parsed into URL parts;
            # pyodbc reads it verbatim from the ``odbc_connect`` query param.
            # Credentials ride in connect_args instead of the URL -- see
            # `engine_kwargs` (mirrors the runtime `build_mssql_engine`).
            return sa_make_url(f"mssql+pyodbc:///?odbc_connect={quote_plus(uri)}")
        parsed = urlparse(uri)
        user = password = ""
        if conn.credentials is not None:
            user = conn.credentials.user.value()
            password = conn.credentials.password.value()
        return engine.URL.create(
            drivername=parsed.scheme or "mssql+pyodbc",
            username=user,
            password=password,
            host=parsed.hostname or "localhost",
            port=parsed.port or 1433,
            database=(parsed.path or "").lstrip("/"),
            # Keep query params (ODBC ``driver`` / ``TrustServerCertificate``):
            # pyodbc needs the driver name and the cert flag to connect.
            query=dict(parse_qsl(parsed.query)),
        )

    def engine_kwargs(self, conn: MSSQLConn) -> dict:
        # For the ODBC-connection-string form the URL carries no credentials
        # (pyodbc reads the connect string verbatim), so user / password must
        # be supplied as ``UID`` / ``PWD`` via connect_args -- mirroring the
        # runtime `build_mssql_engine`. The URI form keeps them in the URL.
        conn_cfg = conn.conn_cfg or {}
        uri = conn._uri()
        if not _is_odbc_connection_string(uri) or conn.credentials is None:
            return conn_cfg
        return conn_cfg | {
            "connect_args": {
                "UID": conn.credentials.user.value(),
                "PWD": conn.credentials.password.value(),
            }
        }
