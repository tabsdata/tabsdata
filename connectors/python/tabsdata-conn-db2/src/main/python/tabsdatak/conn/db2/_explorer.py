#
#  Copyright 2026. Tabs Data Inc.
#

"""Db2 metadata explorer."""

from typing import TypeAlias

from sqlalchemy import engine, make_url as sa_make_url

from tabsdatak.conn.common.sql._explorer import SqlAlchemyConnectionExplorer
from tabsdatak.conn.db2 import Db2DestConn, Db2SrcConn

# Either Db2 connection flavour -- source or destination.
Db2Conn: TypeAlias = Db2SrcConn | Db2DestConn


class Db2ConnectionExplorer(SqlAlchemyConnectionExplorer):
    """Db2 metadata via ibm_db_sa.

    Drives the shared :class:`SqlAlchemyConnectionExplorer`
    introspection surface (``check_connect`` / ``metadata`` / ``data``)
    against a :class:`Db2SrcConn` or :class:`Db2DestConn`.
    """

    def connectivity_probe(self) -> str:
        # Db2 rejects a bare ``SELECT 1``; it requires a FROM clause.
        return "SELECT 1 FROM SYSIBM.SYSDUMMY1"

    def schema_filter(self, conn: Db2Conn) -> str | None:
        src = sa_make_url(conn._uri())
        cur = src.query.get("currentSchema") or src.query.get("CurrentSchema")
        if cur:
            return (cur[0] if isinstance(cur, (list, tuple)) else cur).upper()
        if conn.credentials is None:
            return None
        return conn.credentials.user.value().upper()

    def system_schemas(self) -> frozenset[str]:
        return frozenset({
            "SYSCAT",
            "SYSIBM",
            "SYSIBMADM",
            "SYSPROC",
            "SYSSTAT",
            "SYSTOOLS",
            "NULLID",
            "SQLJ",
        })

    def make_url(self, conn: Db2Conn) -> engine.URL:
        src = sa_make_url(conn._uri())
        user = password = ""
        if conn.credentials is not None:
            user = conn.credentials.user.value()
            password = conn.credentials.password.value()
        return engine.URL.create(
            drivername="db2+ibm_db",
            username=user,
            password=password,
            host=src.host or "localhost",
            port=src.port or 50000,
            database=src.database or "",
            query=src.query,
        )
