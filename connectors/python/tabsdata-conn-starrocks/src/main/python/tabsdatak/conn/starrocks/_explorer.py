#
#  Copyright 2026. Tabs Data Inc.
#

"""StarRocks metadata explorer."""

from sqlalchemy import engine

from tabsdatak.conn.common.sql._explorer import SqlAlchemyConnectionExplorer

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import _convert
from tabsdatak.conn.starrocks import StarRocksDestConn


class StarRocksConnectionExplorer(SqlAlchemyConnectionExplorer):
    """StarRocks metadata via the MySQL wire protocol.

    Drives the shared :class:`SqlAlchemyConnectionExplorer`
    introspection surface (``check_connect`` / ``metadata`` / ``data``)
    against a :class:`StarRocksDestConn`.
    """

    def schema_filter(self, conn: StarRocksDestConn) -> str | None:
        return _convert(conn.database, str) or None

    def map_entity_fields(self, conn: StarRocksDestConn, schema: str | None) -> dict:
        return {"database": schema or conn.database or "", "schema": ""}

    def system_schemas(self) -> frozenset[str]:
        return frozenset(
            {
                "information_schema",
                "_statistics_",
                "sys",
                "mysql",
            }
        )

    def engine_kwargs(self, conn: StarRocksDestConn) -> dict:
        return conn.conn_cfg or {}

    def make_url(self, conn: StarRocksDestConn) -> engine.URL:
        user = password = ""
        if conn.credentials is not None:
            user = conn.credentials.user.value()
            password = conn.credentials.password.value()
        return engine.URL.create(
            drivername="mysql+pymysql",
            username=user,
            password=password,
            host=_convert(conn.host, str) or "localhost",
            port=_convert(conn.port, int),
            database=_convert(conn.database, str) or "",
        )
