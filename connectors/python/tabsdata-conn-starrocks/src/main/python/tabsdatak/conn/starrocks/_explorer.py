#
#  Copyright 2026 Tabsdata Inc.
#

"""StarRocks metadata explorer."""

from sqlalchemy import engine
from sqlalchemy.exc import DBAPIError

from tabsdatak.conn.common.sql._explorer import SqlAlchemyConnectionExplorer
from tabsdatak.conn.starrocks import StarRocksDestConn

# StarRocks rejects a statement naming a table it cannot resolve in the analyzer,
# under MySQL's generic syntax-error code, instead of sending the unknown-table
# 1146 that the MySQL dialect turns into `NoSuchTableError`.
_ANALYZER_ERROR = 1064
_TABLE_GONE = "is not found"


class StarRocksConnectionExplorer(SqlAlchemyConnectionExplorer):
    """StarRocks metadata via the MySQL wire protocol.

    Drives the shared :class:`SqlAlchemyConnectionExplorer`
    introspection surface (``check_connect`` / ``metadata`` / ``data``)
    against a :class:`StarRocksDestConn`.
    """

    def is_missing_table_error(self, error: DBAPIError) -> bool:
        args = tuple(getattr(error.orig, "args", ()))
        return args[:1] == (_ANALYZER_ERROR,) and _TABLE_GONE in str(error.orig)

    def schema_filter(self, conn: StarRocksDestConn) -> str | None:
        return conn._database() or None

    def map_entity_fields(self, conn: StarRocksDestConn, schema: str | None) -> dict:
        return {"database": schema or conn.database or "", "schema": ""}

    def system_schemas(self) -> frozenset[str]:
        return frozenset({
            "information_schema",
            "_statistics_",
            "sys",
            "mysql",
        })

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
            host=conn._host() or "localhost",
            port=conn._port(),
            database=conn._database() or "",
        )
