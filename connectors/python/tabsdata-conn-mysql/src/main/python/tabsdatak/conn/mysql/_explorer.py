#
#  Copyright 2026. Tabs Data Inc.
#

from typing import TypeAlias
from urllib.parse import parse_qsl, urlparse

from sqlalchemy import engine

from tabsdatak.conn.common.sql._explorer import SqlAlchemyConnectionExplorer
from tabsdatak.conn.mysql import MySQLDestConn, MySQLSrcConn

# Either MySQL connection flavour -- source or destination.
MySQLConn: TypeAlias = MySQLSrcConn | MySQLDestConn


class MySQLConnectionExplorer(SqlAlchemyConnectionExplorer):
    """MySQL metadata via PyMySQL.

    Restrict the scan by encoding the target database in the URI path.
    """

    def schema_filter(self, conn: MySQLConn) -> str | None:
        return urlparse(conn.uri).path.lstrip("/") or None

    def map_entity_fields(self, conn: MySQLConn, schema: str | None) -> dict:
        return {"database": schema or "", "schema": ""}

    def system_schemas(self) -> frozenset[str]:
        return frozenset(
            {
                "information_schema",
                "mysql",
                "performance_schema",
                "sys",
            }
        )

    def engine_kwargs(self, conn: MySQLConn) -> dict:
        return conn.conn_cfg or {}

    def make_url(self, conn: MySQLConn) -> engine.URL:
        parsed = urlparse(conn.uri)
        user = password = ""
        if conn.credentials is not None:
            user = conn.credentials.user.value()
            password = conn.credentials.password.value()
        return engine.URL.create(
            drivername="mysql+pymysql",
            username=user,
            password=password,
            host=parsed.hostname or "localhost",
            port=parsed.port or 3306,
            database=(parsed.path or "").lstrip("/"),
            query=dict(parse_qsl(parsed.query)),
        )
