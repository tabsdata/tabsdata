#
#  Copyright 2026 Tabsdata Inc.
#

from typing import TypeAlias
from urllib.parse import urlparse

from sqlalchemy import engine, make_url

from tabsdatak.conn.common.sql._explorer import SqlAlchemyConnectionExplorer
from tabsdatak.conn.mysql import MySQLDestConn, MySQLSrcConn

# noinspection PyProtectedMember
from tabsdatak.conn.mysql._uri import normalize_mysql_uri

# Either MySQL connection flavour -- source or destination.
MySQLConn: TypeAlias = MySQLSrcConn | MySQLDestConn


class MySQLConnectionExplorer(SqlAlchemyConnectionExplorer):
    """MySQL metadata through the same driver resolution as the
    read/write path, so check-connection exercises the DBAPI a run
    will actually use.

    Restrict the scan by encoding the target database in the URI path.
    """

    def schema_filter(self, conn: MySQLConn) -> str | None:
        return urlparse(conn._uri()).path.lstrip("/") or None

    def map_entity_fields(self, conn: MySQLConn, schema: str | None) -> dict:
        return {"database": schema or "", "schema": ""}

    def system_schemas(self) -> frozenset[str]:
        return frozenset({
            "information_schema",
            "mysql",
            "performance_schema",
            "sys",
        })

    def engine_kwargs(self, conn: MySQLConn) -> dict:
        return {"connect_args": dict(conn.conn_cfg or {})}

    def make_url(self, conn: MySQLConn) -> engine.URL:
        url = make_url(normalize_mysql_uri(conn._uri()))
        if conn.credentials is not None:
            url = url.set(
                username=conn.credentials.user.value(),
                password=conn.credentials.password.value(),
            )
        return url


class MySQLCdcConnectionExplorer(MySQLConnectionExplorer):
    """Metadata/preview for a MySQL CDC stream source.

    :class:`MySQLCdcStreamSrcConn` is attribute-identical to the batch
    connection (same ``uri`` / ``credentials`` / ``conn_cfg``), so the batch
    SQLAlchemy introspection surface applies unchanged.
    """
