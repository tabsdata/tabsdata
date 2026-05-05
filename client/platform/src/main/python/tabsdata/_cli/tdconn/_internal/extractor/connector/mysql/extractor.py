#
#  Copyright 2026. Tabs Data Inc.
#

"""MySQL metadata extractor."""

from urllib.parse import urlparse

from sqlalchemy import engine

from tabsdata._cli.tdconn._internal.extractor.connector.common_sql.common import (
    SqlAlchemyConnectionExtractor,
)
from tabsdata._io.connections.mysql_connection import MySQLConn


class MySQLConnectionExtractor(SqlAlchemyConnectionExtractor):
    """MySQL metadata via PyMySQL.

    The ``info`` dict matches ``MySQLConn``'s JSON shape:
        uri: mysql://host[:port][/database]
        credentials: {user, password}     (optional)
    Restrict the scan by encoding the target database in the URI path.
    """

    def schema_filter(self, info: dict) -> str | None:
        return urlparse(MySQLConn._load_from_dict(info).uri).path.lstrip("/") or None

    def map_entity_fields(self, info: dict, schema: str | None) -> dict:
        return {"database": schema or "", "schema": ""}

    def system_schemas(self) -> frozenset[str]:
        return frozenset({
            "information_schema",
            "mysql",
            "performance_schema",
            "sys",
        })

    def make_url(self, info: dict) -> engine.URL:
        conn = MySQLConn._load_from_dict(info)
        parsed = urlparse(conn.uri)
        user = password = ""
        if conn.credentials is not None:
            user = conn.credentials.user.secret_value
            password = conn.credentials.password.secret_value
        return engine.URL.create(
            drivername="mysql+pymysql",
            username=user,
            password=password,
            host=parsed.hostname or "localhost",
            port=parsed.port or 3306,
            database=(parsed.path or "").lstrip("/"),
        )
