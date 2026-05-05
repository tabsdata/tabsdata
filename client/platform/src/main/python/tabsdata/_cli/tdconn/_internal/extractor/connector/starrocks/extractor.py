#
#  Copyright 2026. Tabs Data Inc.
#
"""StarRocks metadata extractor.

StarRocks speaks the MySQL wire protocol, so the MySQL driver works out
of the box. Only the system-schema list and default port differ.
"""

from sqlalchemy import engine

from tabsdata._cli.tdconn._internal.extractor.connector.common_sql.common import (
    SqlAlchemyConnectionExtractor,
)
from tabsdata._io.connections.starrocks_connection import StarRocksConn


class StarRocksConnectionExtractor(SqlAlchemyConnectionExtractor):
    """StarRocks metadata via the MySQL wire-compatible driver.

    The ``info`` dict matches ``StarRocksConn``'s JSON shape:
        host, port, database, credentials, staging
    StarRocksConn requires ``staging``; the extractor doesn't use it for
    metadata reads but it's still validated so the JSON matches the Conn
    definition one-to-one. The ``database`` field also acts as the scan
    filter.
    """

    def schema_filter(self, info: dict) -> str | None:
        return StarRocksConn._load_from_dict(info).database or None

    def map_entity_fields(self, info: dict, schema: str | None) -> dict:
        return {
            "database": schema or StarRocksConn._load_from_dict(info).database or "",
            "schema": "",
        }

    def system_schemas(self) -> frozenset[str]:
        return frozenset({
            "information_schema",
            "_statistics_",
            "sys",
            "mysql",
        })

    def make_url(self, info: dict) -> engine.URL:
        conn = StarRocksConn._load_from_dict(info)
        user = password = ""
        if conn.credentials is not None:
            user = conn.credentials.user.secret_value
            password = conn.credentials.password.secret_value
        return engine.URL.create(
            drivername="mysql+pymysql",
            username=user,
            password=password,
            host=conn.host or "localhost",
            port=conn.port,
            database=conn.database or "",
        )
