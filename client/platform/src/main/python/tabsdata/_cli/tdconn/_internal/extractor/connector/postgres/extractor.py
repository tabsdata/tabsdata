#
#  Copyright 2026. Tabs Data Inc.
#

"""PostgreSQL metadata extractor."""

from urllib.parse import urlparse

from sqlalchemy import engine

from tabsdata._cli.tdconn._internal.extractor.connector.common_sql.common import (
    SqlAlchemyConnectionExtractor,
)
from tabsdata._io.connections.postgres_connection import PostgresConn


class PostgresConnectionExtractor(SqlAlchemyConnectionExtractor):
    """PostgreSQL metadata via psycopg2.

    The ``info`` dict matches ``PostgresConn``'s JSON shape:
        uri: postgresql://host[:port][/database]
        credentials: {user, password}     (optional)

    Extractor-level option:
        schema: restrict the scan to the given schema (default enumerates all).
    """

    def schema_filter(self, info: dict) -> str | None:
        return info.get("schema")

    def system_schemas(self) -> frozenset[str]:
        return frozenset({
            "information_schema",
            "pg_catalog",
            "pg_toast",
            "pg_temp_1",
            "pg_toast_temp_1",
        })

    def make_url(self, info: dict) -> engine.URL:
        conn = PostgresConn._load_from_dict(_without_extras(info))
        parsed = urlparse(conn.uri)
        user = password = ""
        if conn.credentials is not None:
            user = conn.credentials.user.secret_value
            password = conn.credentials.password.secret_value
        return engine.URL.create(
            drivername="postgresql",
            username=user,
            password=password,
            host=parsed.hostname or "localhost",
            port=parsed.port or 5432,
            database=(parsed.path or "").lstrip("/"),
        )


def _without_extras(info: dict) -> dict:
    """Strip extractor-only keys (e.g. ``schema``) before Conn validation."""
    return {k: v for k, v in info.items() if k != "schema"}
