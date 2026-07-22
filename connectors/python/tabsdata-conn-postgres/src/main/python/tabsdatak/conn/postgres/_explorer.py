#
#  Copyright 2026 Tabsdata Inc.
#

from typing import TypeAlias
from urllib.parse import parse_qsl, urlparse

from sqlalchemy import engine

from tabsdatak.conn.common.sql._explorer import SqlAlchemyConnectionExplorer
from tabsdatak.conn.postgres import PostgresDestConn, PostgresSrcConn

# Either Postgres connection flavour -- source or destination.
PostgresConn: TypeAlias = PostgresSrcConn | PostgresDestConn


class PostgresConnectionExplorer(SqlAlchemyConnectionExplorer):
    """PostgreSQL metadata via psycopg2.

    Drives the shared :class:`SqlAlchemyConnectionExplorer`
    introspection surface (``check_connect`` / ``metadata`` / ``data``)
    against a :class:`PostgresSrcConn` or :class:`PostgresDestConn`.
    """

    def system_schemas(self) -> frozenset[str]:
        return frozenset({
            "information_schema",
            "pg_catalog",
            "pg_toast",
            "pg_temp_1",
            "pg_toast_temp_1",
        })

    def map_entity_fields(self, conn: PostgresConn, schema: str | None) -> dict:
        parsed = urlparse(conn._uri())
        return {
            "database": parsed.path.lstrip("/") if parsed.path else "",
            "schema": schema or "",
        }

    def engine_kwargs(self, conn: PostgresConn) -> dict:
        return conn.conn_cfg or {}

    def make_url(self, conn: PostgresConn) -> engine.URL:
        # `uri` is a str or a `Secret`; `_uri()` yields the plain str
        # form either way (mirrors the runtime `build_postgres_engine`).
        parsed = urlparse(conn._uri())
        user = password = ""
        if conn.credentials is not None:
            user = conn.credentials.user.value()
            password = conn.credentials.password.value()
        return engine.URL.create(
            drivername="postgresql+psycopg2",
            username=user,
            password=password,
            host=parsed.hostname or "localhost",
            port=parsed.port or 5432,
            database=(parsed.path or "").lstrip("/"),
            query=dict(parse_qsl(parsed.query)),
        )


class PostgresCdcConnectionExplorer(PostgresConnectionExplorer):
    """Metadata/preview for a Postgres CDC stream source.

    :class:`PostgresCdcStreamSrcConn` is attribute-identical to the batch
    connection (same ``uri`` / ``credentials`` / ``conn_cfg``), so the batch
    SQLAlchemy introspection surface applies unchanged.
    """
