#
#  Copyright 2026 Tabsdata Inc.
#

"""Snowflake metadata explorer."""

from sqlalchemy import engine
from sqlalchemy.sql.elements import quoted_name

from tabsdatak.conn.common.sql._explorer import SqlAlchemyConnectionExplorer
from tabsdatak.conn.snowflake import SnowflakeDestConn


class SnowflakeConnectionExplorer(SqlAlchemyConnectionExplorer):
    """Snowflake metadata via the snowflake-sqlalchemy dialect.

    Drives the shared :class:`SqlAlchemyConnectionExplorer`
    introspection surface (``check_connect`` / ``metadata`` / ``data``)
    against a :class:`SnowflakeDestConn`.
    """

    def schema_filter(self, conn: SnowflakeDestConn) -> str | None:
        if conn.schema is None:
            return None
        name = conn._schema() or None
        # Snowflake schema names are case-sensitive as stored; tdx provisions them
        # lowercase (quoted), so force-quote to reference them verbatim rather than
        # letting the dialect upper-fold an unquoted name.
        return quoted_name(name, quote=True) if name else None

    def system_schemas(self) -> frozenset[str]:
        return frozenset({"INFORMATION_SCHEMA"})

    def engine_kwargs(self, conn: SnowflakeDestConn) -> dict:
        return conn.conn_cfg or {}

    def map_entity_fields(self, conn: SnowflakeDestConn, schema: str | None) -> dict:
        """Map the iterated schema name to entity database/schema fields."""
        return {
            "database": conn._database() if conn.database is not None else "",
            "schema": schema or "",
        }

    def _data_schema(self, entity):
        # `SQLEntity` coerces the quoted_name back to a plain str, so re-quote
        # here (as in `schema_filter`) so `data()` references the case-sensitive
        # schema verbatim rather than letting the dialect upper-fold it.
        return quoted_name(entity.schema, quote=True) if entity.schema else None

    def make_url(self, conn: SnowflakeDestConn) -> engine.URL:
        query: dict[str, str] = {}
        if conn.warehouse is not None:
            query["warehouse"] = conn._warehouse()
        if conn.role is not None:
            query["role"] = conn._role()
        user = password = ""
        if conn.credentials is not None:
            user = conn.credentials.user.value()
            password = conn.credentials.password.value()
        return engine.URL.create(
            drivername="snowflake",
            username=user,
            password=password,
            # The account locator is the host for the snowflake dialect.
            host=conn._account(),
            database=(conn._database() if conn.database is not None else None),
            query=query,
        )
