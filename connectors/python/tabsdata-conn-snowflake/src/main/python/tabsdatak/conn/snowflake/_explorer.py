#
#  Copyright 2026. Tabs Data Inc.
#

"""Snowflake metadata explorer."""

from sqlalchemy import engine

from tabsdatak.conn.common.sql._explorer import SqlAlchemyConnectionExplorer

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import _convert
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
        return _convert(conn.schema, str) or None

    def system_schemas(self) -> frozenset[str]:
        return frozenset({"INFORMATION_SCHEMA"})

    def engine_kwargs(self, conn: SnowflakeDestConn) -> dict:
        return conn.conn_cfg or {}

    def make_url(self, conn: SnowflakeDestConn) -> engine.URL:
        query: dict[str, str] = {}
        if conn.warehouse is not None:
            query["warehouse"] = _convert(conn.warehouse, str)
        if conn.role is not None:
            query["role"] = _convert(conn.role, str)
        user = password = ""
        if conn.credentials is not None:
            user = conn.credentials.user.value()
            password = conn.credentials.password.value()
        return engine.URL.create(
            drivername="snowflake",
            username=user,
            password=password,
            # The account locator is the host for the snowflake dialect.
            host=_convert(conn.account, str),
            database=(
                _convert(conn.database, str) if conn.database is not None else None
            ),
            query=query,
        )
