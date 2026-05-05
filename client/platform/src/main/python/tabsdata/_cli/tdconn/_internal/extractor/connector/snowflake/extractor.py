#
#  Copyright 2026. Tabs Data Inc.
#
"""Snowflake metadata extractor."""

from sqlalchemy import engine

from tabsdata._cli.tdconn._internal.extractor.connector.common_sql.common import (
    SqlAlchemyConnectionExtractor,
)
from tabsdata.connector.snowflake._connection import SnowflakeConn


class SnowflakeConnectionExtractor(SqlAlchemyConnectionExtractor):
    """Snowflake metadata via ``snowflake-sqlalchemy``.

    The ``info`` dict matches ``SnowflakeConn``'s JSON shape — all seven
    fields are required (pass empty strings for any unused slots):
        account, user, password, role, database, schema, warehouse.

    The extractor uses ``schema`` as its catalog filter.
    """

    def schema_filter(self, info: dict) -> str | None:
        conn = SnowflakeConn._load_from_dict(info)
        return conn.schema.secret_value or None

    def system_schemas(self) -> frozenset[str]:
        return frozenset({"INFORMATION_SCHEMA"})

    def make_url(self, info: dict) -> engine.URL:
        conn = SnowflakeConn._load_from_dict(info)
        query: dict[str, str] = {}
        warehouse = conn.warehouse.secret_value
        role = conn.role.secret_value
        if warehouse:
            query["warehouse"] = warehouse
        if role:
            query["role"] = role
        return engine.URL.create(
            drivername="snowflake",
            username=conn.user.secret_value,
            password=conn.password.secret_value,
            host=conn.account.secret_value,
            database=conn.database.secret_value,
            query=query,
        )
