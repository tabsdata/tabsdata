#
#  Copyright 2026. Tabs Data Inc.
#
"""ClickHouse metadata extractor."""

from sqlalchemy import engine

from tabsdata._cli.tdconn._internal.extractor.connector.common_sql.common import (
    SqlAlchemyConnectionExtractor,
)
from tabsdata._io.connections.clickhouse_connection import ClickHouseConn


class ClickHouseConnectionExtractor(SqlAlchemyConnectionExtractor):
    """ClickHouse metadata via ``clickhouse-sqlalchemy``.

    The ``info`` dict matches ``ClickHouseConn``'s JSON shape:
        host, port, database, secure, credentials, staging
    ClickHouseConn requires ``staging`` (an S3/Azure/GCS config); the
    extractor doesn't use it for reading metadata but it's still
    validated to keep the JSON one-to-one with the Conn definition.
    The ``database`` field also acts as the scan filter.
    """

    def schema_filter(self, info: dict) -> str | None:
        return ClickHouseConn._load_from_dict(info).database or None

    def map_entity_fields(self, info: dict, schema: str | None) -> dict:
        return {
            "database": schema or ClickHouseConn._load_from_dict(info).database or "",
            "schema": "",
        }

    def system_schemas(self) -> frozenset[str]:
        return frozenset({"system", "information_schema", "INFORMATION_SCHEMA"})

    def make_url(self, info: dict) -> engine.URL:
        conn = ClickHouseConn._load_from_dict(info)
        user = "default"
        password = ""
        if conn.credentials is not None:
            user = conn.credentials.user.secret_value or "default"
            password = conn.credentials.password.secret_value or ""
        return engine.URL.create(
            drivername="clickhouse+native",
            username=user,
            password=password,
            host=conn.host or "localhost",
            port=conn.port,
            database=conn.database or "default",
        )
