#
#  Copyright 2026 Tabsdata Inc.
#

"""Druid metadata explorer."""

from urllib.parse import urlparse

from sqlalchemy import engine

from tabsdatak.conn.common.sql._explorer import SqlAlchemyConnectionExplorer
from tabsdatak.conn.druid import DruidDestConn

# Druid's SQL endpoint lives on the Broker; its standard port.
_DRUID_BROKER_PORT = 8082
# Broker SQL HTTP path used by the pydruid SQLAlchemy dialect.
_DRUID_SQL_PATH = "druid/v2/sql"


class DruidConnectionExplorer(SqlAlchemyConnectionExplorer):
    """Druid metadata via the pydruid SQLAlchemy dialect.

    :categories: spi

    Drives the shared `SqlAlchemyConnectionExplorer` introspection
    surface (`check_connect` / `metadata` / `data`) against a
    `DruidDestConn`.

    Adapted from the CLI `DruidConnectionExtractor`. Two
    Druid-specific notes:

    * Catalog / data reads target the Broker SQL endpoint, but the conn
      only carries `overlord_url` (the ingestion service). We reuse
      its host and assume the Broker's standard port (8082) and SQL
      path -- correct for a standard cluster where the Broker is
      co-located.
    * Datasources live in the `druid` schema; the scan is pinned
      there, and `INFORMATION_SCHEMA` / `sys` / `lookup` are
      skipped.
    """

    def schema_filter(self, conn: DruidDestConn) -> str | None:
        # Druid datasources are exposed under the `druid` schema.
        return "druid"

    def system_schemas(self) -> frozenset[str]:
        return frozenset({"INFORMATION_SCHEMA", "sys", "lookup"})

    def make_url(self, conn: DruidDestConn) -> engine.URL:
        parsed = urlparse(conn._overlord_url())
        user = password = ""
        if conn.credentials is not None:
            user = conn.credentials.user.value()
            password = conn.credentials.password.value()
        return engine.URL.create(
            drivername="druid",
            username=user,
            password=password,
            host=parsed.hostname or "localhost",
            port=parsed.port or _DRUID_BROKER_PORT,
            database=_DRUID_SQL_PATH,
        )
