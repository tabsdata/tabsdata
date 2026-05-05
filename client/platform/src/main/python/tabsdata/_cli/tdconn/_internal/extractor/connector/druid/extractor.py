#
#  Copyright 2026. Tabs Data Inc.
#
"""Apache Druid metadata extractor."""

from urllib.parse import urlparse

from sqlalchemy import engine

from tabsdata._cli.tdconn._internal.extractor.connector.common_sql.common import (
    SqlAlchemyConnectionExtractor,
)
from tabsdata._io.connections.druid_connection import DruidConn


class DruidConnectionExtractor(SqlAlchemyConnectionExtractor):
    """Druid metadata via ``pydruid`` SQLAlchemy dialect.

    The ``info`` dict matches ``DruidConn``'s JSON shape:
        overlord_url, staging, credentials, plus any of the other
        ``DruidConn`` tuning knobs (``time_column``, ``segment_granularity``, …).
    ``DruidConn`` was designed for cloud-staged ingestion; the extractor
    only needs a SQL endpoint, so it re-uses the ``overlord_url`` host/port
    and appends a broker SQL path.

    Extractor-level options:
        path: SQL endpoint path (default ``"druid/v2/sql"``).
        tls: when true, use the ``druid+https`` dialect.
    """

    def schema_filter(self, info: dict) -> str | None:
        return info.get("schema") or "druid"

    def system_schemas(self) -> frozenset[str]:
        return frozenset({"INFORMATION_SCHEMA", "sys", "lookup"})

    def make_url(self, info: dict) -> engine.URL:
        conn = DruidConn._load_from_dict(_without_extras(info))
        parsed = urlparse(conn.overlord_url)
        user = password = ""
        if conn.credentials is not None:
            user = conn.credentials.user.secret_value
            password = conn.credentials.password.secret_value
        scheme = "druid+https" if info.get("tls", False) else "druid"
        return engine.URL.create(
            drivername=scheme,
            username=user,
            password=password,
            host=parsed.hostname or "localhost",
            port=parsed.port or 8082,
            database=info.get("path", "druid/v2/sql"),
        )


def _without_extras(info: dict) -> dict:
    extras = {"schema", "path", "tls"}
    return {k: v for k, v in info.items() if k not in extras}
