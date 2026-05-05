#
#  Copyright 2026. Tabs Data Inc.
#
"""MariaDB metadata extractor.

Wire-compatible with MySQL; overrides the MySQL extractor only to
validate the ``info`` dict against ``MariaDBConn`` so it matches the
Conn's JSON shape one-to-one.
"""

from urllib.parse import urlparse

from sqlalchemy import engine

from tabsdata._cli.tdconn._internal.extractor.connector.mysql.extractor import (
    MySQLConnectionExtractor,
)
from tabsdata._io.connections.mariadb_connection import MariaDBConn


class MariaDbConnectionExtractor(MySQLConnectionExtractor):
    """MariaDB metadata — JSON shape matches :class:`MariaDBConn`."""

    def schema_filter(self, info: dict) -> str | None:
        return urlparse(MariaDBConn._load_from_dict(info).uri).path.lstrip("/") or None

    def make_url(self, info: dict) -> engine.URL:
        conn = MariaDBConn._load_from_dict(info)
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
