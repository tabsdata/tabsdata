#
#  Copyright 2026. Tabs Data Inc.
#
"""MySQL CDC metadata extractor.

CDC is a runtime concern (change-event stream); the *catalog* it
exposes is identical to the underlying database, so this extractor
reuses :class:`MySQLConnectionExtractor`. The ``info`` dict is
validated against :class:`MySQLCdcConn` so its JSON matches the CDC
connection definition one-to-one.
"""

from urllib.parse import urlparse

from sqlalchemy import engine

from tabsdata._cli.tdconn._internal.extractor.connector.mysql.extractor import (
    MySQLConnectionExtractor,
)
from tabsdata.connector.cdc.mysql._connection import MySQLCdcConn


class MySQLCdcConnectionExtractor(MySQLConnectionExtractor):
    """MySQL CDC — JSON shape matches :class:`MySQLCdcConn`."""

    def schema_filter(self, info: dict) -> str | None:
        return urlparse(MySQLCdcConn._load_from_dict(info).uri).path.lstrip("/") or None

    def make_url(self, info: dict) -> engine.URL:
        conn = MySQLCdcConn._load_from_dict(info)
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
