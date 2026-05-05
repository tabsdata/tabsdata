#
#  Copyright 2026. Tabs Data Inc.
#
"""PostgreSQL CDC metadata extractor.

CDC is a runtime concern (change-event stream); the *catalog* it
exposes is identical to the underlying database, so this extractor
reuses :class:`PostgresConnectionExtractor`. The ``info`` dict is
validated against :class:`PostgresCdcConn` so its JSON matches the
CDC connection definition one-to-one.
"""

from urllib.parse import urlparse

from sqlalchemy import engine

from tabsdata._cli.tdconn._internal.extractor.connector.postgres.extractor import (
    PostgresConnectionExtractor,
    _without_extras,
)
from tabsdata.connector.cdc.postgres._connection import PostgresCdcConn


class PostgresCdcConnectionExtractor(PostgresConnectionExtractor):
    """Postgres CDC — JSON shape matches :class:`PostgresCdcConn`."""

    def make_url(self, info: dict) -> engine.URL:
        conn = PostgresCdcConn._load_from_dict(_without_extras(info))
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
