#
#  Copyright 2026. Tabs Data Inc.
#
"""Db2 CDC metadata extractor.

CDC is a runtime concern (change-event stream); the *catalog* it
exposes is identical to the underlying database, so this extractor
reuses :class:`Db2ConnectionExtractor`. The ``info`` dict is validated
against :class:`Db2CdcConn` so its JSON matches the CDC connection
definition one-to-one.
"""

from urllib.parse import urlparse

from sqlalchemy import engine

from tabsdata._cli.tdconn._internal.extractor.connector.db2.extractor import (
    Db2ConnectionExtractor,
    _without_extras,
)
from tabsdata.connector.cdc.db2._connection import Db2CdcConn


class Db2CdcConnectionExtractor(Db2ConnectionExtractor):
    """Db2 CDC — JSON shape matches :class:`Db2CdcConn`."""

    def make_url(self, info: dict) -> engine.URL:
        conn = Db2CdcConn._load_from_dict(_without_extras(info))
        parsed = urlparse(conn.uri)
        user = password = ""
        if conn.credentials is not None:
            user = conn.credentials.user.secret_value
            password = conn.credentials.password.secret_value
        return engine.URL.create(
            drivername="ibm_db_sa",
            username=user,
            password=password,
            host=parsed.hostname or "localhost",
            port=parsed.port or 50000,
            database=(parsed.path or "").lstrip("/"),
        )
