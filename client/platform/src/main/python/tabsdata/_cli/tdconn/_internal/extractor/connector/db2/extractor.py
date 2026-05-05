#
#  Copyright 2026. Tabs Data Inc.
#

"""DB2 metadata extractor."""

from urllib.parse import urlparse

from sqlalchemy import engine

from tabsdata._cli.tdconn._internal.extractor.connector.common_sql.common import (
    SqlAlchemyConnectionExtractor,
)
from tabsdata._io.connections.db2_connection import Db2Conn


class Db2ConnectionExtractor(SqlAlchemyConnectionExtractor):
    """DB2 metadata via ibm_db_sa.

    The ``info`` dict matches ``Db2Conn``'s JSON shape:
        uri: db2://host[:port][/database]
        credentials: {user, password}     (optional)

    Extractor-level option:
        schema: restrict the scan to the given schema (e.g. "DB2INST1").
    """

    def schema_filter(self, info: dict) -> str | None:
        return info.get("schema")

    def system_schemas(self) -> frozenset[str]:
        return frozenset({
            "SYSCAT",
            "SYSIBM",
            "SYSIBMADM",
            "SYSPROC",
            "SYSSTAT",
            "SYSTOOLS",
            "NULLID",
            "SQLJ",
        })

    def make_url(self, info: dict) -> engine.URL:
        conn = Db2Conn._load_from_dict(_without_extras(info))
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


def _without_extras(info: dict) -> dict:
    return {k: v for k, v in info.items() if k != "schema"}
