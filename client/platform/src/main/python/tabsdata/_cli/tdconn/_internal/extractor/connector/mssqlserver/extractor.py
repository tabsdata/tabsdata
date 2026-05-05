#
#  Copyright 2026. Tabs Data Inc.
#

"""SQL Server metadata extractor."""

from urllib.parse import urlparse

from sqlalchemy import engine

from tabsdata._cli.tdconn._internal.extractor.connector.common_sql.common import (
    SqlAlchemyConnectionExtractor,
)
from tabsdata.connector.mssql.connection.mssql_connection import MSSQLConn


class MSSQLServerConnectionExtractor(SqlAlchemyConnectionExtractor):
    """SQL Server metadata via pyodbc.

    The ``info`` dict matches ``MSSQLConn``'s JSON shape:
        connection_string_or_uri: mssql:// URI or full ODBC connection string
        credentials: {user, password}     (optional — omit for trusted auth)

    Extractor-level options:
        schema: restrict the scan to the given schema (e.g. "dbo").
        driver: ODBC driver name (default: "ODBC Driver 18 for SQL Server").
        trusted_connection: when true, skip credentials and use integrated auth.
        trust_server_certificate: passthrough to the ODBC
            ``TrustServerCertificate`` flag.
    """

    def schema_filter(self, info: dict) -> str | None:
        return info.get("schema")

    def system_schemas(self) -> frozenset[str]:
        return frozenset({
            "sys",
            "INFORMATION_SCHEMA",
            "db_owner",
            "db_accessadmin",
            "db_securityadmin",
            "db_ddladmin",
            "db_backupoperator",
            "db_datareader",
            "db_datawriter",
            "db_denydatareader",
            "db_denydatawriter",
            "guest",
        })

    def make_url(self, info: dict) -> engine.URL:
        conn = MSSQLConn._load_from_dict(_without_extras(info))
        parsed = urlparse(conn.connection_string_or_uri)
        driver = info.get("driver", "ODBC Driver 18 for SQL Server")
        trusted = bool(info.get("trusted_connection", False))
        query = {
            "driver": driver,
            "TrustServerCertificate": "yes"
            if info.get("trust_server_certificate", True)
            else "no",
        }
        if trusted:
            query["Trusted_Connection"] = "yes"

        user = password = ""
        if not trusted and conn.credentials is not None:
            user = conn.credentials.user.secret_value
            password = conn.credentials.password.secret_value

        return engine.URL.create(
            drivername="mssql+pyodbc",
            username=user,
            password=password,
            host=parsed.hostname or "localhost",
            port=parsed.port or 1433,
            database=(parsed.path or "").lstrip("/"),
            query=query,
        )


def _without_extras(info: dict) -> dict:
    extras = {"schema", "driver", "trusted_connection", "trust_server_certificate"}
    return {k: v for k, v in info.items() if k not in extras}
