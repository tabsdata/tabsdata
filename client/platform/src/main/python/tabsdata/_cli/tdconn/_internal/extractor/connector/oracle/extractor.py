#
#  Copyright 2026. Tabs Data Inc.
#

"""Oracle metadata extractor."""

from urllib.parse import urlparse

from tabsdata._cli.tdconn._internal.extractor.connector.common_sql.common import (
    SqlAlchemyConnectionExtractor,
)
from tabsdata._io.connections.oracle_connection import OracleConn


class OracleConnectionExtractor(SqlAlchemyConnectionExtractor):
    """Oracle metadata via oracledb (thin mode).

    The ``info`` dict matches ``OracleConn``'s JSON shape:
        uri: oracle://host[:port][/database]  (or oracle+oracledb://)
        credentials: {user, password}         (optional)

    Extractor-level options:
        owner: restrict the scan to the given owner (Oracle schema = owner).
        service_name: when true, treat the URI's database segment as a
            service name rather than a SID.
    """

    def schema_filter(self, info: dict) -> str | None:
        return info.get("owner")

    def system_schemas(self) -> frozenset[str]:
        return frozenset({
            "SYS",
            "SYSTEM",
            "SYSAUX",
            "OUTLN",
            "DBSNMP",
            "APPQOSSYS",
            "DBSFWUSER",
            "GSMADMIN_INTERNAL",
            "CTXSYS",
            "MDSYS",
            "ORDSYS",
            "ORDDATA",
            "ORDPLUGINS",
            "XDB",
            "WMSYS",
            "EXFSYS",
            "OLAPSYS",
            "LBACSYS",
            "DVSYS",
            "OJVMSYS",
            "AUDSYS",
            "GGSYS",
            "GGSHAREDCAP",
            "ANONYMOUS",
            "APEX_PUBLIC_USER",
            "FLOWS_FILES",
            "APEX_040000",
            "APEX_030200",
            "ORACLE_OCM",
            "SI_INFORMTN_SCHEMA",
            "OWBSYS",
            "REMOTE_SCHEDULER_AGENT",
        })

    def make_url(self, info: dict) -> str:
        conn = OracleConn._load_from_dict(_without_extras(info))
        parsed = urlparse(conn.uri)
        user = password = ""
        if conn.credentials is not None:
            user = conn.credentials.user.secret_value
            password = conn.credentials.password.secret_value
        host = parsed.hostname or "localhost"
        port = parsed.port or 1521
        database = (parsed.path or "").lstrip("/")
        use_service_name = bool(info.get("service_name", False))
        if use_service_name:
            return f"oracle+oracledb://{user}:{password}@{host}:{port}/?service_name={database}"
        return f"oracle+oracledb://{user}:{password}@{host}:{port}/{database}"


def _without_extras(info: dict) -> dict:
    return {k: v for k, v in info.items() if k not in ("owner", "service_name")}
