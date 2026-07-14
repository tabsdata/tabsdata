#
#  Copyright 2026. Tabs Data Inc.
#

"""Oracle metadata explorer."""

from typing import TypeAlias
from urllib.parse import parse_qsl, urlparse

from sqlalchemy import engine

from tabsdatak.conn.common.sql._explorer import SqlAlchemyConnectionExplorer

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import _convert
from tabsdatak.conn.oracle import OracleDestConn, OracleSrcConn

# Either Oracle connection flavour -- source or destination.
OracleConn: TypeAlias = OracleSrcConn | OracleDestConn


class OracleConnectionExplorer(SqlAlchemyConnectionExplorer):
    """Oracle metadata via oracledb (thin mode).

    Drives the shared :class:`SqlAlchemyConnectionExplorer`
    introspection surface (``check_connect`` / ``metadata`` / ``data``)
    against an :class:`OracleSrcConn` or :class:`OracleDestConn`.
    """

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

    def map_entity_fields(self, conn: OracleConn, schema: str | None) -> dict:
        return {"database": "", "schema": schema or ""}

    def engine_kwargs(self, conn: OracleConn) -> dict:
        # SQLAlchemy's Oracle dialect hides tables in the SYSTEM / SYSAUX
        # tablespaces from reflection by default; clear that so a user schema
        # whose tables happen to live there is still introspected. System
        # *schemas* are excluded separately via `system_schemas`.
        return {"exclude_tablespaces": ()} | (conn.conn_cfg or {})

    def make_url(self, conn: OracleConn) -> engine.URL:
        # `uri` is a str or a `Secret`; `_convert` yields the plain str
        # form either way (mirrors the runtime `build_oracle_engine`).
        parsed = urlparse(_convert(conn.uri, str))
        user = password = ""
        if conn.credentials is not None:
            user = conn.credentials.user.value()
            password = conn.credentials.password.value()
        return engine.URL.create(
            drivername="oracle+oracledb",
            username=user,
            password=password,
            host=parsed.hostname or "localhost",
            port=parsed.port or 1521,
            database=(parsed.path or "").lstrip("/"),
            # Keep query params (e.g. ``service_name``) -- on Oracle the
            # target is often a service rather than a path-style SID.
            query=dict(parse_qsl(parsed.query)),
        )
