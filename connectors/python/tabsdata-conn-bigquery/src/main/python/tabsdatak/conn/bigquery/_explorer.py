#
#  Copyright 2026 Tabsdata Inc.
#

"""BigQuery metadata explorer."""

import json

from sqlalchemy import engine

from tabsdatak.conn.bigquery import BigQueryDestConn
from tabsdatak.conn.common.sql._explorer import SqlAlchemyConnectionExplorer

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import _convert


class BigQueryConnectionExplorer(SqlAlchemyConnectionExplorer):
    """BigQuery metadata via the sqlalchemy-bigquery dialect.

    Drives the shared :class:`SqlAlchemyConnectionExplorer`
    introspection surface (``check_connect`` / ``metadata`` / ``data``)
    against a :class:`BigQueryDestConn`.
    """

    def schema_filter(self, conn: BigQueryDestConn) -> str | None:
        if conn.dataset is None:
            return None
        return conn._dataset() or None

    def map_entity_fields(self, conn: BigQueryDestConn, schema: str | None) -> dict:
        project = conn._project() if conn.project is not None else ""
        return {"database": project, "schema": schema or ""}

    def system_schemas(self) -> frozenset[str]:
        return frozenset({"INFORMATION_SCHEMA"})

    def engine_kwargs(self, conn: BigQueryDestConn) -> dict:
        # BigQuery auth can't go in the URL; the dialect reads a parsed
        # service-account dict from ``credentials_info``. An empty dict
        # would make the dialect fall back to ADC, so skip it.
        conn_cfg = conn.conn_cfg or {}
        if conn.credentials is None:
            return conn_cfg
        raw = _convert(conn.credentials.json, str)
        payload = json.loads(raw) if raw else {}
        if not payload:
            return conn_cfg
        return conn_cfg | {"credentials_info": payload}

    def make_url(self, conn: BigQueryDestConn) -> engine.URL:
        project = conn._project() if conn.project is not None else ""
        dataset = conn._dataset() if conn.dataset is not None else ""

        return engine.URL.create(
            drivername="bigquery",
            host=project or None,
            database=dataset or None,
        )
