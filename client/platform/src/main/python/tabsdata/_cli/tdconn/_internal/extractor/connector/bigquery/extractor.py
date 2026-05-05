#
#  Copyright 2026. Tabs Data Inc.
#
"""Google BigQuery metadata extractor."""

import json

from sqlalchemy import engine

from tabsdata._cli.tdconn._internal.extractor.connector.common_sql.common import (
    SqlAlchemyConnectionExtractor,
)
from tabsdata.connector.bigquery._connector import BigQueryConn


class BigQueryConnectionExtractor(SqlAlchemyConnectionExtractor):
    """BigQuery metadata via ``sqlalchemy-bigquery``.

    The ``info`` dict matches ``BigQueryConn``'s JSON shape:
        gcs_folder, credentials, project, dataset

    The service-account key (inline JSON under
    ``credentials.service_account_key``) is forwarded to ``create_engine``
    as ``credentials_info``; no service-account file path is accepted
    since the Conn JSON carries the key inline.
    """

    def schema_filter(self, info: dict) -> str | None:
        # BigQuery datasets map to SQLAlchemy schemas.
        return BigQueryConn._load_from_dict(info).dataset

    def map_entity_fields(self, info: dict, schema: str | None) -> dict:
        conn = BigQueryConn._load_from_dict(info)
        return {"database": conn.project or "", "schema": schema or ""}

    def system_schemas(self) -> frozenset[str]:
        return frozenset({"INFORMATION_SCHEMA"})

    def make_url(self, info: dict) -> engine.URL:
        conn = BigQueryConn._load_from_dict(info)
        project = conn.project or ""
        dataset = conn.dataset or ""
        path = f"/{project}/{dataset}" if dataset else f"/{project}"
        return engine.URL.create(drivername="bigquery", database=path.lstrip("/"))

    def engine_kwargs(self, info: dict) -> dict:
        conn = BigQueryConn._load_from_dict(info)
        key = (
            conn.credentials.service_account_key.secret_value
            if conn.credentials
            else ""
        )
        if not key:
            return {}
        payload = json.loads(key) if isinstance(key, str) else key
        # Empty dicts shouldn't be forwarded — the dialect treats an
        # empty ``credentials_info`` as "use ADC", which is confusing.
        if not payload:
            return {}
        return {"credentials_info": payload}
