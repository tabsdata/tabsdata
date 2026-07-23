#
# Copyright 2026 Tabsdata Inc.
#

"""Databricks backend — a remote (non-container) SQL warehouse.

A domain is a SCHEMA inside the Unity ``CATALOG``. Connection comes from the
``DATABRICKS__`` environment (HOST/TOKEN/WAREHOUSE_ID/CATALOG, plus an optional
VOLUME); the data plane is inherited from :class:`RemoteSqlBackend` over the
``databricks`` SQLAlchemy dialect (which drives the SQL warehouse and resumes it
on connect). Databricks FK constraints are informational, so the employees
model's parent deletes apply cleanly.
"""

from __future__ import annotations

from urllib.parse import quote_plus

from tabsdatax.itest.tdksystems2 import options
from tabsdatax.itest.tdksystems2.systems._common.sql import RemoteSqlBackend


class Databricks(RemoteSqlBackend):
    slug = "databricks"
    label = "Databricks"
    domain_word = "schema"
    max_entity_name_length = 255
    driver = "databricks"
    env_prefix = "DATABRICKS"
    sql_quote = "`"
    namespace_meta = "CATALOG"
    system_schemas = frozenset({"information_schema", "default"})

    def options(self) -> tuple[options.Option, ...]:
        return (
            options.Option("HOST", "host", options.ENV, "env $DATABRICKS__HOST"),
            options.Option(
                "TOKEN", "token", options.ENV, "env $DATABRICKS__TOKEN", secret=True
            ),
            options.Option(
                "WAREHOUSE_ID",
                "warehouse-id",
                options.ENV,
                "env $DATABRICKS__WAREHOUSE_ID",
            ),
            options.Option(
                "CATALOG", "catalog", options.ENV, "env $DATABRICKS__CATALOG"
            ),
            options.Option(
                "VOLUME",
                "volume",
                options.ENV,
                "env $DATABRICKS__VOLUME (optional per-domain volume)",
                required=False,
            ),
        )

    @staticmethod
    def _host(meta: dict[str, str]) -> str:
        return meta["HOST"].replace("https://", "").replace("http://", "").rstrip("/")

    def _location(self, meta: dict[str, str]) -> str:
        return f"databricks://{self._host(meta)}/{meta.get('CATALOG', '')}"

    def _url(self, meta: dict[str, str], schema: str = "") -> str:
        http_path = quote_plus(f"/sql/1.0/warehouses/{meta['WAREHOUSE_ID']}")
        params = f"http_path={http_path}&catalog={quote_plus(meta['CATALOG'])}"
        if schema:
            params += f"&schema={quote_plus(schema)}"
        return f"databricks://token:{quote_plus(meta['TOKEN'])}@{self._host(meta)}?{params}"

    def _extra_domain_sql(self, sys_meta: dict[str, str], schema: str) -> list[str]:
        vol = sys_meta.get("VOLUME", "")
        if not vol:
            return []
        cat = sys_meta["CATALOG"]
        return [f"CREATE VOLUME IF NOT EXISTS `{cat}`.`{schema}`.`{vol}`"]
