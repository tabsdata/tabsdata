#
# Copyright 2026 Tabsdata Inc.
#

"""Databricks SaaS system — hooks only; the lifecycle is in SaasBase.

Domain == schema (`<catalog>.<name>`), created via the SQL Statement
Execution REST API, together with a Unity Catalog staging Volume
(`<catalog>.<name>.<VOLUME>`) when the `VOLUME` option is set.
"""

from __future__ import annotations

from .._saas import SaasBase
from ..base import DomainContext, Option
from . import connection


class DatabricksSystem(SaasBase):
    slug = "databricks"
    label = "Databricks"
    ident_quote = "`"  # Databricks quotes identifiers with backticks
    options = (
        Option("HOST", "host"),
        Option("TOKEN", "token", secret=True),
        Option("WAREHOUSE_ID", "warehouse-id"),
        Option("CATALOG", "catalog"),
        Option("VOLUME", "volume"),
    )

    # Fakeable seam: resolve `(host, token, warehouse_id, catalog)` once.
    # (Databricks is REST-based — no DB-API cursor like snowflake's `_cursor`.)
    def _resolve(self, values: dict[str, str]) -> tuple[str, str, str, str]:
        return connection.resolve(values)

    def _ping(self, values: dict[str, str]) -> None:
        host, token, wh_id, _catalog = self._resolve(values)
        connection.ping(host, token, wh_id)

    def _ensure_running(self, host: str, token: str, warehouse_id: str) -> None:
        connection.ensure_running(host, token, warehouse_id)

    def create_schema(self, values: dict[str, str], name: str, *, force: bool) -> None:
        host, token, wh_id, catalog = self._resolve(values)
        volume = values.get("VOLUME")
        self._ensure_running(host, token, wh_id)
        if force:
            connection.exec_sql(
                host,
                token,
                wh_id,
                catalog,
                f"DROP SCHEMA IF EXISTS `{catalog}`.`{name}` CASCADE",
            )
        connection.exec_sql(
            host, token, wh_id, catalog, f"CREATE SCHEMA `{catalog}`.`{name}`"
        )
        # Staging Volume the connector PUTs files into before COPY INTO; lives
        # inside the schema, so it is created after it (and dropped with it via
        # the CASCADE above).
        if volume:
            connection.exec_sql(
                host,
                token,
                wh_id,
                catalog,
                f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{name}`.`{volume}`",
            )

    def drop_schema(self, values: dict[str, str], name: str) -> None:
        host, token, wh_id, catalog = self._resolve(values)
        self._ensure_running(host, token, wh_id)
        connection.exec_sql(
            host,
            token,
            wh_id,
            catalog,
            f"DROP SCHEMA IF EXISTS `{catalog}`.`{name}` CASCADE",
        )

    def exec_statements(self, ctx: DomainContext, statements: list[str]) -> None:
        values = self.resolve_values(self.settings_from_meta(ctx.meta))
        host, token, wh_id, catalog = self._resolve(values)
        self._ensure_running(host, token, wh_id)
        schema = ctx.meta.get("SCHEMA")
        for stmt in statements:
            connection.exec_sql(host, token, wh_id, catalog, stmt, schema=schema)
