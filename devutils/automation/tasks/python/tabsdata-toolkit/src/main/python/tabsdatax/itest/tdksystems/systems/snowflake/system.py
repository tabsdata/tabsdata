#
# Copyright 2026 Tabsdata Inc.
#

"""Snowflake SaaS system — hooks only; the lifecycle is in SaasBase.

Domain == schema (`<database>.<name>`). Auth uses a Personal Access Token
(PAT) passed as the connector's `password`.
"""

from __future__ import annotations

from .._saas import SaasBase
from ..base import DomainContext, Option
from . import connection


class SnowflakeSystem(SaasBase):
    slug = "snowflake"
    label = "Snowflake"
    options = (
        Option("ACCOUNT", "account"),
        Option("USERNAME", "username"),
        Option("PAT", "pat", secret=True),
        Option("WAREHOUSE", "warehouse"),
        Option("DATABASE", "database"),
    )

    # Fakeable seam (connector cursor context manager).
    def _cursor(self, values: dict[str, str]):
        return connection.cursor(values)

    def _ping(self, values: dict[str, str]) -> None:
        with self._cursor(values) as cur:
            cur.execute("SELECT 1")
            cur.fetchone()

    def create_schema(self, values: dict[str, str], name: str, *, force: bool) -> None:
        db = values.get("DATABASE", "")
        with self._cursor(values) as cur:
            if force:
                cur.execute(f'DROP SCHEMA IF EXISTS "{db}"."{name}" CASCADE')
            cur.execute(f'CREATE SCHEMA "{db}"."{name}"')

    def drop_schema(self, values: dict[str, str], name: str) -> None:
        db = values.get("DATABASE", "")
        with self._cursor(values) as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{db}"."{name}" CASCADE')

    def exec_statements(self, ctx: DomainContext, statements: list[str]) -> None:
        values = self.resolve_values(self.settings_from_meta(ctx.meta))
        db, schema = values.get("DATABASE", ""), ctx.meta.get("SCHEMA")
        with self._cursor(values) as cur:
            if schema:
                cur.execute(f'USE SCHEMA "{db}"."{schema}"')
            for stmt in statements:
                cur.execute(stmt)
