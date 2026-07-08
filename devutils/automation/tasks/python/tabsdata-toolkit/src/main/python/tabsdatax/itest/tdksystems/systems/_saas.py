"""Shared SaaS lifecycle on the unified `System` contract.

A saas system's pluggable logic: `_ping(values)` (auth check) and — for
schema-domain systems (databricks/snowflake) — `create_schema` /
`drop_schema` / `exec_statements`. Accounts are externally owned, so there's
no provisioning / teardown / pause. `salesforce` sets `domain_is_named = False`
(a credentials clone, no schema) and `supports_sql = False` (manual workflow).
"""

from __future__ import annotations

import re

from ..errors import DatagenError
from ..shell import validate_name
from .base import (
    CreateDomainSpec,
    CreateSystemSpec,
    DomainContext,
    Health,
    LoadRequest,
    Setting,
    System,
    SystemContext,
)


class SaasBase(System):
    category = "saas"
    kind = "sql"
    supports_sql = True
    domain_is_named = True  # schema domains; salesforce overrides to False

    # ---- system lifecycle (account is external: verify, don't provision) ----

    def create_system(self, spec: CreateSystemSpec) -> dict[str, Setting]:
        self._ping(self.resolve_values(spec.settings))
        return dict(spec.settings)

    def destroy_system(self, ctx: SystemContext) -> None:
        return None

    def system_status(self, ctx: SystemContext) -> Health:
        return self._reachable(ctx.meta, f"{self.slug} account")

    # ---- domain lifecycle ----

    def create_domain(
        self, ctx: SystemContext, spec: CreateDomainSpec
    ) -> dict[str, Setting]:
        settings = self.settings_from_meta(ctx.meta)
        if self.domain_is_named:
            validate_name(spec.name, label="schema")
            self.create_schema(
                self.resolve_values(settings), spec.name, force=spec.force
            )
            settings["SCHEMA"] = Setting("SCHEMA", value=spec.name)
        return settings

    def destroy_domain(self, ctx: DomainContext) -> None:
        schema = ctx.meta.get("SCHEMA")
        if schema:
            self.drop_schema(
                self.resolve_values(self.settings_from_meta(ctx.meta)), schema
            )

    def domain_status(self, ctx: DomainContext) -> Health:
        return self._reachable(
            ctx.meta, f"{self.slug} domain '{ctx.meta.get('SCHEMA', ctx.name)}'"
        )

    # ---- data plane (SQL against the schema) ----

    # Identifier quote char for exotic names: ANSI double-quote by default;
    # Databricks overrides to backtick. (Our names are bare, so it rarely fires.)
    ident_quote = '"'

    _INSERT_BATCH = 500  # rows per multi-row INSERT for the bulk load

    def install_schema(
        self, ctx: DomainContext, model: str, *, force: bool = False
    ) -> None:
        if not self.supports_sql:
            raise DatagenError(f"{self.slug} has no SQL schema to install")
        statements = self._split(self._model_ddl_text(model))
        if force:
            drops = [
                f"DROP TABLE IF EXISTS {t}" for t in self._ddl_table_names(statements)
            ]
            if drops:
                print(
                    f">>> --force: dropping {len(drops)} existing table(s) "
                    f"on {self.slug} ..."
                )
                self.exec_statements(ctx, drops)
        print(f">>> creating schema from {self.slug}.ddl on {self.slug} ...")
        self.exec_statements(ctx, statements)
        print(">>> schema ready")

    def load_data(self, ctx: DomainContext, req: LoadRequest) -> int:
        if not self.supports_sql:
            raise DatagenError(f"{self.slug} data load is a manual workflow")
        from ..emit import iter_insert_statements, read_structured

        if req.force:
            self.install_schema(ctx, req.model, force=True)
        print(f">>> loading data via the {self.slug} SQL API ...")
        total = 0
        for table, cols, rows in read_structured(req.data_dir):
            stmts = list(
                iter_insert_statements(
                    table, cols, rows, batch=self._INSERT_BATCH, quote=self.ident_quote
                )
            )
            if stmts:
                self.exec_statements(ctx, stmts)
            total += len(rows)
            print(f"    {table}: {len(rows)} row(s)")
        print(
            f">>> load complete ({total} row(s) into schema '{ctx.meta.get('SCHEMA')}')"
        )
        return total

    _CREATE_TABLE_RE = re.compile(
        r"create\s+table\s+(?:if\s+not\s+exists\s+)?[`\"]?([A-Za-z0-9_]+)", re.I
    )

    @classmethod
    def _ddl_table_names(cls, statements: list[str]) -> list[str]:
        names: list[str] = []
        for s in statements:
            m = cls._CREATE_TABLE_RE.search(s)
            if m:
                names.append(m.group(1))
        return names

    # ---- per-system hooks ----

    def _ping(self, values: dict[str, str]) -> None:
        raise NotImplementedError

    def create_schema(self, values: dict[str, str], name: str, *, force: bool) -> None:
        raise NotImplementedError

    def drop_schema(self, values: dict[str, str], name: str) -> None:
        raise NotImplementedError

    def exec_statements(self, ctx: DomainContext, statements: list[str]) -> None:
        raise NotImplementedError

    # ---- helpers ----

    def _reachable(self, meta: dict[str, str], what: str) -> Health:
        try:
            self._ping(self.resolve_values(self.settings_from_meta(meta)))
            return Health(True, f"{what} reachable")
        except DatagenError:
            raise
        except Exception as exc:  # noqa: BLE001
            return Health(False, f"cannot reach {self.slug}: {exc}")

    @staticmethod
    def _split(sql: str) -> list[str]:
        import sqlparse

        return [s for s in sqlparse.split(sql) if s.strip()]
