#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from datetime import tzinfo
from typing import Any

import sqlalchemy.types as sa_types
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import BYTEA

from tabsdata.connector.sql.common._generic.dialect import LARGE_COLUMN_TYPES, Dialect
from tabsdata.connector.sql.common._utils import parse_server_timezone

POSTGRES_LARGE_COLUMN_TYPES = LARGE_COLUMN_TYPES + (BYTEA,)


class PostgresDialect(Dialect):
    @property
    def sqlglot_dialect(self) -> str | None:
        return "postgres"

    @property
    def open_quote(self) -> str:
        return '"'

    @property
    def close_quote(self) -> str:
        return '"'

    def default_case_fold(self, name: str) -> str:
        return name.lower()

    def server_timezone(self, engine: Any) -> tzinfo | None:
        with engine.connect() as connection:
            row = connection.execute(text("show timezone")).fetchone()
        if row and row[0] is not None:
            return parse_server_timezone(str(row[0]))
        return None

    def is_large_column(self, satype: Any) -> bool:
        return isinstance(satype, POSTGRES_LARGE_COLUMN_TYPES)

    def get_catalog_columns(
        self,
        connection: Any,
        schema: str,
        table: str,
    ) -> list[str]:
        rows = connection.execute(
            text(
                "select a.attname "
                "from pg_catalog.pg_attribute a "
                "join pg_catalog.pg_class c "
                "on a.attrelid = c.oid "
                "join pg_catalog.pg_namespace n "
                "on c.relnamespace = n.oid "
                "where n.nspname = :schema "
                "and c.relname = :table "
                "and a.attnum > 0 "
                "and not a.attisdropped "
                "order by a.attnum"
            ),
            {
                "schema": schema,
                "table": table,
            },
        ).fetchall()
        return [row[0] for row in rows]

    def cdc_to_sa(self, ingester_type: str, cdc_dtype: Any) -> sa_types.TypeEngine:
        match ingester_type:
            case "wal2json":
                return self.cdc_to_sa_wal2json(cdc_dtype)
            case _:
                raise NotImplementedError(
                    f"{type(self).__name__}.cdc_to_sa is not implemented "
                    f"for ingester type {ingester_type!r}"
                )

    def cdc_to_sa_wal2json(self, cdc_dtype: str) -> sa_types.TypeEngine:
        return self.ddl_to_sa(cdc_dtype)
