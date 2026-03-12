#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from datetime import tzinfo
from typing import Any

from sqlalchemy import text

from tabsdata.connector.sql.common._generic.dialect import Dialect
from tabsdata.connector.sql.common._utils import parse_server_timezone


class PostgresDialect(Dialect):
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
