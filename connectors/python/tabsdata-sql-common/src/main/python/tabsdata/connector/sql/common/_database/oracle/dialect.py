#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from datetime import tzinfo
from typing import Any

from sqlalchemy import text

from tabsdata.connector.sql.common._generic.dialect import Dialect
from tabsdata.connector.sql.common._utils import parse_server_timezone


class OracleDialect(Dialect):
    @property
    def open_quote(self) -> str:
        return '"'

    @property
    def close_quote(self) -> str:
        return '"'

    def default_case_fold(self, name: str) -> str:
        return name.upper()

    def server_timezone(self, engine: Any) -> tzinfo | None:
        with engine.connect() as connection:
            row = connection.execute(text("select dbtimezone from dual")).fetchone()
        if row and row[0] is not None:
            return parse_server_timezone(row[0])
        return None

    def get_catalog_columns(
        self,
        connection: Any,
        schema: str,
        table: str,
    ) -> list[str]:
        rows = connection.execute(
            text(
                "select column_name "
                "from all_tab_columns "
                "where owner = :schema "
                "and table_name = :table "
                "order by column_id"
            ),
            {
                "schema": schema,
                "table": table,
            },
        ).fetchall()
        return [row[0] for row in rows]
