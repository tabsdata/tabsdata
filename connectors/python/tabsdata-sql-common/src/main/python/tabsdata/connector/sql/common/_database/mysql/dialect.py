#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from datetime import tzinfo
from typing import Any

from sqlalchemy import text

from tabsdata.connector.sql.common._generic.dialect import Dialect
from tabsdata.connector.sql.common._utils import parse_server_timezone


class MySQLDialect(Dialect):
    @property
    def open_quote(self) -> str:
        return "`"

    @property
    def close_quote(self) -> str:
        return "`"

    def default_case_fold(self, name: str) -> str:
        return name

    def server_timezone(self, engine: Any) -> tzinfo | None:
        with engine.connect() as connection:
            row = connection.execute(
                text("select @@global.time_zone, @@session.time_zone")
            ).fetchone()
        if row and row[0] is not None:
            tz_value = row[0]
            if tz_value == "SYSTEM":
                with engine.connect() as connection:
                    row = connection.execute(
                        text("select timediff(now(), utc_timestamp())")
                    ).fetchone()
                if row and row[0] is not None:
                    return parse_server_timezone(row[0])
                return None
            return parse_server_timezone(tz_value)
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
                "from information_schema.columns "
                "where table_schema = :schema "
                "and table_name = :table "
                "order by ordinal_position"
            ),
            {
                "schema": schema,
                "table": table,
            },
        ).fetchall()
        return [row[0] for row in rows]
