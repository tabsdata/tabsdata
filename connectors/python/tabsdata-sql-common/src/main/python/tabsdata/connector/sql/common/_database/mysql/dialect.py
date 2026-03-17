#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from datetime import tzinfo
from typing import Any

from sqlalchemy import text
from sqlalchemy.dialects.mysql import (
    LONGBLOB,
    LONGTEXT,
    MEDIUMBLOB,
    MEDIUMTEXT,
    TINYBLOB,
)
from sqlalchemy.types import Text

from tabsdata.connector.sql.common._generic.dialect import LARGE_COLUMN_TYPES, Dialect
from tabsdata.connector.sql.common._utils import parse_server_timezone

MYSQL_LARGE_COLUMN_TYPES = LARGE_COLUMN_TYPES + (
    Text,
    LONGBLOB,
    LONGTEXT,
    MEDIUMBLOB,
    MEDIUMTEXT,
    TINYBLOB,
)


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

    def is_large_column(self, satype: Any) -> bool:
        return isinstance(satype, MYSQL_LARGE_COLUMN_TYPES)

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
