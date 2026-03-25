#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
from datetime import tzinfo
from typing import Any

from ibm_db_sa import BLOB, CLOB
from sqlalchemy import text
from sqlalchemy.types import TypeEngine

from tabsdata.connector.sql.common._generic.dialect import LARGE_COLUMN_TYPES, Dialect
from tabsdata.connector.sql.common._utils import parse_server_timezone

DB2_LARGE_COLUMN_TYPES = LARGE_COLUMN_TYPES + (
    BLOB,
    CLOB,
)

logger = logging.getLogger(__name__)


class Db2Dialect(Dialect):
    @property
    def sqlglot_dialect(self) -> str | None:
        return None

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
            row = connection.execute(
                text("select current timezone from sysibm.sysdummy1")
            ).fetchone()
        if row and row[0] is not None:
            tz = parse_server_timezone(row[0])
            if tz is not None:
                return tz
        return None

    def is_large_column(self, satype: Any) -> bool:
        return isinstance(satype, DB2_LARGE_COLUMN_TYPES)

    def cdc_to_sa(self, ingester_type: str, cdc_dtype: Any) -> TypeEngine:
        match ingester_type:
            case "sqlreplica":
                return cdc_dtype
            case _:
                raise NotImplementedError(
                    f"{type(self).__name__}.cdc_to_sa is not implemented "
                    f"for ingester type {ingester_type!r}"
                )

    def get_catalog_columns(
        self,
        connection: Any,
        schema: str,
        table: str,
    ) -> list[str]:
        rows = connection.execute(
            text(
                "select colname "
                "from syscat.columns "
                "where tabschema = :schema "
                "and tabname = :table "
                "order by colno"
            ),
            {
                "schema": schema,
                "table": table,
            },
        ).fetchall()
        return [row[0].strip() for row in rows]
