#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from datetime import tzinfo
from typing import Any

from sqlalchemy import text

from tabsdata.connector.sql.common._generic.dialect import Dialect
from tabsdata.connector.sql.common._utils import parse_server_timezone


class MSSQLDialect(Dialect):
    @property
    def open_quote(self) -> str:
        return "["

    @property
    def close_quote(self) -> str:
        return "]"

    def default_case_fold(self, name: str) -> str:
        return name.lower()

    def server_timezone(self, engine: Any) -> tzinfo | None:
        with engine.connect() as connection:
            row = connection.execute(text("select current_timezone_id()")).fetchone()
        if row and row[0] is not None:
            return parse_server_timezone(row[0])
        return None
