#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
from datetime import tzinfo
from typing import Any

from sqlalchemy import text

from tabsdata.connector.sql.common._generic.dialect import Dialect
from tabsdata.connector.sql.common._utils import parse_server_timezone

logger = logging.getLogger(__name__)


class Db2Dialect(Dialect):
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
