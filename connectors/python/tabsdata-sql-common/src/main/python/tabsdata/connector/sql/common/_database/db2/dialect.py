#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from tabsdata.connector.sql.common._generic.dialect import Dialect


class Db2Dialect(Dialect):
    @property
    def open_quote(self) -> str:
        return '"'

    @property
    def close_quote(self) -> str:
        return '"'

    def default_case_fold(self, name: str) -> str:
        return name.upper()
