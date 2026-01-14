#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

from typing import TYPE_CHECKING, TypeAlias

if TYPE_CHECKING:
    from tabsdata.tableframe.lazyframe.frame import TableFrame

from tabsdata.tableframe._actions.dataquality.pojo import DerivedTableFrame

InputTableSpec: TypeAlias = list["TableFrame | None | list[TableFrame | None]"]
OutputTableSpec: TypeAlias = list["TableFrame | None"]

DerivedTableFrameSpec: TypeAlias = list[DerivedTableFrame | None]
