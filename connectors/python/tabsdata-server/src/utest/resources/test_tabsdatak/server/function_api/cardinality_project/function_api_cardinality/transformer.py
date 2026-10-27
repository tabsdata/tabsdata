#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.api import TableFrameSpec, TableFramesSpec, transformer
from tabsdatak.tableframe.functions import concat


@transformer(
    input_tables=["c/first", "c/versions", "c/last"],
    output_tables=["merged"],
)
def merge(
    first: TableFrameSpec, versions: TableFramesSpec, last: TableFrameSpec
) -> TableFrameSpec:
    return concat([first, *versions, last], how="vertical_relaxed")
