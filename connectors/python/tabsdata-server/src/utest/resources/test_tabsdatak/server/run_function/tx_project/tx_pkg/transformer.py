#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.api import TableFrameSpec, transformer


@transformer(input_tables=["c1/t_in"], output_tables=["t_out"])
def my_transformer(in_1: TableFrameSpec) -> TableFrameSpec:
    return in_1
