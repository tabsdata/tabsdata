#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.api import TableFrameSpec, TrxCtx, transformer


@transformer(input_tables=["c1/t_in"], output_tables=["t_out"])
def my_transformer(
    in_1: TableFrameSpec, ctx: TrxCtx
) -> TableFrameSpec:
    seen = ctx.get_attr("smoke.last-seen")
    ctx.set_attr("smoke.last-seen", "tx-after-" + str(seen))
    return in_1
