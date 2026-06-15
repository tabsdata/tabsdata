#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from tabsdatak.tableframe._core.hooks import set_hook
from tabsdatak.tableframe._engine.frame.frame import EngineFrame


def join_hook(frame, sources, params):
    from tabsdatak.extensions.tableframe._engine.column.assembly import (
        assemble_system_columns,
    )
    from tabsdatak.tableframe._engine.taxonomy.frame import CreationMode

    left = sources[0]
    lf = assemble_system_columns(frame._lf)
    genesis = left._state.genesis if left._state is not None else None
    return type(frame)._build(
        lf,
        CreationMode.TAB,
        idx=genesis.idx if genesis is not None else None,
        source=left,
    )


set_hook(EngineFrame, "join", join_hook)
