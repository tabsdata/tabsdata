#
# Copyright 2026 Tabs Data Inc.
#

"""Edition feature installation (OS: system-column guards + write progress).

``install(frame_cls)`` registers the aspect hooks this edition supports onto the
generated frame class. The generated ``_load_registry`` calls it once, on first
use, through the stable ``tabsdatak.extensions.tableframe._engine.features``
path -- whichever edition's te_tableframe is installed decides what gets
registered. The OS edition installs system-column guards and sink write progress.
"""

from __future__ import annotations


def install(frame_cls) -> None:
    from tabsdatak.extensions.tableframe._engine.aspect import columns, progress
    from tabsdatak.tableframe._core.hooks import set_hook, set_sink_observer

    for op in ("select", "with_columns", "drop", "rename"):
        set_hook(frame_cls, op, columns.guard, phase="before")

    set_sink_observer(frame_cls, progress.observe)
