#
# Copyright 2026 Tabsdata Inc.
#

"""Edition feature installation (OS: system-column guards + write progress).

``install(frame_cls)`` registers the aspect hooks this edition supports onto the
generated frame class. The generated ``_load_registry`` calls it once, on first
use, through the stable ``tabsdatak.extensions.tableframe._engine.features``
path -- whichever edition's te_tableframe is installed decides what gets
registered. The OS edition installs system-column guards, join system-column
reassembly, and sink write progress.
"""

from __future__ import annotations


def install(frame_cls) -> None:
    from tabsdatak.extensions.tableframe._engine.aspect import columns, progress
    from tabsdatak.tableframe._core.hooks import set_hook, set_sink_observer

    for op in ("select", "with_columns", "drop", "rename", "udf"):
        set_hook(frame_cls, op, columns.guard, phase="before")

    set_hook(frame_cls, "join", _join_hook_after, phase="after")

    set_sink_observer(frame_cls, progress.observe)


def aggregations(op) -> list:
    """Expressions aggregating system columns within a group-by ``op``: a group
    carries the latest version among its rows."""
    from tabsdatak.tableframe._engine.column.versions import latest_version

    return latest_version()


def _join_hook_after(lazyframe, op, views, params):
    # Reassemble the system columns after a join. A join brings a $td.x column
    # from each side, so polars keeps one as $td.x and suffixes the other as
    # $td.x_right. For each such system column: if it is mergeable, both copies
    # are fused into a single $td.x carrying their combined contents; if not, the
    # extra copies are dropped and the column is regenerated downstream if
    # necessary. compute() owns the _build(TAB, source=primary) that follows, so
    # this hook only shapes the lazyframe.
    from tabsdatak.extensions.tableframe._engine.column.assembly import (
        assemble_system_columns,
    )
    from tabsdatak.tableframe._engine.constant.column.prefixes import (
        TD_CONNECTOR_SYSTEM_COLUMN_PREFIX,
    )

    lazyframe = assemble_system_columns(lazyframe)
    # A connector column is single-valued metadata, like the version: where both
    # sides carry one the left side's is kept, and the suffixed copy polars
    # makes of the right side's -- a name neither input had -- is dropped.
    known = {column for view in views for column in view.columns}
    twins = [
        column
        for column in lazyframe.collect_schema().names()
        if column.startswith(TD_CONNECTOR_SYSTEM_COLUMN_PREFIX) and column not in known
    ]
    return lazyframe.drop(twins) if twins else lazyframe
