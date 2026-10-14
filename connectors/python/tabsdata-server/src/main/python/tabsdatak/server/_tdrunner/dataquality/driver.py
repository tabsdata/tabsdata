#
# Copyright 2026 Tabsdata Inc.
#

"""Data-quality driver for the run_function command.

Runs the data-quality actions a function declares in ``on_tables`` against its
output tables. Data-quality actions act on the function's output tables: an
in-place action (e.g. filter) rewrites the output frame; a producing action
(e.g. enrich, select, summary) yields an additional derived table. The driver
returns the (possibly rewritten) output frames plus the derived tables, each
carrying the table name and the ``TableType`` the catalog registers it under.
"""

from __future__ import annotations

from dataclasses import dataclass

from tabsdatak.common._actions.dataquality.processor import DataQualityProcessor
from tabsdatak.common._actions.dataquality.record import Check
from tabsdatak.dataquality import DataQuality
from tabsdatak.dataquality._symbols import TableType
from tabsdatak.tableframe import TableFrame


@dataclass(frozen=True)
class DerivedTable:
    """A data-quality derived output table and the type it registers under."""

    name: str
    frame: TableFrame
    table_type: TableType


@dataclass(frozen=True)
class DataQualityResult:
    """Outcome of running data quality over a function's outputs.

    ``output_frames`` mirrors the input output-frame list with in-place actions
    applied. ``derived`` holds the extra tables producing actions yielded.
    ``checks`` holds the verdicts of Fail operators, to settle when the tables
    are written.
    """

    output_frames: list[TableFrame | None]
    derived: list[DerivedTable]
    checks: list[Check]


def has_data_quality(on_tables) -> bool:
    """Whether ``on_tables`` declares any data-quality action."""
    return any(isinstance(action, DataQuality) for action in on_tables or [])


def run_data_quality(
    *,
    collection: str,
    output_tables: list[str],
    output_frames: list[TableFrame | None],
    on_tables,
) -> DataQualityResult:
    """Run the function's data-quality actions over its output tables.

    Returns the rewritten output frames and the derived tables. Assumes
    ``has_data_quality(on_tables)`` is true; the output-frame list is positional,
    matching ``output_tables``.
    """
    actions = [action for action in on_tables or [] if isinstance(action, DataQuality)]
    processor = DataQualityProcessor.from_actions(
        collection=collection,
        output_tables=output_tables,
        actions=actions,
    )

    _, output_table_infos = processor.table_names()

    frames = list(output_frames)
    derived_frames: list = []
    checks: list[Check] = []
    processor.tables_data([], frames, derived_frames, checks)

    derived = [
        DerivedTable(
            name=info.table,
            frame=record.derived_tf,
            table_type=info.kind.type,
        )
        for info, record in zip(output_table_infos, derived_frames)
    ]
    return DataQualityResult(output_frames=frames, derived=derived, checks=checks)
