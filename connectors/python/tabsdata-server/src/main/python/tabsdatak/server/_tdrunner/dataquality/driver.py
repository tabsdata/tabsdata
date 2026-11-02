#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import dataclass

from tabsdatak.common._actions.dataquality.processor import DataQualityProcessor
from tabsdatak.common._actions.dataquality.record import Check
from tabsdatak.dataquality import DataQuality
from tabsdatak.dataquality._symbols import TableType
from tabsdatak.tableframe import TableFrame


@dataclass(frozen=True)
class DerivedTable:
    name: str
    frame: TableFrame
    table_type: TableType


@dataclass(frozen=True)
class DataQualityResult:
    output_frames: list[TableFrame | None]
    derived: list[DerivedTable]
    checks: list[Check]


def has_data_quality(on_tables) -> bool:
    return any(isinstance(action, DataQuality) for action in on_tables or [])


def run_data_quality(
    *,
    collection: str,
    output_tables: list[str],
    output_frames: list[TableFrame | None],
    on_tables,
) -> DataQualityResult:
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
