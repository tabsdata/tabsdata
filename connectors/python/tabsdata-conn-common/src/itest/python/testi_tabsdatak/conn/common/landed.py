#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import uuid
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import polars as pl

from tabsdatak.common._storage.failures import collected
from tabsdatak.common._storage.landed import StoredMetadata, serve
from tabsdatak.spi import FrameMetadata, LazyFrameInput


@dataclass(frozen=True)
class Served:
    frame: pl.DataFrame
    metadata: dict[str, Any] = field(default_factory=dict)


def served(table: LazyFrameInput, folder: Path) -> Served:
    folder.mkdir(parents=True, exist_ok=True)
    target = folder / f"{uuid.uuid4().hex}.parquet"
    collected(table.frame.sink_parquet(target, lazy=True))
    metadata = table.metadata() if callable(table.metadata) else table.metadata
    stored = StoredMetadata.read(StoredMetadata.of(metadata or FrameMetadata()))
    frame = serve(pl.scan_parquet(target), stored).collect()
    return Served(frame, dict(stored.columns or {}))


def taken(slots: list[Iterable[LazyFrameInput]], folder: Path) -> list[list[Served]]:
    return [[served(table, folder) for table in slot] for slot in slots]


def empty(slot: list[Served]) -> pl.Schema | None:
    """The columns of what a source hands over when a run finds no new data:
    one empty table. `None` when the slot holds anything else."""
    if len(slot) == 1 and slot[0].frame.height == 0:
        return slot[0].frame.schema
    return None
