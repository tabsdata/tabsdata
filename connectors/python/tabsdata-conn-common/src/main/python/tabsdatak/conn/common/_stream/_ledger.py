#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from collections.abc import Iterable

import polars as pl
import pyarrow as pa

from tabsdatak.spi import (
    FrameMetadata,
    LazyFrameInput,
    StageFileStats,
    StreamSrcPluginCtx,
    StreamStateSpec,
)

logger = logging.getLogger(__name__)


class StageLedger:
    def __init__(self, *, ctx: StreamSrcPluginCtx, slots: Iterable[str]):
        self._ctx = ctx
        self._positions = {slot: position for position, slot in enumerate(slots)}
        self._staged: list[list[StageFileStats]] = [[] for _ in self._positions]
        self._rows: int = 0
        self._bytes: int = 0

    @property
    def rows(self) -> int:
        return self._rows

    @property
    def bytes(self) -> int:
        return self._bytes

    @property
    def has_data(self) -> bool:
        return self._rows > 0

    @property
    def has_files(self) -> bool:
        return any(self._staged)

    def unstaged(self) -> list[str]:
        return [
            slot
            for slot, position in self._positions.items()
            if not self._staged[position]
        ]

    def write(self, slot: str, table: pa.Table) -> None:
        self.stage(slot, pl.DataFrame(table).lazy())

    def stage(
        self,
        slot: str,
        frame: pl.LazyFrame,
        metadata: FrameMetadata | None = None,
    ) -> None:
        position = self._positions[slot]
        tables: list[list[LazyFrameInput] | None] = [None] * len(self._positions)
        tables[position] = [LazyFrameInput(frame, metadata)]
        for stats in self._ctx.stage(tables)[position]:
            self._staged[position].append(stats)
            self._rows += stats.rows
            self._bytes += stats.bytes
            logger.debug(
                f"Staged {stats.rows} row(s) and {stats.bytes} byte(s) of {slot} "
                f"as {stats.name}"
            )

    def manifest(self) -> list[list[StageFileStats]]:
        return [list(files) for files in self._staged]

    def publish(self, new_state: StreamStateSpec) -> None:
        self._ctx.stage(None, new_state)
        self.clear()

    def clear(self) -> None:
        for files in self._staged:
            files.clear()
        self._rows = 0
        self._bytes = 0

    def discard(self) -> None:
        self._ctx.unstage([stats for files in self._staged for stats in files])
        self.clear()
