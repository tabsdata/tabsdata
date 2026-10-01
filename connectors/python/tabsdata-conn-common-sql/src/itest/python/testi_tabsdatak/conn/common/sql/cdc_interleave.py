#
# Copyright 2026 Tabsdata Inc.
#

"""Interleaved transactions for the SQL CDC connectors' integration tests.

A change stream delivers each transaction whole, in commit order, but the
stage publisher keeps only rows whose ``(seg, ord)`` is past the highest it
has already published. A connector whose ``seg`` does not grow in delivery
order therefore loses rows of a transaction that committed after another was
staged. :func:`interleave` produces that on a real source, and
:func:`publish_in_order` replays what was staged through the real publisher
to show nothing was dropped.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
from sqlalchemy import Engine, text

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql.cdc._stage_plugin import CdcStageSrcPlugin
from tabsdatak.spi import TableFileInput
from testi_tabsdatak.conn.common.sql.plugin_ctx import WatermarkSrcCtx

INTERLEAVED_IDS = list(range(1, 8))


class OpenTransaction:
    """A transaction held open on its own connection, so a test decides when
    each statement runs and when it commits."""

    def __init__(self, engine: Engine, insert: str) -> None:
        self._connection = engine.connect()
        self._transaction = self._connection.begin()
        self._insert = text(insert)

    def insert(self, row_id: int) -> None:
        self._connection.execute(self._insert, {"id": row_id})

    def commit(self) -> None:
        self._transaction.commit()
        self._connection.close()

    def close(self) -> None:
        if self._transaction.is_active:
            self._transaction.rollback()
        self._connection.close()


def interleave(
    begin: Callable[[], OpenTransaction],
    await_staged: Callable[[set[int]], None],
) -> dict[str, tuple[list[int], list[int]]]:
    """Two interleavings, one after the other, inserting rows 1 to 7; the
    rows each transaction inserted, earlier-committed first, by case.

    * ``overlapping``: T2 starts after T1 and commits after it. T1 inserts 1,
      T2 inserts 2, T1 inserts 3 and commits; T1 is staged; T2 inserts 4 and
      commits.
    * ``nested``: T4 starts after T3 and commits before it. T3 inserts 5, T4
      inserts 6 and commits; T4 is staged; T3 inserts 7 and commits.

    Each wait puts a stage between the two commits, which is what exposes a
    ``seg`` that does not grow in delivery order: the later-committed
    transaction holds a row written before the earlier one's last row."""
    opened: list[OpenTransaction] = []

    def start() -> OpenTransaction:
        transaction = begin()
        opened.append(transaction)
        return transaction

    try:
        t1 = start()
        t1.insert(1)
        t2 = start()
        t2.insert(2)
        t1.insert(3)
        t1.commit()
        await_staged({1, 3})
        t2.insert(4)
        t2.commit()
        await_staged({1, 2, 3, 4})

        t3 = start()
        t3.insert(5)
        t4 = start()
        t4.insert(6)
        t4.commit()
        await_staged({6})
        t3.insert(7)
        t3.commit()
        await_staged(set(INTERLEAVED_IDS))
    finally:
        for transaction in opened:
            transaction.close()
    return {"overlapping": ([1, 3], [2, 4]), "nested": ([6], [5, 7])}


def _ids(path: Path, id_column: str) -> set[int]:
    frame = pl.from_arrow(pq.read_table(path))
    if id_column not in frame.columns:
        return set()
    return {value for value in frame[id_column].to_list() if value is not None}


def first_stages(manifests: list[list[list[Path]]], id_column: str) -> dict[int, int]:
    """The stage each row id was first staged in."""
    stages: dict[int, int] = {}
    for stage, manifest in enumerate(manifests):
        for files in manifest:
            for path in files:
                for row_id in _ids(Path(path), id_column):
                    stages.setdefault(row_id, stage)
    return stages


def publish_in_order(
    manifests: list[list[list[Path]]], work: Path, id_column: str
) -> set[int]:
    """The row ids the stage publisher publishes when every staged manifest
    reaches it in the order it was staged, with one scratch store carried
    across them as the server carries it between runs."""
    ctx = WatermarkSrcCtx(work)
    published: set[int] = set()
    plugin = CdcStageSrcPlugin()
    for manifest in manifests:
        outputs = plugin.process_files(
            ctx,
            [[TableFileInput(file=Path(path)) for path in files] for files in manifest],
        )
        for slot in outputs:
            for output in slot:
                published |= _ids(Path(output.file), id_column)
    return published
