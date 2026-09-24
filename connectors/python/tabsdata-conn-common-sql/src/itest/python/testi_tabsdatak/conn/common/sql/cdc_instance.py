#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import time
from collections.abc import Callable

import polars as pl
from tabsdatax.itest.fixture.functions.harness import table_frame

from tabsdatak.sdk.tabsdata_server import Collection, Function

POLL_INTERVAL_SECONDS = 3
POLL_TIMEOUT_SECONDS = 180

WORKER_STOP_TIMEOUT_SECONDS = 120

_STAGE_TRIGGER_WORKER = {"ST", "StageTrigger", "stage_trigger"}
_LIVE_WORKER_STATUSES = {
    "S",
    "RR",
    "R",
    "Scheduled",
    "Requested",
    "Running",
}

OP_COLUMN_SUFFIX = "cdc.meta.op"
SEG_COLUMN_SUFFIX = "cdc.meta.seg"

INSERT, UPDATE, DELETE, LOAD, TRUNCATE = "i", "u", "d", "l", "t"

INITIAL_LOAD_SEG = -1


def column(frame: pl.DataFrame, suffix: str) -> str:
    return next(name for name in frame.columns if name.endswith(suffix))


def ops(frame: pl.DataFrame) -> list[str]:
    if not any(name.endswith(OP_COLUMN_SUFFIX) for name in frame.columns):
        return []
    return frame[column(frame, OP_COLUMN_SUFFIX)].to_list()


def ids(frame: pl.DataFrame, key: str) -> list[int]:
    if key not in frame.columns:
        return []
    return sorted(value for value in frame[key].to_list() if value is not None)


def loaded_ids(frame: pl.DataFrame, key: str) -> list[int]:
    seg = column(frame, SEG_COLUMN_SUFFIX)
    return ids(frame.filter(pl.col(seg) == INITIAL_LOAD_SEG), key)


def captured_ids(frame: pl.DataFrame, key: str) -> list[int]:
    seg = column(frame, SEG_COLUMN_SUFFIX)
    return ids(frame.filter(pl.col(seg) != INITIAL_LOAD_SEG), key)


def _live_workers(function: Function) -> set[str]:
    return {
        worker.id
        for worker in function.workers
        if worker.type in _STAGE_TRIGGER_WORKER
        and worker.status in _LIVE_WORKER_STATUSES
    }


def stop_streamer(function: Function) -> None:
    function.enable(False)
    deadline = time.time() + WORKER_STOP_TIMEOUT_SECONDS
    while time.time() < deadline:
        if not _live_workers(function):
            return
        time.sleep(POLL_INTERVAL_SECONDS)
    raise AssertionError(
        f"The streamer worker for {function.name!r} did not stop within "
        f"{WORKER_STOP_TIMEOUT_SECONDS}s"
    )


def start_streamer(function: Function) -> None:
    function.enable(True)
    deadline = time.time() + WORKER_STOP_TIMEOUT_SECONDS
    while time.time() < deadline:
        if _live_workers(function):
            return
        time.sleep(POLL_INTERVAL_SECONDS)
    raise AssertionError(
        f"The streamer worker for {function.name!r} did not start within "
        f"{WORKER_STOP_TIMEOUT_SECONDS}s"
    )


def await_published(
    collection: Collection,
    table: str,
    ready: Callable[[pl.DataFrame], bool],
) -> pl.DataFrame:
    deadline = time.time() + POLL_TIMEOUT_SECONDS
    last: pl.DataFrame | None = None
    while time.time() < deadline:
        handle = collection.get_table(table)
        if handle is not None and handle.dataversions:
            try:
                last = table_frame(collection, table)
            except (pl.exceptions.PolarsError, OSError, ValueError):
                last = None
            if last is not None and ready(last):
                return last
        time.sleep(POLL_INTERVAL_SECONDS)
    raise AssertionError(
        f"The table {table!r} did not reach the expected state within "
        f"{POLL_TIMEOUT_SECONDS}s (last seen: {last})"
    )
