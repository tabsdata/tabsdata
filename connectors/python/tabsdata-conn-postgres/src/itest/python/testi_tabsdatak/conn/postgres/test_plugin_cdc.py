#
# Copyright 2026 Tabsdata Inc.
#

"""Postgres CDC stream-source plugin integration tests using fake context"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow.parquet as pq
import pytest
from sqlalchemy import create_engine
from tabsdatax.itest.fixture.backends.postgrescdc import (
    PostgrescdcDomain,
    postgrescdc_domain,
)
from testi_tabsdatak.conn.common.sql.cdc_interleave import (
    INTERLEAVED_IDS,
    OpenTransaction,
    first_stages,
    interleave,
    publish_in_order,
)
from testi_tabsdatak.conn.common.sql.plugin_io import work_dir
from testi_tabsdatak.conn.postgres.backend import (
    cdc_probe,
    cdc_src_conn,
    cdc_table,
    replication_slot,
)

from tabsdatak.conn.common.sql.cdc._ingester import CDC_OFFSET_STATE_KEY
from tabsdatak.conn.postgres import PostgresCdcStreamSrc, PostgresCdcStreamSrcConn

# noinspection PyProtectedMember
from tabsdatak.conn.postgres._cdc_impl import LsnOffset

# noinspection PyProtectedMember
from tabsdatak.conn.postgres._cdc_plugin import PostgresCdcStreamSrcPlugin
from tabsdatak.spi import StreamSrcPluginCtx

pytestmark = [
    pytest.mark.postgres,
    pytest.mark.xdist_group("postgrescdc_seq"),
]

CAPTURE_TIMEOUT_SECONDS = 90.0
PLUGIN_EXIT_TIMEOUT_SECONDS = 60.0
POLL_SECONDS = 0.25

# One row per staged batch, so a captured change reaches `ctx.stage` within a poll
# interval instead of waiting for the run loop to end.
CDC_CFG = {
    "tabsdata.cdc.buffer.max_rows": 1,
    "tabsdata.cdc.buffer.max_secs": 1,
    "tabsdata.cdc.trigger.max_rows": 1,
    "tabsdata.cdc.poll_interval_seconds": 1,
    "postgres.cdc.blocking_timeout_seconds": 2,
}

OP_COLUMN_SUFFIX = "cdc.meta.op"
INSERT, UPDATE, DELETE = "i", "u", "d"


class _CdcCtx(StreamSrcPluginCtx):
    """``active`` holds the runner's poll loop open until the test ends it: capture is
    asynchronous, so a test waits for the batches it expects and then stops the loop,
    rather than budgeting a fixed number of polls."""

    def __init__(self, work: Path) -> None:
        self._work_dir = work
        self._stop = threading.Event()
        self._state: dict[str, Any] = {}
        self._tabsdata_owns_state = False
        self._staged: list[list[list[Path]]] = []
        self._lock = threading.Lock()

    @property
    def work_dir(self) -> Path:
        return self._work_dir

    @property
    def exec_plan_id(self) -> str:
        return "postgrescdc-integ-exec-plan"

    @property
    def trx_id(self) -> str:
        return "postgrescdc-integ-trx"

    @property
    def exec_plan_ts(self) -> str:
        return "2026-01-01T00:00:00.000000+00:00"

    @property
    def collection_id(self) -> str:
        return "test-collection-id"

    @property
    def collection_name(self) -> str:
        return "test-collection"

    @property
    def function_id(self) -> str:
        return "test-function-id"

    @property
    def function_name(self) -> str:
        return "test-function"

    def get_attr(self, attr):
        return None

    def set_attr(self, attr, val):
        pass

    def add_metadata(self, attr, val):
        pass

    @property
    def active(self) -> bool:
        return not self._stop.is_set()

    @property
    def first_stage(self) -> bool:
        with self._lock:
            return not self._staged

    @property
    def stream_state(self) -> dict | None:
        with self._lock:
            if not self._tabsdata_owns_state:
                return None
            return dict(self._state) if self._state else None

    @property
    def flush_interval_mins(self) -> int:
        return 0

    def stage(self, tables_files, new_state):
        with self._lock:
            self._staged.append([list(files) for files in tables_files])
            if isinstance(new_state, dict):
                self._state = dict(new_state)
                self._tabsdata_owns_state = True
            else:
                self._tabsdata_owns_state = False
                new_state()

    def checkpoint(self, new_state):
        with self._lock:
            self._state = dict(new_state)
            self._tabsdata_owns_state = True

    def stop(self) -> None:
        self._stop.set()

    def manifests(self) -> list[list[list[Path]]]:
        with self._lock:
            return list(self._staged)


def _stream_src(
    tables: list[str], slot: str, start_from: str, **cfg
) -> PostgresCdcStreamSrc:
    return PostgresCdcStreamSrc(
        tables=tables,
        replication_slot=slot,
        replication_slot_behavior="reuse",
        start_from=start_from,
        src_cfg=CDC_CFG | cfg,
    )


@contextmanager
def _running(
    ctx: _CdcCtx, conn: PostgresCdcStreamSrcConn, src: PostgresCdcStreamSrc
) -> Iterator[None]:
    """``run`` is an endless decode-the-WAL loop, so it is driven on a worker thread
    while the test asserts (and produces) on the main one; leaving the block ends the
    loop through ``ctx.active`` and re-raises whatever the plugin failed with."""
    err: list[BaseException] = []

    def _go():
        try:
            PostgresCdcStreamSrcPlugin().run(ctx=ctx, conn=conn, stream_src=src)
        except BaseException as exc:  # noqa: BLE001
            err.append(exc)
            ctx.stop()

    thread = threading.Thread(target=_go, name="postgrescdc-plugin", daemon=True)
    thread.start()
    try:
        yield
    finally:
        ctx.stop()
        thread.join(timeout=PLUGIN_EXIT_TIMEOUT_SECONDS)
        if err:
            raise err[0]
        if thread.is_alive():
            pytest.fail(
                f"plugin run did not exit within {PLUGIN_EXIT_TIMEOUT_SECONDS}s"
            )


def _captured(ctx: _CdcCtx, slot: int | None = None) -> pl.DataFrame | None:
    """Every change staged so far, or those of one output slot; ``None`` while nothing
    has been staged. Rows carrying a before-image have columns the insert-only ones do
    not, hence the diagonal concat."""
    files = [
        file
        for manifest in ctx.manifests()
        for index, table_files in enumerate(manifest)
        if slot is None or slot == index
        for file in table_files
    ]
    if not files:
        return None
    return pl.concat(
        [pl.from_arrow(pq.read_table(file)) for file in files], how="diagonal_relaxed"
    )


def _await_captured(
    ctx: _CdcCtx, expected: str, ready: Callable[[pl.DataFrame], bool]
) -> pl.DataFrame:
    """Poll until the staged changes satisfy `ready`. Capture latency is the WAL decode
    round trip plus a poll interval, so the wait is a deadline rather than a sleep; a
    timeout fails the test (an uncaptured change is the defect these tests look for)."""
    deadline = time.monotonic() + CAPTURE_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        frame = _captured(ctx)
        if frame is not None and ready(frame):
            return frame
        time.sleep(POLL_SECONDS)
    pytest.fail(
        f"CDC did not capture {expected} within {CAPTURE_TIMEOUT_SECONDS}s "
        f"(staged: {_captured(ctx)})"
    )


def _ops(frame: pl.DataFrame) -> set[str]:
    column = next(name for name in frame.columns if name.endswith(OP_COLUMN_SUFFIX))
    return set(frame[column].to_list())


def _ids(frame: pl.DataFrame) -> list[int]:
    return sorted(frame["id"].to_list())


@postgrescdc_domain("postgrescdc_employees_xs")
def test_cdc_plugin_stages_captured_inserts(
    domain_postgrescdc_employees_xs: PostgrescdcDomain, tmp_path: Path
):
    domain = domain_postgrescdc_employees_xs
    with cdc_probe(domain, "td_cdc_inserts") as probe:
        with replication_slot(domain, "td_cdc_inserts_slot") as slot:
            src = _stream_src([cdc_table(probe.name)], slot, "head")
            probe.insert(1, "alice")
            probe.insert(2, "bob")

            ctx = _CdcCtx(work_dir(tmp_path))
            with _running(ctx, cdc_src_conn(domain), src):
                frame = _await_captured(
                    ctx, "both inserts", lambda f: len(_ids(f)) >= 2
                )

    # `columns_format="columns"` + `flatten_columns=True` inline the new values, so the
    # probe's own columns show up at the top level.
    assert set(frame.columns) >= {"id", "name"}
    assert _ids(frame) == [1, 2]
    assert frame.sort("id")["name"].to_list() == ["alice", "bob"]
    assert _ops(frame) == {INSERT}


@postgrescdc_domain("postgrescdc_employees_xs")
def test_cdc_plugin_captures_update_and_delete(
    domain_postgrescdc_employees_xs: PostgrescdcDomain, tmp_path: Path
):
    domain = domain_postgrescdc_employees_xs
    with cdc_probe(domain, "td_cdc_ops") as probe:
        with replication_slot(domain, "td_cdc_ops_slot") as slot:
            src = _stream_src([cdc_table(probe.name)], slot, "head")
            probe.insert(7, "carol")
            probe.update(7, "carol-2")
            probe.delete(7)

            ctx = _CdcCtx(work_dir(tmp_path))
            with _running(ctx, cdc_src_conn(domain), src):
                frame = _await_captured(
                    ctx,
                    "insert, update and delete",
                    lambda f: {INSERT, UPDATE, DELETE} <= _ops(f),
                )

    assert _ops(frame) == {INSERT, UPDATE, DELETE}
    assert set(frame["name"].to_list()) >= {"carol", "carol-2"}


@postgrescdc_domain("postgrescdc_employees_xs")
def test_cdc_plugin_tail_skips_changes_committed_before_it_attached(
    domain_postgrescdc_employees_xs: PostgrescdcDomain, tmp_path: Path
):
    domain = domain_postgrescdc_employees_xs
    with cdc_probe(domain, "td_cdc_tail") as probe:
        with replication_slot(domain, "td_cdc_tail_slot") as slot:
            probe.insert(100, "before-1")
            probe.insert(101, "before-2")

            src = _stream_src([cdc_table(probe.name)], slot, "tail")
            ctx = _CdcCtx(work_dir(tmp_path))
            with _running(ctx, cdc_src_conn(domain), src):
                # "tail" attaches at the current WAL position, which the test cannot
                # observe, so it keeps producing rows until one is captured instead of
                # waiting a fixed time for the stream to be up.
                produced = 200
                deadline = time.monotonic() + CAPTURE_TIMEOUT_SECONDS
                while _captured(ctx) is None and time.monotonic() < deadline:
                    probe.insert(produced, f"after-{produced}")
                    produced += 1
                    time.sleep(POLL_SECONDS)
                frame = _await_captured(
                    ctx, "a row inserted after attach", lambda _frame: True
                )

    ids = _ids(frame)
    assert 100 not in ids
    assert 101 not in ids
    assert min(ids) >= 200


@postgrescdc_domain("postgrescdc_employees_xs")
def test_cdc_plugin_persists_the_lsn_in_stream_state(
    domain_postgrescdc_employees_xs: PostgrescdcDomain, tmp_path: Path
):
    """Tabsdata owns the resume offset: each staged batch persists the LSN in the
    stream state, like mysql / db2 persist theirs; the slot is only acknowledged."""
    domain = domain_postgrescdc_employees_xs
    with cdc_probe(domain, "td_cdc_offset") as probe:
        with replication_slot(domain, "td_cdc_offset_slot") as slot:
            src = _stream_src([cdc_table(probe.name)], slot, "head")
            probe.insert(1, "alice")

            ctx = _CdcCtx(work_dir(tmp_path))
            with _running(ctx, cdc_src_conn(domain), src):
                _await_captured(ctx, "the insert", lambda f: len(_ids(f)) >= 1)

    state = ctx.stream_state
    assert state is not None
    assert LsnOffset.read(state[CDC_OFFSET_STATE_KEY]).lsn > 0


@postgrescdc_domain("postgrescdc_employees_xs")
def test_cdc_plugin_captures_with_wal2json_v1(
    domain_postgrescdc_employees_xs: PostgrescdcDomain, tmp_path: Path
):
    domain = domain_postgrescdc_employees_xs
    with cdc_probe(domain, "td_cdc_v1") as probe:
        with replication_slot(domain, "td_cdc_v1_slot") as slot:
            src = _stream_src(
                [cdc_table(probe.name)],
                slot,
                "head",
                **{"postgres.cdc.wal2json_format_version": 1},
            )
            probe.insert(9, "v1")

            ctx = _CdcCtx(work_dir(tmp_path))
            with _running(ctx, cdc_src_conn(domain), src):
                frame = _await_captured(ctx, "the insert", lambda f: 9 in _ids(f))

    assert frame["name"].to_list() == ["v1"]


@postgrescdc_domain("postgrescdc_employees_xs")
def test_cdc_plugin_captures_every_tracked_table(
    domain_postgrescdc_employees_xs: PostgrescdcDomain, tmp_path: Path
):
    domain = domain_postgrescdc_employees_xs
    with cdc_probe(domain, "td_cdc_first") as first:
        with cdc_probe(domain, "td_cdc_second") as second:
            with replication_slot(domain, "td_cdc_tables_slot") as slot:
                src = _stream_src(
                    [cdc_table(first.name), cdc_table(second.name)], slot, "head"
                )
                first.insert(1, "first-table")
                second.insert(2, "second-table")

                ctx = _CdcCtx(work_dir(tmp_path))
                with _running(ctx, cdc_src_conn(domain), src):
                    _await_captured(
                        ctx,
                        "a change from each tracked table",
                        lambda _frame: (
                            _captured(ctx, slot=0) is not None
                            and _captured(ctx, slot=1) is not None
                        ),
                    )

    # Each tracked table owns an output slot, positionally by `tables`.
    assert _ids(_captured(ctx, slot=0)) == [1]
    assert _ids(_captured(ctx, slot=1)) == [2]


@postgrescdc_domain("postgrescdc_employees_xs")
def test_cdc_plugin_publishes_every_row_of_interleaved_transactions(
    domain_postgrescdc_employees_xs: PostgrescdcDomain, tmp_path: Path
):
    """Transactions that overlap, and one nested inside another, each with a
    stage between the two commits. The later-committed transaction holds a row
    written in the WAL before the earlier one's last: positioned by where rows
    were written, that row would sort below the stage cursor and be dropped;
    positioned by the commit, every row is published."""
    domain = domain_postgrescdc_employees_xs
    with cdc_probe(domain, "td_cdc_interleaved") as probe:
        with replication_slot(domain, "td_cdc_interleaved_slot") as slot:
            src = _stream_src([cdc_table(probe.name)], slot, "head")
            engine = create_engine(domain.sqlalchemy_uri)
            insert = (
                f"INSERT INTO \"{probe.name}\" (id, name) VALUES (:id, 'interleaved')"
            )
            ctx = _CdcCtx(work_dir(tmp_path))
            try:
                with _running(ctx, cdc_src_conn(domain), src):
                    cases = interleave(
                        lambda: OpenTransaction(engine, insert),
                        lambda ids: _await_captured(
                            ctx, f"rows {sorted(ids)}", lambda f: ids <= set(_ids(f))
                        ),
                    )
            finally:
                engine.dispose()

    stages = first_stages(ctx.manifests(), "id")
    for case, (earlier, later) in cases.items():
        assert max(stages[i] for i in earlier) < min(stages[i] for i in later), case
    published = publish_in_order(ctx.manifests(), work_dir(tmp_path, "publish"), "id")
    assert published == set(INTERLEAVED_IDS)
