#
# Copyright 2026 Tabsdata Inc.
#

"""MySQL CDC stream-source plugin integration tests using fake context"""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow.parquet as pq
import pytest
from tabsdatax.itest.fixture.backends.mysqlcdc import MysqlcdcDomain, mysqlcdc_domain
from testi_tabsdatak.conn.common.sql.plugin_io import work_dir
from testi_tabsdatak.conn.mysql.backend import (
    cdc_anchor,
    cdc_probe,
    cdc_src_conn,
    cdc_table,
)

from tabsdatak.conn.mysql import MySQLCdcStreamSrc, MySQLCdcStreamSrcConn

# noinspection PyProtectedMember
from tabsdatak.conn.mysql._cdc_plugin import MySQLCdcStreamSrcPlugin
from tabsdatak.spi import StreamSrcPluginCtx

pytestmark = [pytest.mark.mysql, pytest.mark.xdist_group("mysqlcdc_seq")]

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
    "mysql.cdc.blocking_timeout_seconds": 2,
}

OP_COLUMN_SUFFIX = "cdc.meta.op"
INSERT, UPDATE, DELETE = "i", "u", "d"


class _CdcCtx(StreamSrcPluginCtx):
    """``active`` holds the runner's poll loop open until the test ends it: capture is
    asynchronous, so a test waits for the batches it expects and then stops the loop,
    rather than budgeting a fixed number of polls. A seeded ``initial_state`` stands in
    for a prior run's persisted offset -- this is then no longer the first stage, so the
    ingester resumes past it instead of replaying from ``start_from``."""

    def __init__(self, work: Path, *, initial_state: dict | None = None) -> None:
        self._work_dir = work
        self._stop = threading.Event()
        self._state: dict[str, Any] = dict(initial_state) if initial_state else {}
        self._tabsdata_owns_state = initial_state is not None
        self._committed_before = initial_state is not None
        self._staged: list[list[list[Path]]] = []
        self._lock = threading.Lock()

    @property
    def work_dir(self) -> Path:
        return self._work_dir

    @property
    def exec_plan_id(self) -> str:
        return "mysqlcdc-integ-exec-plan"

    @property
    def trx_id(self) -> str:
        return "mysqlcdc-integ-trx"

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
            return not self._committed_before and not self._staged

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

    def stop(self) -> None:
        self._stop.set()

    def manifests(self) -> list[list[list[Path]]]:
        with self._lock:
            return list(self._staged)


def _stream_src(tables: list[str], start_from, **cfg) -> MySQLCdcStreamSrc:
    return MySQLCdcStreamSrc(
        tables=tables, start_from=start_from, src_cfg=CDC_CFG | cfg
    )


@contextmanager
def _running(
    ctx: _CdcCtx, conn: MySQLCdcStreamSrcConn, src: MySQLCdcStreamSrc
) -> Iterator[None]:
    """``run`` is an endless read-the-binlog loop, so it is driven on a worker thread
    while the test asserts (and produces) on the main one; leaving the block ends the
    loop through ``ctx.active`` and re-raises whatever the plugin failed with."""
    err: list[BaseException] = []

    def _go():
        try:
            MySQLCdcStreamSrcPlugin().run(ctx=ctx, conn=conn, stream_src=src)
        except BaseException as exc:  # noqa: BLE001
            err.append(exc)
            ctx.stop()

    thread = threading.Thread(target=_go, name="mysqlcdc-plugin", daemon=True)
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
    """Poll until the staged changes satisfy `ready`. Capture latency is the binlog
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


@mysqlcdc_domain("mysqlcdc_employees_xs")
def test_cdc_plugin_stages_captured_inserts(
    domain_mysqlcdc_employees_xs: MysqlcdcDomain, tmp_path: Path
):
    domain = domain_mysqlcdc_employees_xs
    with cdc_probe(domain, "td_cdc_inserts") as probe:
        src = _stream_src([cdc_table(domain, probe.name)], cdc_anchor(domain))
        probe.insert(1, "alice")
        probe.insert(2, "bob")

        ctx = _CdcCtx(work_dir(tmp_path))
        with _running(ctx, cdc_src_conn(domain), src):
            frame = _await_captured(ctx, "both inserts", lambda f: len(_ids(f)) >= 2)

    # `columns_format="columns"` + `flatten_columns=True` inline the new values, so the
    # probe's own columns show up at the top level.
    assert set(frame.columns) >= {"id", "name"}
    assert _ids(frame) == [1, 2]
    assert frame.sort("id")["name"].to_list() == ["alice", "bob"]
    assert _ops(frame) == {INSERT}


@mysqlcdc_domain("mysqlcdc_employees_xs")
def test_cdc_plugin_captures_update_and_delete(
    domain_mysqlcdc_employees_xs: MysqlcdcDomain, tmp_path: Path
):
    domain = domain_mysqlcdc_employees_xs
    with cdc_probe(domain, "td_cdc_ops") as probe:
        src = _stream_src([cdc_table(domain, probe.name)], cdc_anchor(domain))
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


@mysqlcdc_domain("mysqlcdc_employees_xs")
def test_cdc_plugin_tail_skips_changes_committed_before_it_attached(
    domain_mysqlcdc_employees_xs: MysqlcdcDomain, tmp_path: Path
):
    domain = domain_mysqlcdc_employees_xs
    with cdc_probe(domain, "td_cdc_tail") as probe:
        probe.insert(100, "before-1")
        probe.insert(101, "before-2")

        src = _stream_src([cdc_table(domain, probe.name)], "tail")
        ctx = _CdcCtx(work_dir(tmp_path))
        with _running(ctx, cdc_src_conn(domain), src):
            # "tail" attaches at the live edge, which the test cannot observe, so it
            # keeps producing rows until one is captured instead of waiting a fixed
            # time for the stream to be up.
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


@mysqlcdc_domain("mysqlcdc_employees_xs")
def test_cdc_plugin_persists_gtid_offset_as_stream_state(
    domain_mysqlcdc_employees_xs: MysqlcdcDomain, tmp_path: Path
):
    domain = domain_mysqlcdc_employees_xs
    with cdc_probe(domain, "td_cdc_offset") as probe:
        src = _stream_src([cdc_table(domain, probe.name)], cdc_anchor(domain))
        probe.insert(1, "alice")

        ctx = _CdcCtx(work_dir(tmp_path))
        with _running(ctx, cdc_src_conn(domain), src):
            _await_captured(ctx, "the insert", lambda f: len(_ids(f)) >= 1)

    # MySQL is a Tabsdata-owned offset: `GtidOffset.write` persists `{"gtid": ...}`.
    state = ctx.stream_state
    assert state is not None
    assert json.loads(state["cdc.offset"])["gtid"]


@mysqlcdc_domain("mysqlcdc_employees_xs")
def test_cdc_plugin_resumes_from_persisted_offset(
    domain_mysqlcdc_employees_xs: MysqlcdcDomain, tmp_path: Path
):
    domain = domain_mysqlcdc_employees_xs
    with cdc_probe(domain, "td_cdc_resume") as probe:
        anchor = cdc_anchor(domain)
        tables = [cdc_table(domain, probe.name)]
        probe.insert(1, "alice")

        first = _CdcCtx(work_dir(tmp_path, "first"))
        with _running(first, cdc_src_conn(domain), _stream_src(tables, anchor)):
            _await_captured(first, "the first insert", lambda f: 1 in _ids(f))
        persisted = first.stream_state
        assert persisted is not None

        probe.insert(2, "bob")

        # Same anchor, so a cold start would replay row 1; seeded with the offset the
        # first run persisted, the second one resumes past it.
        second = _CdcCtx(work_dir(tmp_path, "second"), initial_state=persisted)
        assert second.first_stage is False
        with _running(second, cdc_src_conn(domain), _stream_src(tables, anchor)):
            frame = _await_captured(second, "the second insert", lambda f: 2 in _ids(f))

    assert _ids(frame) == [2]


@mysqlcdc_domain("mysqlcdc_employees_xs")
def test_cdc_plugin_captures_every_tracked_table(
    domain_mysqlcdc_employees_xs: MysqlcdcDomain, tmp_path: Path
):
    domain = domain_mysqlcdc_employees_xs
    with cdc_probe(domain, "td_cdc_first") as first:
        with cdc_probe(domain, "td_cdc_second") as second:
            src = _stream_src(
                [cdc_table(domain, first.name), cdc_table(domain, second.name)],
                cdc_anchor(domain),
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
