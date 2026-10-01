#
# Copyright 2026 Tabsdata Inc.
#

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
from sqlalchemy import create_engine, text
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

pytestmark = [
    pytest.mark.mysql,
    pytest.mark.xdist_group("mysqlcdc_seq"),
]

CAPTURE_TIMEOUT_SECONDS = 120.0
PLUGIN_EXIT_TIMEOUT_SECONDS = 60.0
POLL_SECONDS = 0.25

OP_COLUMN_SUFFIX = "cdc.meta.op"
SEG_COLUMN_SUFFIX = "cdc.meta.seg"
SCH_COLUMN_SUFFIX = "cdc.meta.sch"
INSERT, UPDATE, DELETE, LOAD, TRUNCATE = "i", "u", "d", "l", "t"

INITIAL_LOAD_SEG = -1

CDC_CFG = {
    "tabsdata.cdc.buffer.max_rows": 1,
    "tabsdata.cdc.buffer.max_secs": 1,
    "tabsdata.cdc.trigger.max_rows": 1,
    "tabsdata.cdc.poll_interval_seconds": 1,
    "mysql.cdc.blocking_timeout_seconds": 2,
}


class _StageFailed(RuntimeError):
    pass


class _CdcCtx(StreamSrcPluginCtx):
    def __init__(
        self,
        work: Path,
        carry_over: dict | None = None,
        fail_on_stage: bool = False,
    ) -> None:
        self._work_dir = work
        self._stop = threading.Event()
        self._state: dict[str, Any] = dict(carry_over or {})
        self._staged: list[list[list[Path]]] = []
        self._resumed = bool(carry_over)
        self._fail_on_stage = fail_on_stage
        self._lock = threading.Lock()

    @property
    def work_dir(self) -> Path:
        return self._work_dir

    @property
    def exec_plan_id(self) -> str:
        return "mysqlcdc-initial-load-exec-plan"

    @property
    def trx_id(self) -> str:
        return "mysqlcdc-initial-load-trx"

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
            return not self._staged and not self._resumed

    @property
    def stream_state(self) -> dict | None:
        with self._lock:
            return dict(self._state) if self._state else None

    @property
    def flush_interval_mins(self) -> int:
        return 0

    def stage(self, tables_files, new_state):
        with self._lock:
            if self._fail_on_stage:
                raise _StageFailed("staging refused by the test")
            self._staged.append([list(files) for files in tables_files])
            if isinstance(new_state, dict):
                self._state = dict(new_state)
            else:
                new_state()

    def stop(self) -> None:
        self._stop.set()

    def manifests(self) -> list[list[list[Path]]]:
        with self._lock:
            return list(self._staged)

    def persisted_state(self) -> dict:
        with self._lock:
            return dict(self._state)


def _stream_src(tables: list[str], **cfg) -> MySQLCdcStreamSrc:
    return MySQLCdcStreamSrc(tables=tables, initial_load=True, src_cfg=CDC_CFG | cfg)


@contextmanager
def _running(
    ctx: _CdcCtx, conn: MySQLCdcStreamSrcConn, src: MySQLCdcStreamSrc
) -> Iterator[None]:
    err: list[BaseException] = []

    def _go():
        try:
            MySQLCdcStreamSrcPlugin().run(ctx=ctx, conn=conn, stream_src=src)
        except BaseException as exc:  # noqa: BLE001
            err.append(exc)
            ctx.stop()

    thread = threading.Thread(target=_go, name="mysqlcdc-initial-load", daemon=True)
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


def _rows(ctx: _CdcCtx, slot: int | None = None) -> pl.DataFrame | None:
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


def _await(ctx: _CdcCtx, expected: str, ready: Callable[[pl.DataFrame], bool]):
    deadline = time.monotonic() + CAPTURE_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        frame = _rows(ctx)
        if frame is not None and ready(frame):
            return frame
        time.sleep(POLL_SECONDS)
    pytest.fail(
        f"initial load / CDC did not produce {expected} within "
        f"{CAPTURE_TIMEOUT_SECONDS}s (staged: {_rows(ctx)})"
    )


def _column(frame: pl.DataFrame, suffix: str) -> str:
    return next(name for name in frame.columns if name.endswith(suffix))


def _ops(frame: pl.DataFrame) -> list[str]:
    return frame[_column(frame, OP_COLUMN_SUFFIX)].to_list()


def _ids(frame: pl.DataFrame) -> list[int]:
    return sorted(value for value in frame["id"].to_list() if value is not None)


def _loaded_ids(frame: pl.DataFrame) -> list[int]:
    seg = _column(frame, SEG_COLUMN_SUFFIX)
    loaded = frame.filter(pl.col(seg) == INITIAL_LOAD_SEG)
    return sorted(value for value in loaded["id"].to_list() if value is not None)


def _captured_ids(frame: pl.DataFrame) -> list[int]:
    seg = _column(frame, SEG_COLUMN_SUFFIX)
    captured = frame.filter(pl.col(seg) != INITIAL_LOAD_SEG)
    return sorted(value for value in captured["id"].to_list() if value is not None)


def _ops_for(frame: pl.DataFrame, identifier: int) -> list[str]:
    return _ops(frame.filter(pl.col("id") == identifier))


@mysqlcdc_domain("mysqlcdc_employees_xs")
def test_initial_load_emits_every_existing_row_once(
    domain_mysqlcdc_employees_xs: MysqlcdcDomain, tmp_path: Path
):
    domain = domain_mysqlcdc_employees_xs
    with cdc_probe(domain, "td_il_rows") as probe:
        for identifier in range(1, 21):
            probe.insert(identifier, f"row-{identifier}")

        src = _stream_src([cdc_table(domain, probe.name)])
        ctx = _CdcCtx(work_dir(tmp_path))
        with _running(ctx, cdc_src_conn(domain), src):
            frame = _await(ctx, "the whole snapshot", lambda f: len(_ids(f)) >= 20)

    assert _loaded_ids(frame) == list(range(1, 21))
    assert _ops(frame).count(LOAD) == 1
    assert _ops(frame).count(INSERT) == 20


@mysqlcdc_domain("mysqlcdc_employees_xs")
def test_changes_during_the_initial_load_are_not_lost(
    domain_mysqlcdc_employees_xs: MysqlcdcDomain, tmp_path: Path
):
    domain = domain_mysqlcdc_employees_xs
    with cdc_probe(domain, "td_il_concurrent") as probe:
        for identifier in range(1, 501):
            probe.insert(identifier, f"seed-{identifier}")

        src = _stream_src(
            [cdc_table(domain, probe.name)],
            **{"tabsdata.cdc.fetch_size": 10, "tabsdata.cdc.chunk_size": 10},
        )
        ctx = _CdcCtx(work_dir(tmp_path))
        with _running(ctx, cdc_src_conn(domain), src):
            for identifier in range(1000, 1010):
                probe.insert(identifier, f"during-{identifier}")
                time.sleep(0.05)
            frame = _await(
                ctx,
                "the snapshot plus every concurrent insert",
                lambda f: (
                    set(range(1000, 1010)) <= set(_ids(f))
                    and len(_loaded_ids(f)) >= 500
                ),
            )

    # A row inserted while the load runs may commit inside the snapshot, so it
    # can arrive as a loaded row or as a captured change. What must hold is that
    # every seed row is loaded and no row is lost either way.
    assert set(range(1, 501)) <= set(_loaded_ids(frame))
    assert set(range(1000, 1010)) <= set(_ids(frame))


@mysqlcdc_domain("mysqlcdc_employees_xs")
def test_changes_right_after_the_initial_load_are_captured(
    domain_mysqlcdc_employees_xs: MysqlcdcDomain, tmp_path: Path
):
    domain = domain_mysqlcdc_employees_xs
    with cdc_probe(domain, "td_il_after") as probe:
        probe.insert(1, "seed")

        src = _stream_src([cdc_table(domain, probe.name)])
        ctx = _CdcCtx(work_dir(tmp_path))
        with _running(ctx, cdc_src_conn(domain), src):
            _await(ctx, "the snapshot", lambda f: _loaded_ids(f) == [1])
            probe.insert(2, "after")
            probe.update(1, "seed-changed")
            probe.delete(2)
            frame = _await(
                ctx,
                "insert, update and delete after the load",
                lambda f: {UPDATE, DELETE} <= set(_ops(f)),
            )

    assert _loaded_ids(frame) == [1]
    assert {INSERT, UPDATE, DELETE} <= set(_ops(frame))
    assert 2 in _captured_ids(frame)


@mysqlcdc_domain("mysqlcdc_employees_xs")
def test_restart_after_the_initial_load_does_not_reload(
    domain_mysqlcdc_employees_xs: MysqlcdcDomain, tmp_path: Path
):
    domain = domain_mysqlcdc_employees_xs
    with cdc_probe(domain, "td_il_restart") as probe:
        probe.insert(1, "before")

        src = _stream_src([cdc_table(domain, probe.name)])

        first = _CdcCtx(work_dir(tmp_path, "first"))
        with _running(first, cdc_src_conn(domain), src):
            _await(first, "the snapshot", lambda f: _loaded_ids(f) == [1])
        carried = first.persisted_state()

        probe.insert(2, "while-stopped")

        second = _CdcCtx(work_dir(tmp_path, "second"), carry_over=carried)
        with _running(second, cdc_src_conn(domain), src):
            frame = _await(
                second, "the row written while stopped", lambda f: 2 in _ids(f)
            )

    assert carried.get("cdc.initial-load") is False
    assert _loaded_ids(frame) == []
    assert 2 in _captured_ids(frame)


@mysqlcdc_domain("mysqlcdc_employees_xs")
def test_initial_load_stages_one_manifest_for_every_tracked_table(
    domain_mysqlcdc_employees_xs: MysqlcdcDomain, tmp_path: Path
):
    domain = domain_mysqlcdc_employees_xs
    with cdc_probe(domain, "td_il_m1") as first_probe:
        with cdc_probe(domain, "td_il_m2") as second_probe:
            for identifier in range(1, 101):
                first_probe.insert(identifier, f"first-{identifier}")
                second_probe.insert(identifier, f"second-{identifier}")

            # File rolling is evaluated per chunk handover, so the chunk size
            # has to be small too or the whole table arrives as one chunk and
            # writes one file.
            src = _stream_src(
                [
                    cdc_table(domain, first_probe.name),
                    cdc_table(domain, second_probe.name),
                ],
                **{
                    "tabsdata.cdc.buffer.max_rows": 25,
                    "tabsdata.cdc.fetch_size": 25,
                    "tabsdata.cdc.chunk_size": 25,
                },
            )
            ctx = _CdcCtx(work_dir(tmp_path))
            with _running(ctx, cdc_src_conn(domain), src):
                _await(
                    ctx,
                    "both tables' snapshots",
                    lambda _f: (
                        _rows(ctx, slot=0) is not None
                        and _rows(ctx, slot=1) is not None
                        and len(_loaded_ids(_rows(ctx, slot=0))) >= 100
                        and len(_loaded_ids(_rows(ctx, slot=1))) >= 100
                    ),
                )
                load_manifest = ctx.manifests()[0]

    assert len(load_manifest) == 2
    assert all(len(slot_files) > 1 for slot_files in load_manifest)
    assert _loaded_ids(_rows(ctx, slot=0)) == list(range(1, 101))
    assert _loaded_ids(_rows(ctx, slot=1)) == list(range(1, 101))


@mysqlcdc_domain("mysqlcdc_employees_xs")
def test_truncate_after_the_initial_load_is_captured(
    domain_mysqlcdc_employees_xs: MysqlcdcDomain, tmp_path: Path
):
    domain = domain_mysqlcdc_employees_xs
    with cdc_probe(domain, "td_il_truncate") as probe:
        probe.insert(1, "seed")

        src = _stream_src([cdc_table(domain, probe.name)])
        ctx = _CdcCtx(work_dir(tmp_path))
        with _running(ctx, cdc_src_conn(domain), src):
            _await(ctx, "the snapshot", lambda f: _loaded_ids(f) == [1])
            probe.insert(2, "second")
            probe.truncate()
            frame = _await(ctx, "the truncate", lambda f: TRUNCATE in _ops(f))

    assert _ops(frame).count(TRUNCATE) == 1
    assert 2 in _captured_ids(frame)


@mysqlcdc_domain("mysqlcdc_employees_xs")
def test_truncate_of_each_tracked_table_is_reported_separately(
    domain_mysqlcdc_employees_xs: MysqlcdcDomain, tmp_path: Path
):
    domain = domain_mysqlcdc_employees_xs
    with cdc_probe(domain, "td_il_tr1") as first_probe:
        with cdc_probe(domain, "td_il_tr2") as second_probe:
            first_probe.insert(1, "first")
            second_probe.insert(1, "second")

            src = _stream_src([
                cdc_table(domain, first_probe.name),
                cdc_table(domain, second_probe.name),
            ])
            ctx = _CdcCtx(work_dir(tmp_path))
            with _running(ctx, cdc_src_conn(domain), src):
                _await(
                    ctx,
                    "both snapshots",
                    lambda _f: (
                        _rows(ctx, slot=0) is not None
                        and _rows(ctx, slot=1) is not None
                    ),
                )
                first_probe.truncate()
                second_probe.truncate()
                _await(
                    ctx,
                    "a truncate for each table",
                    lambda _f: (
                        TRUNCATE in _ops(_rows(ctx, slot=0))
                        and TRUNCATE in _ops(_rows(ctx, slot=1))
                    ),
                )

    assert TRUNCATE in _ops(_rows(ctx, slot=0))
    assert TRUNCATE in _ops(_rows(ctx, slot=1))


@mysqlcdc_domain("mysqlcdc_employees_xs")
def test_the_initial_load_state_carries_both_the_flag_and_the_offset(
    domain_mysqlcdc_employees_xs: MysqlcdcDomain, tmp_path: Path
):
    domain = domain_mysqlcdc_employees_xs
    with cdc_probe(domain, "td_il_state") as probe:
        probe.insert(1, "seed")

        src = _stream_src([cdc_table(domain, probe.name)])
        ctx = _CdcCtx(work_dir(tmp_path))
        with _running(ctx, cdc_src_conn(domain), src):
            _await(ctx, "the snapshot", lambda f: _loaded_ids(f) == [1])
        staged = ctx.persisted_state()

    assert staged["cdc.initial-load"] is False
    assert "cdc.offset" in staged


@mysqlcdc_domain("mysqlcdc_employees_xs")
def test_resetting_the_flag_reloads_every_row_including_the_new_ones(
    domain_mysqlcdc_employees_xs: MysqlcdcDomain, tmp_path: Path
):
    domain = domain_mysqlcdc_employees_xs
    with cdc_probe(domain, "td_il_reset") as probe:
        probe.insert(1, "first")
        probe.insert(2, "second")

        src = _stream_src([cdc_table(domain, probe.name)])

        first = _CdcCtx(work_dir(tmp_path, "first"))
        with _running(first, cdc_src_conn(domain), src):
            _await(first, "the first snapshot", lambda f: _loaded_ids(f) == [1, 2])
        carried = first.persisted_state()

        probe.insert(3, "added-while-stopped")

        reset = dict(carried) | {"cdc.initial-load": True}
        second = _CdcCtx(work_dir(tmp_path, "second"), carry_over=reset)
        with _running(second, cdc_src_conn(domain), src):
            frame = _await(
                second, "a second full snapshot", lambda f: _loaded_ids(f) == [1, 2, 3]
            )

    assert _loaded_ids(frame) == [1, 2, 3]
    assert _ops(frame).count(LOAD) == 1
    assert second.persisted_state()["cdc.initial-load"] is False


@mysqlcdc_domain("mysqlcdc_employees_xs")
def test_a_reset_reload_does_not_also_replay_the_rows_as_changes(
    domain_mysqlcdc_employees_xs: MysqlcdcDomain, tmp_path: Path
):
    domain = domain_mysqlcdc_employees_xs
    with cdc_probe(domain, "td_il_reset_nodup") as probe:
        probe.insert(1, "first")

        src = _stream_src([cdc_table(domain, probe.name)])

        first = _CdcCtx(work_dir(tmp_path, "first"))
        with _running(first, cdc_src_conn(domain), src):
            _await(first, "the first snapshot", lambda f: _loaded_ids(f) == [1])
        carried = first.persisted_state()

        probe.insert(2, "between-runs")

        reset = dict(carried) | {"cdc.initial-load": True}
        second = _CdcCtx(work_dir(tmp_path, "second"), carry_over=reset)
        with _running(second, cdc_src_conn(domain), src):
            _await(second, "the second snapshot", lambda f: _loaded_ids(f) == [1, 2])
            probe.insert(3, "after-second-load")
            frame = _await(
                second,
                "the change made after the second load",
                lambda f: 3 in _captured_ids(f),
            )

    assert _loaded_ids(frame) == [1, 2]
    assert _captured_ids(frame) == [3]


@mysqlcdc_domain("mysqlcdc_employees_xs")
def test_a_source_without_initial_load_never_loads_whatever_the_state_says(
    domain_mysqlcdc_employees_xs: MysqlcdcDomain, tmp_path: Path
):
    domain = domain_mysqlcdc_employees_xs
    with cdc_probe(domain, "td_il_disabled") as probe:
        probe.insert(1, "pre-existing")

        src = MySQLCdcStreamSrc(
            tables=[cdc_table(domain, probe.name)],
            start_from=cdc_anchor(domain),
            src_cfg=dict(CDC_CFG),
        )
        ctx = _CdcCtx(work_dir(tmp_path), carry_over={"cdc.initial-load": True})
        with _running(ctx, cdc_src_conn(domain), src):
            probe.insert(2, "after-attach")
            frame = _await(
                ctx, "the post-attach insert", lambda f: 2 in _captured_ids(f)
            )

    assert _loaded_ids(frame) == []
    assert LOAD not in _ops(frame)


@mysqlcdc_domain("mysqlcdc_employees_xs")
def test_a_run_interrupted_before_the_load_stages_redoes_it(
    domain_mysqlcdc_employees_xs: MysqlcdcDomain, tmp_path: Path
):
    domain = domain_mysqlcdc_employees_xs
    with cdc_probe(domain, "td_il_interrupted") as probe:
        probe.insert(1, "seed")

        src = _stream_src([cdc_table(domain, probe.name)])

        aborted = _CdcCtx(work_dir(tmp_path, "aborted"), fail_on_stage=True)
        with pytest.raises(_StageFailed):
            with _running(aborted, cdc_src_conn(domain), src):
                time.sleep(POLL_SECONDS)

        assert aborted.persisted_state() == {}

        retry = _CdcCtx(work_dir(tmp_path, "retry"))
        with _running(retry, cdc_src_conn(domain), src):
            frame = _await(retry, "the snapshot", lambda f: _loaded_ids(f) == [1])

    assert _loaded_ids(frame) == [1]
    assert retry.persisted_state()["cdc.initial-load"] is False


@mysqlcdc_domain("mysqlcdc_employees_xs")
def test_two_consecutive_restarts_each_resume_without_replaying(
    domain_mysqlcdc_employees_xs: MysqlcdcDomain, tmp_path: Path
):
    domain = domain_mysqlcdc_employees_xs
    with cdc_probe(domain, "td_il_chain") as probe:
        probe.insert(1, "seed")

        src = _stream_src([cdc_table(domain, probe.name)])

        first = _CdcCtx(work_dir(tmp_path, "first"))
        with _running(first, cdc_src_conn(domain), src):
            _await(first, "the snapshot", lambda f: _loaded_ids(f) == [1])
        state = first.persisted_state()

        probe.insert(2, "between-one-and-two")

        second = _CdcCtx(work_dir(tmp_path, "second"), carry_over=state)
        with _running(second, cdc_src_conn(domain), src):
            _await(second, "the second run's row", lambda f: 2 in _ids(f))
        second_frame = _rows(second)
        state = second.persisted_state()

        probe.insert(3, "between-two-and-three")

        third = _CdcCtx(work_dir(tmp_path, "third"), carry_over=state)
        with _running(third, cdc_src_conn(domain), src):
            _await(third, "the third run's row", lambda f: 3 in _ids(f))
        third_frame = _rows(third)

    # Binlog capture is at-least-once: a restart resumes from the last committed
    # position and may redeliver the change at it. What must not happen is a
    # lost change or a repeated snapshot.
    assert _loaded_ids(second_frame) == []
    assert 2 in _captured_ids(second_frame)
    assert _loaded_ids(third_frame) == []
    assert 3 in _captured_ids(third_frame)


@mysqlcdc_domain("mysqlcdc_employees_xs")
def test_updates_and_deletes_of_loaded_rows_arrive_after_the_load(
    domain_mysqlcdc_employees_xs: MysqlcdcDomain, tmp_path: Path
):
    domain = domain_mysqlcdc_employees_xs
    with cdc_probe(domain, "td_il_mutate") as probe:
        probe.insert(1, "loaded-then-updated")
        probe.insert(2, "loaded-then-deleted")

        src = _stream_src([cdc_table(domain, probe.name)])

        first = _CdcCtx(work_dir(tmp_path, "first"))
        with _running(first, cdc_src_conn(domain), src):
            _await(first, "the snapshot", lambda f: _loaded_ids(f) == [1, 2])
        state = first.persisted_state()

        probe.update(1, "updated-while-stopped")
        probe.delete(2)

        second = _CdcCtx(work_dir(tmp_path, "second"), carry_over=state)
        with _running(second, cdc_src_conn(domain), src):
            frame = _await(
                second,
                "the update and the delete",
                lambda f: {UPDATE, DELETE} <= set(_ops(f)),
            )

    assert _loaded_ids(frame) == []
    assert _ops_for(frame, 1) == [UPDATE]
    assert _ops_for(frame, 2) == [DELETE]


@mysqlcdc_domain("mysqlcdc_employees_xs")
def test_a_table_added_after_the_load_is_not_back_filled(
    domain_mysqlcdc_employees_xs: MysqlcdcDomain, tmp_path: Path
):
    domain = domain_mysqlcdc_employees_xs
    with cdc_probe(domain, "td_il_add1") as first_probe:
        with cdc_probe(domain, "td_il_add2") as second_probe:
            first_probe.insert(1, "tracked-from-the-start")
            second_probe.insert(1, "pre-existing-untracked")

            loaded = _stream_src([cdc_table(domain, first_probe.name)])

            first = _CdcCtx(work_dir(tmp_path, "first"))
            with _running(first, cdc_src_conn(domain), loaded):
                _await(first, "the snapshot", lambda f: _loaded_ids(f) == [1])
            state = first.persisted_state()

            widened = _stream_src([
                cdc_table(domain, first_probe.name),
                cdc_table(domain, second_probe.name),
            ])
            second_probe.insert(2, "written-while-tracked")

            second = _CdcCtx(work_dir(tmp_path, "second"), carry_over=state)
            with _running(second, cdc_src_conn(domain), widened):
                _await(
                    second,
                    "the new table's change",
                    lambda _f: (
                        _rows(second, slot=1) is not None
                        and 2 in _ids(_rows(second, slot=1))
                    ),
                )
                added = _rows(second, slot=1)

    assert _loaded_ids(added) == []
    assert 2 in _captured_ids(added)
    assert 1 not in _ids(added)


@mysqlcdc_domain("mysqlcdc_employees_xs")
def test_per_table_offsets_advance_independently_after_the_load(
    domain_mysqlcdc_employees_xs: MysqlcdcDomain, tmp_path: Path
):
    domain = domain_mysqlcdc_employees_xs
    with cdc_probe(domain, "td_il_off1") as first_probe:
        with cdc_probe(domain, "td_il_off2") as second_probe:
            first_probe.insert(1, "first-seed")
            second_probe.insert(1, "second-seed")

            src = _stream_src([
                cdc_table(domain, first_probe.name),
                cdc_table(domain, second_probe.name),
            ])

            first = _CdcCtx(work_dir(tmp_path, "first"))
            with _running(first, cdc_src_conn(domain), src):
                _await(
                    first,
                    "both snapshots",
                    lambda _f: (
                        _rows(first, slot=0) is not None
                        and _rows(first, slot=1) is not None
                    ),
                )
            state = first.persisted_state()

            second_probe.insert(2, "only-the-second-table")

            second = _CdcCtx(work_dir(tmp_path, "second"), carry_over=state)
            with _running(second, cdc_src_conn(domain), src):
                _await(
                    second,
                    "the second table's change",
                    lambda _f: (
                        _rows(second, slot=1) is not None
                        and 2 in _ids(_rows(second, slot=1))
                    ),
                )
                untouched = _rows(second, slot=0)
                changed = _rows(second, slot=1)

    # At-least-once resume, so membership rather than exact equality.
    assert 2 in _captured_ids(changed)
    assert untouched is None or _ids(untouched) == []


@mysqlcdc_domain("mysqlcdc_employees_xs")
def test_truncate_then_reinsert_after_the_load_keeps_their_order(
    domain_mysqlcdc_employees_xs: MysqlcdcDomain, tmp_path: Path
):
    domain = domain_mysqlcdc_employees_xs
    with cdc_probe(domain, "td_il_tr_reinsert") as probe:
        probe.insert(1, "loaded")

        src = _stream_src([cdc_table(domain, probe.name)])
        ctx = _CdcCtx(work_dir(tmp_path))
        with _running(ctx, cdc_src_conn(domain), src):
            _await(ctx, "the snapshot", lambda f: _loaded_ids(f) == [1])
            probe.truncate()
            probe.insert(9, "after-the-truncate")
            frame = _await(
                ctx,
                "the truncate and the row after it",
                lambda f: TRUNCATE in _ops(f) and 9 in _captured_ids(f),
            )

    seg = _column(frame, SEG_COLUMN_SUFFIX)
    captured = _ops(frame.filter(pl.col(seg) != INITIAL_LOAD_SEG))
    assert captured.index(TRUNCATE) < captured.index(INSERT)


# One column per common type, so that a type whose catalog and binlog
# descriptions normalize differently gives the loaded rows and the captured
# changes two schema hashes.
TYPE_ZOO_COLUMNS = (
    "id INTEGER PRIMARY KEY, c_tiny TINYINT, c_small SMALLINT, c_big BIGINT, "
    "c_unsigned INT UNSIGNED, c_num DECIMAL(10, 2), c_float FLOAT, c_double DOUBLE, "
    "c_bool BOOLEAN, c_varchar VARCHAR(20), c_char CHAR(5), c_text TEXT, "
    "c_date DATE, c_time TIME, c_datetime DATETIME, c_ts TIMESTAMP NULL, "
    "c_varbinary VARBINARY(8)"
)
TYPE_ZOO_VALUES = (
    "{id}, 1, 7, 9000000000, 4000000000, 12.34, 1.5, 2.25, true, 'varchar', 'chr', "
    "'text', '2026-01-02', '03:04:05', '2026-01-02 03:04:05', "
    "'2026-01-02 03:04:05', x'0001'"
)


@contextmanager
def _type_zoo(domain: MysqlcdcDomain, table: str) -> Iterator[str]:
    engine = create_engine(domain.sqlalchemy_uri)
    try:
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS `{table}`"))
            conn.execute(text(f"CREATE TABLE `{table}` ({TYPE_ZOO_COLUMNS})"))
        yield table
    finally:
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS `{table}`"))
        engine.dispose()


def _insert_zoo_row(domain: MysqlcdcDomain, table: str, id_: int) -> None:
    engine = create_engine(domain.sqlalchemy_uri)
    try:
        with engine.begin() as conn:
            values = TYPE_ZOO_VALUES.format(id=id_)
            conn.execute(text(f"INSERT INTO `{table}` VALUES ({values})"))
    finally:
        engine.dispose()


@mysqlcdc_domain("mysqlcdc_employees_xs")
def test_loaded_rows_and_changes_share_one_schema_hash(
    domain_mysqlcdc_employees_xs: MysqlcdcDomain, tmp_path: Path
):
    domain = domain_mysqlcdc_employees_xs
    with _type_zoo(domain, "td_il_types") as table:
        _insert_zoo_row(domain, table, 1)

        src = _stream_src([cdc_table(domain, table)])
        ctx = _CdcCtx(work_dir(tmp_path))
        with _running(ctx, cdc_src_conn(domain), src):
            _await(ctx, "the snapshot", lambda f: _loaded_ids(f) == [1])
            _insert_zoo_row(domain, table, 2)
            frame = _await(ctx, "the change", lambda f: 2 in _captured_ids(f))

    hashes = frame[_column(frame, SCH_COLUMN_SUFFIX)].unique().to_list()
    assert len(hashes) == 1, (
        f"the load and the stream normalized the schema differently: {hashes}"
    )
