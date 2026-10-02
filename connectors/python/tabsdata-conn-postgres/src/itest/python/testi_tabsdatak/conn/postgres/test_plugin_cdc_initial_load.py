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
import psycopg2
import psycopg2.extras
import pyarrow.parquet as pq
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from tabsdatax.itest.fixture.backends.postgrescdc import (
    PostgrescdcDomain,
    postgrescdc_domain,
)
from testi_tabsdatak.conn.common.sql.plugin_io import work_dir
from testi_tabsdatak.conn.postgres.backend import (
    cdc_probe,
    cdc_src_conn,
    cdc_table,
    free_replication_slot,
    replication_slot,
)

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._floor import unsafe_seconds
from tabsdatak.conn.postgres import PostgresCdcStreamSrc, PostgresCdcStreamSrcConn

# noinspection PyProtectedMember
from tabsdatak.conn.postgres._cdc_impl import LsnOffset, gap_to_text

# noinspection PyProtectedMember
from tabsdatak.conn.postgres._cdc_plugin import PostgresCdcStreamSrcPlugin
from tabsdatak.conn.postgres.error import PostgresErrorCode, PostgresRuntimeException
from tabsdatak.spi import StreamSrcPluginCtx

pytestmark = [
    pytest.mark.postgres,
    pytest.mark.xdist_group("postgrescdc_seq"),
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
    "tabsdata.cdc.trigger.max_secs": unsafe_seconds(1),
    "tabsdata.cdc.poll.idle_secs": 1,
    "tabsdata.cdc.poll.max_secs": 2,
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
        return "postgrescdc-initial-load-exec-plan"

    @property
    def trx_id(self) -> str:
        return "postgrescdc-initial-load-trx"

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

    def checkpoint(self, new_state):
        with self._lock:
            self._state = dict(new_state)

    def stop(self) -> None:
        self._stop.set()

    def manifests(self) -> list[list[list[Path]]]:
        with self._lock:
            return list(self._staged)

    def persisted_state(self) -> dict:
        with self._lock:
            return dict(self._state)


def _stream_src(tables: list[str], slot: str, **cfg) -> PostgresCdcStreamSrc:
    return PostgresCdcStreamSrc(
        tables=tables,
        replication_slot=slot,
        replication_slot_behavior="reuse",
        initial_load=True,
        src_cfg=CDC_CFG | cfg,
    )


@contextmanager
def _running(
    ctx: _CdcCtx, conn: PostgresCdcStreamSrcConn, src: PostgresCdcStreamSrc
) -> Iterator[None]:
    err: list[BaseException] = []

    def _go():
        try:
            PostgresCdcStreamSrcPlugin().run(ctx=ctx, conn=conn, stream_src=src)
        except BaseException as exc:  # noqa: BLE001
            err.append(exc)
            ctx.stop()

    thread = threading.Thread(target=_go, name="postgrescdc-initial-load", daemon=True)
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
    rows = frame.filter(pl.col("id") == identifier)
    return _ops(rows)


@postgrescdc_domain("postgrescdc_employees_xs")
def test_initial_load_emits_every_existing_row_once(
    domain_postgrescdc_employees_xs: PostgrescdcDomain, tmp_path: Path
):
    domain = domain_postgrescdc_employees_xs
    with cdc_probe(domain, "td_il_rows") as probe:
        for identifier in range(1, 21):
            probe.insert(identifier, f"row-{identifier}")

        with replication_slot(domain, "td_il_rows_slot") as slot:
            src = _stream_src([cdc_table(probe.name)], slot)
            ctx = _CdcCtx(work_dir(tmp_path))
            with _running(ctx, cdc_src_conn(domain), src):
                frame = _await(ctx, "the whole snapshot", lambda f: len(_ids(f)) >= 20)

    assert _loaded_ids(frame) == list(range(1, 21))
    assert _ops(frame).count(LOAD) == 1
    assert _ops(frame).count(INSERT) == 20


@postgrescdc_domain("postgrescdc_employees_xs")
def test_changes_during_the_initial_load_are_not_lost(
    domain_postgrescdc_employees_xs: PostgrescdcDomain, tmp_path: Path
):
    domain = domain_postgrescdc_employees_xs
    with cdc_probe(domain, "td_il_concurrent") as probe:
        for identifier in range(1, 501):
            probe.insert(identifier, f"seed-{identifier}")

        with replication_slot(domain, "td_il_concurrent_slot") as slot:
            src = _stream_src(
                [cdc_table(probe.name)],
                slot,
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


@postgrescdc_domain("postgrescdc_employees_xs")
def test_changes_right_after_the_initial_load_are_captured(
    domain_postgrescdc_employees_xs: PostgrescdcDomain, tmp_path: Path
):
    domain = domain_postgrescdc_employees_xs
    with cdc_probe(domain, "td_il_after") as probe:
        probe.insert(1, "seed")

        with replication_slot(domain, "td_il_after_slot") as slot:
            src = _stream_src([cdc_table(probe.name)], slot)
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


@postgrescdc_domain("postgrescdc_employees_xs")
def test_restart_after_the_initial_load_does_not_reload(
    domain_postgrescdc_employees_xs: PostgrescdcDomain, tmp_path: Path
):
    domain = domain_postgrescdc_employees_xs
    with cdc_probe(domain, "td_il_restart") as probe:
        probe.insert(1, "before")

        with replication_slot(domain, "td_il_restart_slot") as slot:
            src = _stream_src([cdc_table(probe.name)], slot)

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


@postgrescdc_domain("postgrescdc_employees_xs")
def test_initial_load_stages_one_manifest_for_every_tracked_table(
    domain_postgrescdc_employees_xs: PostgrescdcDomain, tmp_path: Path
):
    domain = domain_postgrescdc_employees_xs
    with cdc_probe(domain, "td_il_m1") as first_probe:
        with cdc_probe(domain, "td_il_m2") as second_probe:
            for identifier in range(1, 101):
                first_probe.insert(identifier, f"first-{identifier}")
                second_probe.insert(identifier, f"second-{identifier}")

            with replication_slot(domain, "td_il_multi_slot") as slot:
                src = _stream_src(
                    [cdc_table(first_probe.name), cdc_table(second_probe.name)],
                    slot,
                    # File rolling is evaluated per chunk handover, so the chunk
                    # size has to be small too or the whole table arrives as one
                    # chunk and writes one file.
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


@postgrescdc_domain("postgrescdc_employees_xs")
def test_truncate_after_the_initial_load_is_captured(
    domain_postgrescdc_employees_xs: PostgrescdcDomain, tmp_path: Path
):
    domain = domain_postgrescdc_employees_xs
    with cdc_probe(domain, "td_il_truncate") as probe:
        probe.insert(1, "seed")

        with replication_slot(domain, "td_il_truncate_slot") as slot:
            src = _stream_src([cdc_table(probe.name)], slot)
            ctx = _CdcCtx(work_dir(tmp_path))
            with _running(ctx, cdc_src_conn(domain), src):
                _await(ctx, "the snapshot", lambda f: _loaded_ids(f) == [1])
                probe.insert(2, "second")
                probe.truncate()
                frame = _await(ctx, "the truncate", lambda f: TRUNCATE in _ops(f))

    assert _ops(frame).count(TRUNCATE) == 1


@postgrescdc_domain("postgrescdc_employees_xs")
def test_truncate_of_each_tracked_table_is_reported_separately(
    domain_postgrescdc_employees_xs: PostgrescdcDomain, tmp_path: Path
):
    domain = domain_postgrescdc_employees_xs
    with cdc_probe(domain, "td_il_tr1") as first_probe:
        with cdc_probe(domain, "td_il_tr2") as second_probe:
            first_probe.insert(1, "first")
            second_probe.insert(1, "second")

            with replication_slot(domain, "td_il_tr_slot") as slot:
                src = _stream_src(
                    [cdc_table(first_probe.name), cdc_table(second_probe.name)], slot
                )
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


@postgrescdc_domain("postgrescdc_employees_xs")
def test_the_initial_load_state_carries_both_the_flag_and_the_offset(
    domain_postgrescdc_employees_xs: PostgrescdcDomain, tmp_path: Path
):
    domain = domain_postgrescdc_employees_xs
    with cdc_probe(domain, "td_il_state") as probe:
        probe.insert(1, "seed")

        with replication_slot(domain, "td_il_state_slot") as slot:
            src = _stream_src([cdc_table(probe.name)], slot)
            ctx = _CdcCtx(work_dir(tmp_path))
            with _running(ctx, cdc_src_conn(domain), src):
                _await(ctx, "the snapshot", lambda f: _loaded_ids(f) == [1])
            staged = ctx.persisted_state()

    assert staged["cdc.initial-load"] is False
    assert "cdc.offset" in staged


@postgrescdc_domain("postgrescdc_employees_xs")
def test_resetting_the_flag_reloads_every_row_including_the_new_ones(
    domain_postgrescdc_employees_xs: PostgrescdcDomain, tmp_path: Path
):
    domain = domain_postgrescdc_employees_xs
    with cdc_probe(domain, "td_il_reset") as probe:
        probe.insert(1, "first")
        probe.insert(2, "second")

        with replication_slot(domain, "td_il_reset_slot") as slot:
            src = _stream_src([cdc_table(probe.name)], slot)

            first = _CdcCtx(work_dir(tmp_path, "first"))
            with _running(first, cdc_src_conn(domain), src):
                _await(first, "the first snapshot", lambda f: _loaded_ids(f) == [1, 2])
            carried = first.persisted_state()

            probe.insert(3, "added-while-stopped")

            reset = dict(carried) | {"cdc.initial-load": True}
            second = _CdcCtx(work_dir(tmp_path, "second"), carry_over=reset)
            with _running(second, cdc_src_conn(domain), src):
                frame = _await(
                    second,
                    "a second full snapshot",
                    lambda f: _loaded_ids(f) == [1, 2, 3],
                )

    assert _loaded_ids(frame) == [1, 2, 3]
    assert _ops(frame).count(LOAD) == 1
    assert second.persisted_state()["cdc.initial-load"] is False


@postgrescdc_domain("postgrescdc_employees_xs")
def test_a_reset_reload_does_not_also_replay_the_rows_as_changes(
    domain_postgrescdc_employees_xs: PostgrescdcDomain, tmp_path: Path
):
    domain = domain_postgrescdc_employees_xs
    with cdc_probe(domain, "td_il_reset_nodup") as probe:
        probe.insert(1, "first")

        with replication_slot(domain, "td_il_reset_nodup_slot") as slot:
            src = _stream_src([cdc_table(probe.name)], slot)

            first = _CdcCtx(work_dir(tmp_path, "first"))
            with _running(first, cdc_src_conn(domain), src):
                _await(first, "the first snapshot", lambda f: _loaded_ids(f) == [1])
            carried = first.persisted_state()

            probe.insert(2, "between-runs")

            reset = dict(carried) | {"cdc.initial-load": True}
            second = _CdcCtx(work_dir(tmp_path, "second"), carry_over=reset)
            with _running(second, cdc_src_conn(domain), src):
                frame = _await(
                    second, "the second snapshot", lambda f: _loaded_ids(f) == [1, 2]
                )
                probe.insert(3, "after-second-load")
                frame = _await(
                    second,
                    "the change made after the second load",
                    lambda f: 3 in _captured_ids(f),
                )

    # The reload re-reads row 2 as part of the snapshot, so it must not also
    # arrive as the insert the first run never consumed.
    assert _loaded_ids(frame) == [1, 2]
    assert _captured_ids(frame) == [3]


@postgrescdc_domain("postgrescdc_employees_xs")
def test_a_source_without_initial_load_never_loads_whatever_the_state_says(
    domain_postgrescdc_employees_xs: PostgrescdcDomain, tmp_path: Path
):
    domain = domain_postgrescdc_employees_xs
    with cdc_probe(domain, "td_il_disabled") as probe:
        probe.insert(1, "pre-existing")

        # No initial load here, so "reuse" needs the slot to already exist and
        # the slot itself is what anchors the capture.
        with replication_slot(domain, "td_il_disabled_slot") as slot:
            src = PostgresCdcStreamSrc(
                tables=[cdc_table(probe.name)],
                replication_slot=slot,
                replication_slot_behavior="reuse",
                start_from="head",
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


@postgrescdc_domain("postgrescdc_employees_xs")
def test_a_run_interrupted_before_the_load_stages_redoes_it(
    domain_postgrescdc_employees_xs: PostgrescdcDomain, tmp_path: Path
):
    domain = domain_postgrescdc_employees_xs
    with cdc_probe(domain, "td_il_interrupted") as probe:
        probe.insert(1, "seed")

        with replication_slot(domain, "td_il_interrupted_slot") as slot:
            src = _stream_src([cdc_table(probe.name)], slot)

            aborted = _CdcCtx(work_dir(tmp_path, "aborted"), fail_on_stage=True)
            with pytest.raises(_StageFailed):
                with _running(aborted, cdc_src_conn(domain), src):
                    time.sleep(POLL_SECONDS)

            # The load stages as one call, so a failure there leaves no state
            # and the flag stays owed.
            assert aborted.persisted_state() == {}

            retry = _CdcCtx(work_dir(tmp_path, "retry"), carry_over=None)
            with _running(retry, cdc_src_conn(domain), src):
                frame = _await(retry, "the snapshot", lambda f: _loaded_ids(f) == [1])

    assert _loaded_ids(frame) == [1]
    assert retry.persisted_state()["cdc.initial-load"] is False


@postgrescdc_domain("postgrescdc_employees_xs")
def test_two_consecutive_restarts_each_resume_without_replaying(
    domain_postgrescdc_employees_xs: PostgrescdcDomain, tmp_path: Path
):
    domain = domain_postgrescdc_employees_xs
    with cdc_probe(domain, "td_il_chain") as probe:
        probe.insert(1, "seed")

        with replication_slot(domain, "td_il_chain_slot") as slot:
            src = _stream_src([cdc_table(probe.name)], slot)

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

    # Logical decoding is at-least-once: a restart resumes from the slot's
    # confirmed flush position, which may redeliver the last change. What must
    # not happen is a lost change or a repeated snapshot.
    assert _loaded_ids(second_frame) == []
    assert 2 in _captured_ids(second_frame)
    assert _loaded_ids(third_frame) == []
    assert 3 in _captured_ids(third_frame)


@postgrescdc_domain("postgrescdc_employees_xs")
def test_updates_and_deletes_of_loaded_rows_arrive_after_the_load(
    domain_postgrescdc_employees_xs: PostgrescdcDomain, tmp_path: Path
):
    domain = domain_postgrescdc_employees_xs
    with cdc_probe(domain, "td_il_mutate") as probe:
        probe.insert(1, "loaded-then-updated")
        probe.insert(2, "loaded-then-deleted")

        with replication_slot(domain, "td_il_mutate_slot") as slot:
            src = _stream_src([cdc_table(probe.name)], slot)

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


@postgrescdc_domain("postgrescdc_employees_xs")
def test_a_table_added_after_the_load_is_not_back_filled(
    domain_postgrescdc_employees_xs: PostgrescdcDomain, tmp_path: Path
):
    domain = domain_postgrescdc_employees_xs
    with cdc_probe(domain, "td_il_add1") as first_probe:
        with cdc_probe(domain, "td_il_add2") as second_probe:
            first_probe.insert(1, "tracked-from-the-start")
            second_probe.insert(1, "pre-existing-untracked")

            with replication_slot(domain, "td_il_add_slot") as slot:
                loaded = _stream_src([cdc_table(first_probe.name)], slot)

                first = _CdcCtx(work_dir(tmp_path, "first"))
                with _running(first, cdc_src_conn(domain), loaded):
                    _await(first, "the snapshot", lambda f: _loaded_ids(f) == [1])
                state = first.persisted_state()

                widened = _stream_src(
                    [cdc_table(first_probe.name), cdc_table(second_probe.name)], slot
                )
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

    # The load flag is per stream, not per table, so a table added later is
    # picked up from the current offset and never snapshotted.
    assert _loaded_ids(added) == []
    assert 2 in _captured_ids(added)
    assert 1 not in _ids(added)


@postgrescdc_domain("postgrescdc_employees_xs")
def test_per_table_offsets_advance_independently_after_the_load(
    domain_postgrescdc_employees_xs: PostgrescdcDomain, tmp_path: Path
):
    domain = domain_postgrescdc_employees_xs
    with cdc_probe(domain, "td_il_off1") as first_probe:
        with cdc_probe(domain, "td_il_off2") as second_probe:
            first_probe.insert(1, "first-seed")
            second_probe.insert(1, "second-seed")

            with replication_slot(domain, "td_il_off_slot") as slot:
                src = _stream_src(
                    [cdc_table(first_probe.name), cdc_table(second_probe.name)], slot
                )

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

                # Only the second table changes while stopped.
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


@postgrescdc_domain("postgrescdc_employees_xs")
def test_truncate_then_reinsert_after_the_load_keeps_their_order(
    domain_postgrescdc_employees_xs: PostgrescdcDomain, tmp_path: Path
):
    domain = domain_postgrescdc_employees_xs
    with cdc_probe(domain, "td_il_tr_reinsert") as probe:
        probe.insert(1, "loaded")

        with replication_slot(domain, "td_il_tr_reinsert_slot") as slot:
            src = _stream_src([cdc_table(probe.name)], slot)
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

    captured = _ops(
        frame.filter(pl.col(_column(frame, SEG_COLUMN_SUFFIX)) != INITIAL_LOAD_SEG)
    )
    assert captured.index(TRUNCATE) < captured.index(INSERT)


def _creating_stream_src(tables: list[str], slot: str, **cfg) -> PostgresCdcStreamSrc:
    return PostgresCdcStreamSrc(
        tables=tables,
        replication_slot=slot,
        replication_slot_behavior="create",
        initial_load=True,
        src_cfg=CDC_CFG | cfg,
    )


@postgrescdc_domain("postgrescdc_employees_xs")
def test_a_creating_load_snapshots_from_the_slot_it_creates(
    domain_postgrescdc_employees_xs: PostgrescdcDomain, tmp_path: Path
):
    domain = domain_postgrescdc_employees_xs
    with cdc_probe(domain, "td_il_create") as probe:
        probe.insert(1, "seed")

        with free_replication_slot(domain, "td_il_create_slot") as slot:
            src = _creating_stream_src([cdc_table(probe.name)], slot)
            ctx = _CdcCtx(work_dir(tmp_path))
            with _running(ctx, cdc_src_conn(domain), src):
                _await(ctx, "the snapshot", lambda f: _loaded_ids(f) == [1])
                probe.insert(2, "after-the-load")
                frame = _await(
                    ctx, "the change after the load", lambda f: 2 in _captured_ids(f)
                )

    assert _loaded_ids(frame) == [1]
    assert 2 in _captured_ids(frame)


@postgrescdc_domain("postgrescdc_employees_xs")
def test_a_creating_load_refuses_a_slot_that_already_exists(
    domain_postgrescdc_employees_xs: PostgrescdcDomain, tmp_path: Path
):
    domain = domain_postgrescdc_employees_xs
    with cdc_probe(domain, "td_il_create_dup") as probe:
        probe.insert(1, "seed")

        with replication_slot(domain, "td_il_create_dup_slot") as slot:
            src = _creating_stream_src([cdc_table(probe.name)], slot)
            ctx = _CdcCtx(work_dir(tmp_path))
            with pytest.raises(PostgresRuntimeException, match="POSTGRES_21"):
                with _running(ctx, cdc_src_conn(domain), src):
                    time.sleep(POLL_SECONDS)

    assert ctx.manifests() == []
    assert ctx.persisted_state() == {}


@postgrescdc_domain("postgrescdc_employees_xs")
def test_a_reusing_load_refuses_a_slot_that_does_not_exist(
    domain_postgrescdc_employees_xs: PostgrescdcDomain, tmp_path: Path
):
    domain = domain_postgrescdc_employees_xs
    with cdc_probe(domain, "td_il_missing") as probe:
        probe.insert(1, "seed")

        with free_replication_slot(domain, "td_il_missing_slot") as slot:
            src = _stream_src([cdc_table(probe.name)], slot)
            ctx = _CdcCtx(work_dir(tmp_path))
            with pytest.raises(PostgresRuntimeException, match="POSTGRES_22"):
                with _running(ctx, cdc_src_conn(domain), src):
                    time.sleep(POLL_SECONDS)

    assert ctx.manifests() == []
    assert ctx.persisted_state() == {}


# One column per common type, so that a type whose catalog and change-stream
# descriptions normalize differently gives the loaded rows and the captured
# changes two schema hashes.
TYPE_ZOO_COLUMNS = (
    "id INTEGER PRIMARY KEY, c_small SMALLINT, c_big BIGINT, c_num NUMERIC(10, 2), "
    "c_real REAL, c_double DOUBLE PRECISION, c_bool BOOLEAN, c_varchar VARCHAR(20), "
    "c_char CHAR(5), c_text TEXT, c_date DATE, c_time TIME, c_ts TIMESTAMP, "
    "c_tstz TIMESTAMPTZ, c_bytea BYTEA"
)
TYPE_ZOO_VALUES = (
    "{id}, 7, 9000000000, 12.34, 1.5, 2.25, true, 'varchar', 'chr', 'text', "
    "'2026-01-02', '03:04:05', '2026-01-02 03:04:05', '2026-01-02 03:04:05+00', "
    "'\\x0001'"
)


@contextmanager
def _type_zoo(domain: PostgrescdcDomain, table: str) -> Iterator[str]:
    engine = create_engine(domain.sqlalchemy_uri)
    try:
        with engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS "{table}"'))
            conn.execute(text(f'CREATE TABLE "{table}" ({TYPE_ZOO_COLUMNS})'))
            conn.execute(text(f'ALTER TABLE "{table}" REPLICA IDENTITY FULL'))
        yield table
    finally:
        with engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS "{table}"'))
        engine.dispose()


def _insert_zoo_row(domain: PostgrescdcDomain, table: str, id_: int) -> None:
    engine = create_engine(domain.sqlalchemy_uri)
    try:
        with engine.begin() as conn:
            values = TYPE_ZOO_VALUES.format(id=id_)
            conn.execute(text(f'INSERT INTO "{table}" VALUES ({values})'))
    finally:
        engine.dispose()


@postgrescdc_domain("postgrescdc_employees_xs")
def test_loaded_rows_and_changes_share_one_schema_hash(
    domain_postgrescdc_employees_xs: PostgrescdcDomain, tmp_path: Path
):
    domain = domain_postgrescdc_employees_xs
    with _type_zoo(domain, "td_il_types") as table:
        _insert_zoo_row(domain, table, 1)

        with replication_slot(domain, "td_il_types_slot") as slot:
            src = _stream_src([cdc_table(table)], slot)
            ctx = _CdcCtx(work_dir(tmp_path))
            with _running(ctx, cdc_src_conn(domain), src):
                _await(ctx, "the snapshot", lambda f: _loaded_ids(f) == [1])
                _insert_zoo_row(domain, table, 2)
                frame = _await(ctx, "the change", lambda f: 2 in _captured_ids(f))

    hashes = frame[_column(frame, SCH_COLUMN_SUFFIX)].unique().to_list()
    assert len(hashes) == 1, (
        f"the load and the stream normalized the schema differently: {hashes}"
    )


def _replay_strictly(ctx: _CdcCtx) -> dict[int, str] | None:
    """Apply every change `ctx` staged, in the order it staged it, the way a
    strict consumer does: an insert on an existing row, or an update or
    delete of a missing one, is a duplicate or a gap and fails. ``None``
    until something is staged."""
    manifests = ctx.manifests()
    if not manifests:
        return None
    rows: dict[int, str] = {}
    for manifest in manifests:
        for files in manifest:
            for path in files:
                frame = pl.from_arrow(pq.read_table(path))
                op_column = _column(frame, OP_COLUMN_SUFFIX)
                old_id = _column(frame, "old.id")
                for change in frame.iter_rows(named=True):
                    op = change[op_column]
                    key = change["id"] if change["id"] is not None else change[old_id]
                    if op == LOAD:
                        rows.clear()
                    elif op == INSERT:
                        assert key not in rows, f"insert of existing row {key}"
                        rows[key] = change["name"]
                    elif op == UPDATE:
                        assert key in rows, f"update of missing row {key}"
                        rows[key] = change["name"]
                    elif op == DELETE:
                        assert key in rows, f"delete of missing row {key}"
                        del rows[key]
    return rows


@postgrescdc_domain("postgrescdc_employees_xs")
def test_changes_during_a_reused_slot_load_are_applied_exactly_once(
    domain_postgrescdc_employees_xs: PostgrescdcDomain, tmp_path: Path
):
    """A load on a reused slot reads the WAL position before exporting its
    snapshot, so a commit in between is both loaded and streamed: the stage
    carries each change exactly once, so a consumer that refuses duplicates
    and gaps ends at the table's contents."""
    domain = domain_postgrescdc_employees_xs
    seeded = 2000
    with cdc_probe(domain, "td_il_exactly_once") as probe:
        engine = create_engine(domain.sqlalchemy_uri)
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        f'INSERT INTO "{probe.name}" (id, name) '
                        f"SELECT g, 'seed-' || g FROM generate_series(1, {seeded}) g"
                    )
                )
        finally:
            engine.dispose()
        stop = threading.Event()

        def churn() -> None:
            # Several changes per row: updated, updated to the same contents,
            # back to its seeded contents, deleted and reinserted with the
            # same or other contents; and rows born, changed and killed.
            step = 0
            while not stop.is_set():
                step += 1
                key = step % seeded + 1
                probe.update(key, f"upd-{step}")
                probe.update(key, f"upd-{step}")
                probe.update(key, f"seed-{key}")
                born = seeded + step
                probe.insert(born, "new")
                probe.update(born, "changed")
                if step % 2 == 0:
                    probe.delete(born)
                if step % 3 == 0:
                    probe.delete(key)
                    probe.insert(key, f"seed-{key}")
                if step % 5 == 0:
                    probe.delete(key)
                    probe.insert(key, f"re-{step}")

        with replication_slot(domain, "td_il_exactly_once_slot") as slot:
            src = _stream_src(
                [cdc_table(probe.name)], slot, **{"tabsdata.cdc.fetch_size": 5}
            )
            ctx = _CdcCtx(work_dir(tmp_path))
            churner = threading.Thread(target=churn, name="pg-churn", daemon=True)
            churner.start()
            with _running(ctx, cdc_src_conn(domain), src):
                try:
                    _await(ctx, "the snapshot", lambda f: len(_loaded_ids(f)) >= seeded)
                finally:
                    stop.set()
                    churner.join(timeout=60)
                engine = create_engine(domain.sqlalchemy_uri)
                try:
                    with engine.connect() as conn:
                        expected = dict(
                            conn
                            .execute(text(f'SELECT id, name FROM "{probe.name}"'))
                            .tuples()
                            .all()
                        )
                finally:
                    engine.dispose()
                _await(
                    ctx,
                    "every change the churn made",
                    lambda f: _replay_strictly(ctx) == expected,
                )

    assert _replay_strictly(ctx) == expected


KEEPALIVE_TIMEOUT_SECONDS = 30.0


@postgrescdc_domain("postgrescdc_employees_xs")
def test_the_server_reports_its_decoded_position_with_nothing_to_send(
    domain_postgrescdc_employees_xs: PostgrescdcDomain,
):
    """The quiet end of a reused-slot load's window rests on this: with no
    change to send, the server's keepalives carry how far it has decoded
    (``sentPtr``), and psycopg2 exposes it as ``cursor.wal_end``. A logical
    message wal2json filters out is WAL with no output, so only a keepalive
    can report past it. Should a server, wal2json or psycopg2 upgrade change
    this, the window's read would no longer end on a quiet stream. Any other
    transaction committing in the database meanwhile is framed by BEGIN and
    COMMIT objects even with no change to send, so those are left out."""
    domain = domain_postgrescdc_employees_xs
    url = make_url(domain.sqlalchemy_uri)
    settings = {
        "host": url.host,
        "port": url.port,
        "dbname": url.database,
        "user": url.username,
        "password": url.password,
    }
    with replication_slot(domain, "td_il_keepalive_slot") as slot:
        replicator = psycopg2.connect(
            connection_factory=psycopg2.extras.LogicalReplicationConnection,
            **settings,
        )
        writer = psycopg2.connect(**settings)
        writer.autocommit = True
        try:
            cursor = replicator.cursor()
            cursor.start_replication(
                slot_name=slot,
                decode=False,
                options={
                    "format-version": "2",
                    "include-lsn": "true",
                    "include-transaction": "false",
                    "filter-msg-prefixes": "tdx.keepalive",
                },
            )
            with writer.cursor() as statement:
                statement.execute(
                    "select pg_logical_emit_message(false, 'tdx.keepalive', 'x')"
                )
                statement.execute("select pg_current_wal_insert_lsn() - '0/0'::pg_lsn")
                end = int(statement.fetchone()[0])
            sent = []
            deadline = time.monotonic() + KEEPALIVE_TIMEOUT_SECONDS
            while time.monotonic() < deadline and cursor.wal_end < end:
                message = cursor.read_message()
                if message is not None:
                    sent.append(message.payload)
                else:
                    time.sleep(POLL_SECONDS)
        finally:
            writer.close()
            replicator.close()

    assert sent == [], f"the filtered message produced output: {sent}"
    assert cursor.wal_end >= end, (
        f"no keepalive reported the decoded position {end:X} within "
        f"{KEEPALIVE_TIMEOUT_SECONDS:.0f}s (wal_end {cursor.wal_end:X})"
    )


@postgrescdc_domain("postgrescdc_employees_xs")
def test_a_reused_slot_load_with_nothing_committed_after_it_stages(
    domain_postgrescdc_employees_xs: PostgrescdcDomain, tmp_path: Path
):
    """Nothing commits after the load, so no transaction past the window's
    end says it is over: only the server's reported position can end the
    window's read, and without it the load would never stage."""
    domain = domain_postgrescdc_employees_xs
    with cdc_probe(domain, "td_il_quiet") as probe:
        for identifier in (1, 2, 3):
            probe.insert(identifier, f"row-{identifier}")

        with replication_slot(domain, "td_il_quiet_slot") as slot:
            src = _stream_src([cdc_table(probe.name)], slot)
            ctx = _CdcCtx(work_dir(tmp_path))
            with _running(ctx, cdc_src_conn(domain), src):
                frame = _await(ctx, "the load", lambda f: _loaded_ids(f) == [1, 2, 3])

    assert _captured_ids(frame) == []


def _until(expected: str, ready: Callable[[], Any]) -> Any:
    deadline = time.monotonic() + CAPTURE_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        value = ready()
        if value:
            return value
        time.sleep(POLL_SECONDS)
    pytest.fail(f"no {expected} within {CAPTURE_TIMEOUT_SECONDS:.0f}s")


def _state_lsn(ctx: _CdcCtx) -> int:
    return LsnOffset.read(ctx.persisted_state()["cdc.offset"]).lsn


def _slot_confirmed(domain: PostgrescdcDomain, slot: str) -> int:
    engine = create_engine(domain.sqlalchemy_uri)
    try:
        with engine.connect() as conn:
            return int(
                conn.execute(
                    text(
                        "select confirmed_flush_lsn - '0/0'::pg_lsn "
                        "from pg_replication_slots where slot_name = :slot"
                    ),
                    {"slot": slot},
                ).scalar_one()
            )
    finally:
        engine.dispose()


def _write_wal_with_no_changes(domain: PostgrescdcDomain, table: str) -> None:
    """A vacuum of `table`'s dead rows writes WAL but commits no
    transaction, so logical decoding has nothing to send for it: only the
    server's keepalives report the position past it."""
    engine = create_engine(domain.sqlalchemy_uri, isolation_level="AUTOCOMMIT")
    try:
        with engine.connect() as conn:
            conn.execute(text(f'VACUUM "{table}"'))
    finally:
        engine.dispose()


def _leave_dead_rows(probe) -> None:
    for identifier in range(1, 51):
        probe.insert(identifier, f"dead-{identifier}")
    for identifier in range(1, 51):
        probe.delete(identifier)


@postgrescdc_domain("postgrescdc_employees_xs")
def test_the_recorded_position_keeps_up_with_the_slot_moving_by_itself(
    domain_postgrescdc_employees_xs: PostgrescdcDomain, tmp_path: Path
):
    """psycopg2 confirms WAL that holds nothing for the stream by itself, on
    the server's keepalives, moving the slot past what was staged. The stream
    reads the move back and records it, so a clean stop leaves its state
    where the slot is, and the restart resumes instead of failing with
    POSTGRES_25 over a gap that holds nothing."""
    domain = domain_postgrescdc_employees_xs
    with (
        cdc_probe(domain, "td_il_moves") as probe,
        cdc_probe(domain, "td_il_moves_elsewhere") as elsewhere,
    ):
        probe.insert(1, "row-1")
        _leave_dead_rows(elsewhere)

        with replication_slot(domain, "td_il_moves_slot") as slot:
            src = _stream_src([cdc_table(probe.name)], slot)
            first = _CdcCtx(work_dir(tmp_path, "first"))
            with _running(first, cdc_src_conn(domain), src):
                _await(first, "the snapshot", lambda f: _loaded_ids(f) == [1])
                staged = _state_lsn(first)
                _write_wal_with_no_changes(domain, elsewhere.name)
                moved = _until(
                    "move of the slot past the load by itself",
                    lambda: max(_slot_confirmed(domain, slot) - staged, 0),
                )
                _until(
                    "record of the slot's move",
                    lambda: _state_lsn(first) >= staged + moved,
                )
            carried = first.persisted_state()
            stopped_at = _slot_confirmed(domain, slot)

            probe.insert(2, "while-stopped")

            second = _CdcCtx(work_dir(tmp_path, "second"), carry_over=carried)
            with _running(second, cdc_src_conn(domain), src):
                frame = _await(
                    second, "the row written while stopped", lambda f: 2 in _ids(f)
                )

    assert _captured_ids(_rows(first)) == []
    assert LsnOffset.read(carried["cdc.offset"]).lsn == stopped_at
    assert _loaded_ids(frame) == []
    assert _captured_ids(frame) == [2]


def _advance_slot(domain: PostgrescdcDomain, slot: str) -> int:
    engine = create_engine(domain.sqlalchemy_uri, isolation_level="AUTOCOMMIT")
    try:
        with engine.connect() as conn:
            conn.execute(
                text("select pg_replication_slot_advance(:slot, pg_current_wal_lsn())"),
                {"slot": slot},
            )
    finally:
        engine.dispose()
    return _slot_confirmed(domain, slot)


@postgrescdc_domain("postgrescdc_employees_xs")
def test_a_gap_made_by_hand_fails_until_the_configuration_accepts_it(
    domain_postgrescdc_employees_xs: PostgrescdcDomain, tmp_path: Path
):
    """A slot advanced by hand while the stream is stopped skips a change:
    the resume fails naming the gap, and resumes once
    ``postgres.cdc.accept_gap`` accepts that gap, with the change in it lost
    as the failure warned."""
    domain = domain_postgrescdc_employees_xs
    with cdc_probe(domain, "td_il_gap") as probe:
        probe.insert(1, "row-1")

        with replication_slot(domain, "td_il_gap_slot") as slot:
            src = _stream_src([cdc_table(probe.name)], slot)
            first = _CdcCtx(work_dir(tmp_path, "first"))
            with _running(first, cdc_src_conn(domain), src):
                _await(first, "the snapshot", lambda f: _loaded_ids(f) == [1])
            carried = first.persisted_state()

            probe.insert(2, "skipped")
            advanced = _advance_slot(domain, slot)
            gap = gap_to_text(LsnOffset.read(carried["cdc.offset"]).lsn, advanced)

            refused = _CdcCtx(work_dir(tmp_path, "refused"), carry_over=carried)
            with pytest.raises(PostgresRuntimeException) as ei:
                with _running(refused, cdc_src_conn(domain), src):
                    _until("refusal to resume", lambda: not refused.active)

            accepting = _CdcCtx(work_dir(tmp_path, "accepting"), carry_over=carried)
            accepting_src = _stream_src(
                [cdc_table(probe.name)], slot, **{"postgres.cdc.accept_gap": gap}
            )
            with _running(accepting, cdc_src_conn(domain), accepting_src):
                probe.insert(3, "after")
                frame = _await(
                    accepting, "the row after the gap", lambda f: 3 in _ids(f)
                )

    assert ei.value.api_error is PostgresErrorCode.POSTGRES_25
    assert f"'{gap}'" in str(ei.value)
    assert _captured_ids(frame) == [3]
