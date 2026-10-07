#
# Copyright 2026 Tabsdata Inc.
#

"""Db2 CDC initial-load integration tests using fake context.

Db2 has no captured TRUNCATE: IBM SQL Replication does not honour ``truncate
immediate``, so a destination reset is expressed as ``delete from`` and arrives
as regular delete events. There is therefore no truncate test here.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import patch

import polars as pl
import pyarrow.parquet as pq
import pytest
from sqlalchemy import text
from tabsdatax.itest.fixture.backends.db2cdc import Db2CdcDomain, db2cdc_domain
from testi_tabsdatak.conn.db2.backend import (
    cdc_delete,
    cdc_engine,
    cdc_insert,
    cdc_source_table,
    cdc_src_conn,
    cdc_update,
    cdc_wait_cd_count,
)

# noinspection PyProtectedMember
from tabsdatak.api import BatchThresholds, StreamThresholds

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._signal import StopSignal
from tabsdatak.conn.db2 import Db2CdcStreamSrc, Db2CdcStreamSrcConn

# noinspection PyProtectedMember
from tabsdatak.conn.db2._cdc_plugin import Db2CdcStreamSrcPlugin
from tabsdatak.conn.db2.error import Db2RuntimeException
from tabsdatak.spi import StreamSrcPluginCtx

pytestmark = [
    pytest.mark.db2,
    pytest.mark.db2_seq,
    pytest.mark.xdist_group("db2cdc_seq"),
]

OP_COLUMN_SUFFIX = "cdc.meta.op"
SEG_COLUMN_SUFFIX = "cdc.meta.seg"
SCH_COLUMN_SUFFIX = "cdc.meta.sch"
INSERT, UPDATE, DELETE, LOAD = "i", "u", "d", "l"

INITIAL_LOAD_SEG = -1
GRACE_POLLS = 2

_SRC_CFG = {
    "tabsdata.cdc.buffer.max_rows": 1,
    "tabsdata.cdc.buffer.max_secs": 1,
    "tabsdata.cdc.poll.idle_secs": 1,
    "tabsdata.cdc.poll.max_secs": 1,
}


@dataclass
class _StagedManifest:
    manifest: list
    committed: bool = False


class _StageFailed(RuntimeError):
    pass


class _CdcCtx(StreamSrcPluginCtx):
    """``active`` holds the run open until ``until`` sees what the test expects
    staged, and then for ``GRACE_POLLS`` more polls, so whatever should not
    arrive has had its chance to."""

    def __init__(
        self,
        work_dir: Path,
        *,
        until: Callable[["_CdcCtx"], bool],
        initial_state: dict | None = None,
        fail_on_stage: bool = False,
    ) -> None:
        self._work_dir = work_dir
        self._until = until
        self._grace_left = GRACE_POLLS
        self._stop = threading.Event()
        self._state: dict[str, Any] = dict(initial_state or {})
        self._tabsdata_owns_state = bool(initial_state)
        self._resumed = bool(initial_state)
        self._fail_on_stage = fail_on_stage
        self.staged: list[_StagedManifest] = []
        self._lock = threading.Lock()

    @property
    def work_dir(self) -> Path:
        return self._work_dir

    @property
    def exec_plan_id(self) -> str:
        return "db2-initial-load-exec-plan"

    @property
    def trx_id(self) -> str:
        return "db2-initial-load-trx"

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
        if self._stop.is_set():
            return False
        if self._until(self):
            if self._grace_left <= 0:
                return False
            self._grace_left -= 1
        return True

    def stop(self) -> None:
        self._stop.set()

    @property
    def first_stage(self) -> bool:
        with self._lock:
            return not self.staged and not self._resumed

    @property
    def stream_state(self) -> dict | None:
        with self._lock:
            if not self._tabsdata_owns_state:
                return None
            return dict(self._state) if self._state else None

    @property
    def thresholds(self) -> StreamThresholds:
        return StreamThresholds(
            batch=BatchThresholds(min_secs=1, max_secs=1, max_rows=1)
        )

    def stage(self, tables_files, new_state):
        with self._lock:
            if self._fail_on_stage:
                raise _StageFailed("staging refused by the test")
            self.staged.append(
                _StagedManifest(manifest=list(tables_files), committed=True)
            )
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

    def persisted_state(self) -> dict:
        with self._lock:
            return dict(self._state)


def _src(tables: list[str], **cfg) -> Db2CdcStreamSrc:
    return Db2CdcStreamSrc(tables=tables, initial_load=True, src_cfg=_SRC_CFG | cfg)


@contextmanager
def _patched_signal_handlers():
    with patch(
        "tabsdatak.conn.common._stream._signal.StopSignal.install",
        side_effect=StopSignal,
    ):
        yield


def _run_plugin(
    ctx: _CdcCtx,
    conn: Db2CdcStreamSrcConn,
    src: Db2CdcStreamSrc,
    timeout_s: float = 180.0,
) -> None:
    err: list[BaseException] = []

    def _go():
        try:
            with _patched_signal_handlers():
                Db2CdcStreamSrcPlugin().run(ctx=ctx, conn=conn, stream_src=src)
        except BaseException as exc:  # noqa: BLE001 -- re-raised on the main thread
            err.append(exc)

    thread = threading.Thread(target=_go, name="db2-initial-load", daemon=True)
    thread.start()
    thread.join(timeout=timeout_s)
    if thread.is_alive():
        ctx.stop()
        thread.join(timeout=timeout_s)
        pytest.fail(
            f"the plugin did not stage what was expected within {timeout_s:.0f}s"
        )
    if err:
        raise err[0]


def _files(ctx: _CdcCtx, slot: int | None = None) -> list[Path]:
    return [
        Path(path)
        for staged in ctx.staged
        if staged.committed
        for index, table_files in enumerate(staged.manifest)
        if slot is None or slot == index
        for path in table_files
    ]


def _frame(ctx: _CdcCtx, slot: int | None = None) -> pl.DataFrame:
    files = _files(ctx, slot)
    assert files, "the plugin staged no committed manifest files"
    return pl.concat(
        [pl.from_arrow(pq.read_table(f)) for f in files], how="diagonal_relaxed"
    )


def _column(frame: pl.DataFrame, suffix: str) -> str:
    return next(name for name in frame.columns if name.endswith(suffix))


def _ops(frame: pl.DataFrame) -> list[str]:
    return frame[_column(frame, OP_COLUMN_SUFFIX)].to_list()


def _loaded_ids(frame: pl.DataFrame) -> set[int]:
    seg = _column(frame, SEG_COLUMN_SUFFIX)
    loaded = frame.filter(pl.col(seg) == INITIAL_LOAD_SEG)
    return {value for value in loaded["ID"].to_list() if value is not None}


def _captured_ids(frame: pl.DataFrame) -> set[int]:
    seg = _column(frame, SEG_COLUMN_SUFFIX)
    captured = frame.filter(pl.col(seg) != INITIAL_LOAD_SEG)
    return {value for value in captured["ID"].to_list() if value is not None}


def _staged(
    ready: Callable[[pl.DataFrame], bool], slot: int | None = None
) -> Callable[[_CdcCtx], bool]:
    return lambda ctx: bool(_files(ctx, slot)) and ready(_frame(ctx, slot))


def _loaded(ids: set[int], slot: int | None = None) -> Callable[[_CdcCtx], bool]:
    return _staged(lambda frame: _loaded_ids(frame) == ids, slot)


def _captured(ids: set[int], slot: int | None = None) -> Callable[[_CdcCtx], bool]:
    return _staged(lambda frame: ids <= _captured_ids(frame), slot)


@db2cdc_domain("db2cdc_employees_xs")
def test_initial_load_emits_every_existing_row_once(
    domain_db2cdc_employees_xs: Db2CdcDomain, tmp_path: Path
):
    domain = domain_db2cdc_employees_xs
    with cdc_source_table(domain) as (table, cd_table):
        cdc_insert(
            domain,
            table,
            [{"id": i, "name": f"row-{i}", "age": i} for i in range(1, 21)],
        )

        ctx = _CdcCtx(tmp_path / "work", until=_loaded(set(range(1, 21))))
        ctx.work_dir.mkdir()
        _run_plugin(ctx, cdc_src_conn(domain), _src([cd_table]))

        frame = _frame(ctx)
        assert _loaded_ids(frame) == set(range(1, 21))
        assert _ops(frame).count(LOAD) == 1
        assert _ops(frame).count(INSERT) == 20


@db2cdc_domain("db2cdc_employees_xs")
def test_changes_right_after_the_initial_load_are_captured(
    domain_db2cdc_employees_xs: Db2CdcDomain, tmp_path: Path
):
    domain = domain_db2cdc_employees_xs
    with cdc_source_table(domain) as (table, cd_table):
        cdc_insert(domain, table, [{"id": 1, "name": "seed", "age": 1}])

        ctx = _CdcCtx(
            tmp_path / "work",
            until=_staged(
                lambda frame: (
                    {INSERT, UPDATE, DELETE} <= set(_ops(frame))
                    and 2 in _captured_ids(frame)
                )
            ),
        )
        ctx.work_dir.mkdir()

        def _produce_after_attach() -> None:
            cdc_insert(domain, table, [{"id": 2, "name": "after", "age": 2}])
            cdc_update(domain, table, 1, "seed-changed", 11)
            cdc_delete(domain, table, 2)

        threading.Timer(5.0, _produce_after_attach).start()
        _run_plugin(ctx, cdc_src_conn(domain), _src([cd_table]))

        frame = _frame(ctx)
        assert _loaded_ids(frame) == {1}
        assert {INSERT, UPDATE, DELETE} <= set(_ops(frame))
        assert 2 in _captured_ids(frame)


@db2cdc_domain("db2cdc_employees_xs")
def test_restart_after_the_initial_load_does_not_reload(
    domain_db2cdc_employees_xs: Db2CdcDomain, tmp_path: Path
):
    domain = domain_db2cdc_employees_xs
    with cdc_source_table(domain) as (table, cd_table):
        cdc_insert(domain, table, [{"id": 1, "name": "before", "age": 1}])

        first = _CdcCtx(tmp_path / "run1", until=_loaded({1}))
        first.work_dir.mkdir()
        _run_plugin(first, cdc_src_conn(domain), _src([cd_table]))

        assert _loaded_ids(_frame(first)) == {1}
        carried = first.persisted_state()
        assert carried.get("cdc.initial-load") is False

        cdc_insert(domain, table, [{"id": 2, "name": "while-stopped", "age": 2}])
        cdc_wait_cd_count(domain, cd_table, expected=2)

        second = _CdcCtx(tmp_path / "run2", until=_captured({2}), initial_state=carried)
        second.work_dir.mkdir()
        assert second.first_stage is False
        _run_plugin(second, cdc_src_conn(domain), _src([cd_table]))

        frame = _frame(second)
        assert _loaded_ids(frame) == set()
        assert 2 in _captured_ids(frame)


@db2cdc_domain("db2cdc_employees_xs")
def test_initial_load_does_not_lose_rows_across_chunks(
    domain_db2cdc_employees_xs: Db2CdcDomain, tmp_path: Path
):
    domain = domain_db2cdc_employees_xs
    with cdc_source_table(domain) as (table, cd_table):
        cdc_insert(
            domain,
            table,
            [{"id": i, "name": f"seed-{i}", "age": i} for i in range(1, 501)],
        )

        ctx = _CdcCtx(tmp_path / "work", until=_loaded(set(range(1, 501))))
        ctx.work_dir.mkdir()
        _run_plugin(
            ctx,
            cdc_src_conn(domain),
            _src(
                [cd_table],
                **{"tabsdata.cdc.fetch_size": 10, "tabsdata.cdc.chunk_size": 10},
            ),
        )

        frame = _frame(ctx)
        assert _loaded_ids(frame) == set(range(1, 501))


@db2cdc_domain("db2cdc_employees_xs")
def test_initial_load_stages_one_manifest_for_every_tracked_table(
    domain_db2cdc_employees_xs: Db2CdcDomain, tmp_path: Path
):
    domain = domain_db2cdc_employees_xs
    with cdc_source_table(domain) as (first_table, first_cd):
        with cdc_source_table(domain) as (second_table, second_cd):
            rows = [{"id": i, "name": f"row-{i}", "age": i} for i in range(1, 101)]
            cdc_insert(domain, first_table, rows)
            cdc_insert(domain, second_table, rows)

            ctx = _CdcCtx(tmp_path / "work", until=lambda ctx: bool(ctx.staged))
            ctx.work_dir.mkdir()
            _run_plugin(
                ctx,
                cdc_src_conn(domain),
                _src(
                    [first_cd, second_cd],
                    # File rolling is evaluated per chunk handover, so the chunk
                    # size has to be small too or the whole table arrives as one
                    # chunk and writes one file.
                    **{
                        "tabsdata.cdc.buffer.max_rows": 25,
                        "tabsdata.cdc.fetch_size": 25,
                        "tabsdata.cdc.chunk_size": 25,
                    },
                ),
            )

            load_manifest = ctx.staged[0].manifest
            assert len(load_manifest) == 2
            assert all(len(table_files) > 1 for table_files in load_manifest)
            assert _loaded_ids(_frame(ctx, slot=0)) == set(range(1, 101))
            assert _loaded_ids(_frame(ctx, slot=1)) == set(range(1, 101))


@db2cdc_domain("db2cdc_employees_xs")
def test_the_initial_load_state_carries_both_the_flag_and_the_offset(
    domain_db2cdc_employees_xs: Db2CdcDomain, tmp_path: Path
):
    domain = domain_db2cdc_employees_xs
    with cdc_source_table(domain) as (table, cd_table):
        cdc_insert(domain, table, [{"id": 1, "name": "seed", "age": 1}])

        ctx = _CdcCtx(tmp_path / "work", until=lambda ctx: bool(ctx.staged))
        ctx.work_dir.mkdir()
        _run_plugin(ctx, cdc_src_conn(domain), _src([cd_table]))

        staged = ctx.persisted_state()
        assert staged["cdc.initial-load"] is False
        assert "cdc.offset" in staged


@db2cdc_domain("db2cdc_employees_xs")
def test_resetting_the_flag_reloads_every_row_including_the_new_ones(
    domain_db2cdc_employees_xs: Db2CdcDomain, tmp_path: Path
):
    domain = domain_db2cdc_employees_xs
    with cdc_source_table(domain) as (table, cd_table):
        cdc_insert(
            domain,
            table,
            [
                {"id": 1, "name": "first", "age": 1},
                {"id": 2, "name": "second", "age": 2},
            ],
        )

        first = _CdcCtx(tmp_path / "run1", until=_loaded({1, 2}))
        first.work_dir.mkdir()
        _run_plugin(first, cdc_src_conn(domain), _src([cd_table]))
        assert _loaded_ids(_frame(first)) == {1, 2}
        carried = first.persisted_state()

        cdc_insert(domain, table, [{"id": 3, "name": "added-while-stopped", "age": 3}])
        cdc_wait_cd_count(domain, cd_table, expected=3)

        reset = dict(carried) | {"cdc.initial-load": True}
        second = _CdcCtx(
            tmp_path / "run2", until=_loaded({1, 2, 3}), initial_state=reset
        )
        second.work_dir.mkdir()
        _run_plugin(second, cdc_src_conn(domain), _src([cd_table]))

        frame = _frame(second)
        assert _loaded_ids(frame) == {1, 2, 3}
        assert _ops(frame).count(LOAD) == 1
        assert second.persisted_state()["cdc.initial-load"] is False


@db2cdc_domain("db2cdc_employees_xs")
def test_a_reset_reload_does_not_also_replay_the_rows_as_changes(
    domain_db2cdc_employees_xs: Db2CdcDomain, tmp_path: Path
):
    domain = domain_db2cdc_employees_xs
    with cdc_source_table(domain) as (table, cd_table):
        cdc_insert(domain, table, [{"id": 1, "name": "first", "age": 1}])

        first = _CdcCtx(tmp_path / "run1", until=_loaded({1}))
        first.work_dir.mkdir()
        _run_plugin(first, cdc_src_conn(domain), _src([cd_table]))
        assert _loaded_ids(_frame(first)) == {1}
        carried = first.persisted_state()

        cdc_insert(domain, table, [{"id": 2, "name": "between-runs", "age": 2}])
        cdc_wait_cd_count(domain, cd_table, expected=2)

        reset = dict(carried) | {"cdc.initial-load": True}
        second = _CdcCtx(tmp_path / "run2", until=_loaded({1, 2}), initial_state=reset)
        second.work_dir.mkdir()
        _run_plugin(second, cdc_src_conn(domain), _src([cd_table]))

        frame = _frame(second)
        # The reload re-reads row 2 as part of the snapshot, so it must not also
        # arrive as the insert the first run never consumed.
        assert _loaded_ids(frame) == {1, 2}
        assert _captured_ids(frame) == set()


@db2cdc_domain("db2cdc_employees_xs")
def test_a_source_without_initial_load_never_loads_whatever_the_state_says(
    domain_db2cdc_employees_xs: Db2CdcDomain, tmp_path: Path
):
    domain = domain_db2cdc_employees_xs
    with cdc_source_table(domain) as (table, cd_table):
        cdc_insert(domain, table, [{"id": 1, "name": "pre-existing", "age": 1}])
        cdc_wait_cd_count(domain, cd_table, expected=1)

        src = Db2CdcStreamSrc(
            tables=[cd_table], start_from="head", src_cfg=dict(_SRC_CFG)
        )
        ctx = _CdcCtx(
            tmp_path / "work",
            until=_captured({1}),
            initial_state={"cdc.initial-load": True},
        )
        ctx.work_dir.mkdir()
        _run_plugin(ctx, cdc_src_conn(domain), src)

        frame = _frame(ctx)
        assert _loaded_ids(frame) == set()
        assert LOAD not in _ops(frame)


@db2cdc_domain("db2cdc_employees_xs")
def test_a_run_interrupted_before_the_load_stages_redoes_it(
    domain_db2cdc_employees_xs: Db2CdcDomain, tmp_path: Path
):
    domain = domain_db2cdc_employees_xs
    with cdc_source_table(domain) as (table, cd_table):
        cdc_insert(domain, table, [{"id": 1, "name": "seed", "age": 1}])

        aborted = _CdcCtx(tmp_path / "aborted", until=_loaded({1}), fail_on_stage=True)
        aborted.work_dir.mkdir()
        with pytest.raises(_StageFailed):
            _run_plugin(aborted, cdc_src_conn(domain), _src([cd_table]))

        # The load stages as one call, so a failure there leaves no state and
        # the flag stays owed.
        assert aborted.persisted_state() == {}

        retry = _CdcCtx(tmp_path / "retry", until=_loaded({1}))
        retry.work_dir.mkdir()
        _run_plugin(retry, cdc_src_conn(domain), _src([cd_table]))

        assert _loaded_ids(_frame(retry)) == {1}
        assert retry.persisted_state()["cdc.initial-load"] is False


@db2cdc_domain("db2cdc_employees_xs")
def test_two_consecutive_restarts_each_resume_without_replaying(
    domain_db2cdc_employees_xs: Db2CdcDomain, tmp_path: Path
):
    domain = domain_db2cdc_employees_xs
    with cdc_source_table(domain) as (table, cd_table):
        cdc_insert(domain, table, [{"id": 1, "name": "seed", "age": 1}])

        first = _CdcCtx(tmp_path / "run1", until=_loaded({1}))
        first.work_dir.mkdir()
        _run_plugin(first, cdc_src_conn(domain), _src([cd_table]))
        assert _loaded_ids(_frame(first)) == {1}
        state = first.persisted_state()

        cdc_insert(domain, table, [{"id": 2, "name": "between-one-and-two", "age": 2}])
        cdc_wait_cd_count(domain, cd_table, expected=2)

        second = _CdcCtx(tmp_path / "run2", until=_captured({2}), initial_state=state)
        second.work_dir.mkdir()
        _run_plugin(second, cdc_src_conn(domain), _src([cd_table]))
        second_frame = _frame(second)
        state = second.persisted_state()

        cdc_insert(
            domain, table, [{"id": 3, "name": "between-two-and-three", "age": 3}]
        )
        cdc_wait_cd_count(domain, cd_table, expected=3)

        third = _CdcCtx(tmp_path / "run3", until=_captured({3}), initial_state=state)
        third.work_dir.mkdir()
        _run_plugin(third, cdc_src_conn(domain), _src([cd_table]))
        third_frame = _frame(third)

        assert _loaded_ids(second_frame) == set()
        assert _captured_ids(second_frame) == {2}
        assert _loaded_ids(third_frame) == set()
        assert _captured_ids(third_frame) == {3}


@db2cdc_domain("db2cdc_employees_xs")
def test_updates_and_deletes_of_loaded_rows_arrive_after_the_load(
    domain_db2cdc_employees_xs: Db2CdcDomain, tmp_path: Path
):
    domain = domain_db2cdc_employees_xs
    with cdc_source_table(domain) as (table, cd_table):
        cdc_insert(
            domain,
            table,
            [
                {"id": 1, "name": "loaded-then-updated", "age": 1},
                {"id": 2, "name": "loaded-then-deleted", "age": 2},
            ],
        )

        first = _CdcCtx(tmp_path / "run1", until=_loaded({1, 2}))
        first.work_dir.mkdir()
        _run_plugin(first, cdc_src_conn(domain), _src([cd_table]))
        assert _loaded_ids(_frame(first)) == {1, 2}
        state = first.persisted_state()

        cdc_update(domain, table, 1, "updated-while-stopped", 11)
        cdc_delete(domain, table, 2)
        cdc_wait_cd_count(domain, cd_table, expected=2)

        second = _CdcCtx(
            tmp_path / "run2",
            until=_staged(
                lambda frame: (
                    {UPDATE, DELETE} <= set(_ops(frame))
                    and _captured_ids(frame) == {1, 2}
                )
            ),
            initial_state=state,
        )
        second.work_dir.mkdir()
        _run_plugin(second, cdc_src_conn(domain), _src([cd_table]))

        frame = _frame(second)
        assert _loaded_ids(frame) == set()
        assert {UPDATE, DELETE} <= set(_ops(frame))
        assert _captured_ids(frame) == {1, 2}


@db2cdc_domain("db2cdc_employees_xs")
def test_a_table_added_after_the_load_fails_until_the_load_is_run_again(
    domain_db2cdc_employees_xs: Db2CdcDomain, tmp_path: Path
):
    """The load flag is per stream, not per table, so a table added later has
    no position the stream can prove its rows continue from: the resume fails,
    and a reload snapshots every tracked table."""
    domain = domain_db2cdc_employees_xs
    with cdc_source_table(domain) as (first_table, first_cd):
        with cdc_source_table(domain) as (second_table, second_cd):
            cdc_insert(domain, first_table, [{"id": 1, "name": "tracked", "age": 1}])
            cdc_insert(domain, second_table, [{"id": 1, "name": "untracked", "age": 1}])

            first = _CdcCtx(tmp_path / "run1", until=_loaded({1}))
            first.work_dir.mkdir()
            _run_plugin(first, cdc_src_conn(domain), _src([first_cd]))
            assert _loaded_ids(_frame(first)) == {1}
            state = first.persisted_state()

            cdc_insert(
                domain, second_table, [{"id": 2, "name": "while-tracked", "age": 2}]
            )
            cdc_wait_cd_count(domain, second_cd, expected=2)

            added = _CdcCtx(
                tmp_path / "run2", until=_loaded({1, 2}, slot=1), initial_state=state
            )
            added.work_dir.mkdir()
            with pytest.raises(Db2RuntimeException, match="DB2_33"):
                _run_plugin(added, cdc_src_conn(domain), _src([first_cd, second_cd]))
            assert added.staged == []

            reset = dict(state) | {"cdc.initial-load": True}
            reload = _CdcCtx(
                tmp_path / "run3",
                until=lambda ctx: (
                    _loaded({1}, slot=0)(ctx) and _loaded({1, 2}, slot=1)(ctx)
                ),
                initial_state=reset,
            )
            reload.work_dir.mkdir()
            _run_plugin(reload, cdc_src_conn(domain), _src([first_cd, second_cd]))

            assert _loaded_ids(_frame(reload, slot=0)) == {1}
            assert _loaded_ids(_frame(reload, slot=1)) == {1, 2}


@db2cdc_domain("db2cdc_employees_xs")
def test_per_table_offsets_advance_independently_after_the_load(
    domain_db2cdc_employees_xs: Db2CdcDomain, tmp_path: Path
):
    domain = domain_db2cdc_employees_xs
    with cdc_source_table(domain) as (first_table, first_cd):
        with cdc_source_table(domain) as (second_table, second_cd):
            cdc_insert(domain, first_table, [{"id": 1, "name": "first", "age": 1}])
            cdc_insert(domain, second_table, [{"id": 1, "name": "second", "age": 1}])

            first = _CdcCtx(
                tmp_path / "run1",
                until=lambda ctx: (
                    _loaded({1}, slot=0)(ctx) and _loaded({1}, slot=1)(ctx)
                ),
            )
            first.work_dir.mkdir()
            _run_plugin(first, cdc_src_conn(domain), _src([first_cd, second_cd]))
            assert _loaded_ids(_frame(first, slot=0)) == {1}
            assert _loaded_ids(_frame(first, slot=1)) == {1}
            state = first.persisted_state()

            cdc_insert(
                domain, second_table, [{"id": 2, "name": "only-second", "age": 2}]
            )
            cdc_wait_cd_count(domain, second_cd, expected=2)

            second = _CdcCtx(
                tmp_path / "run2", until=_captured({2}, slot=1), initial_state=state
            )
            second.work_dir.mkdir()
            _run_plugin(second, cdc_src_conn(domain), _src([first_cd, second_cd]))

            assert _captured_ids(_frame(second, slot=1)) == {2}
            assert (
                _files(second, slot=0) == []
                or _loaded_ids(_frame(second, slot=0)) == set()
            )


# One column per common type, so that a type whose catalog and CD-table
# descriptions normalize differently gives the loaded rows and the captured
# changes two schema hashes.
# No BOOLEAN: SQL Replication cannot register a table with one (ASN1571E).
TYPE_ZOO_COLUMNS = (
    "ID INTEGER NOT NULL PRIMARY KEY, C_SMALL SMALLINT, C_BIG BIGINT, "
    "C_NUM DECIMAL(10, 2), C_REAL REAL, C_DOUBLE DOUBLE, "
    "C_VARCHAR VARCHAR(20), C_CHAR CHAR(5), C_DATE DATE, C_TIME TIME, "
    "C_TS TIMESTAMP, C_VARBINARY VARBINARY(8)"
)
TYPE_ZOO_VALUES = (
    "{id}, 7, 9000000000, 12.34, 1.5, 2.25, 'varchar', 'chr', '2026-01-02', "
    "'03:04:05', '2026-01-02-03.04.05', bx'0001'"
)


def _insert_zoo_row(domain: Db2CdcDomain, table: str, id_: int) -> None:
    engine = cdc_engine(domain)
    try:
        with engine.begin() as conn:
            values = TYPE_ZOO_VALUES.format(id=id_)
            conn.execute(
                text(f"insert into {domain.schema.upper()}.{table} values ({values})")
            )
    finally:
        engine.dispose()


@db2cdc_domain("db2cdc_employees_xs")
def test_loaded_rows_and_changes_share_one_schema_hash(
    domain_db2cdc_employees_xs: Db2CdcDomain, tmp_path: Path
):
    domain = domain_db2cdc_employees_xs
    with cdc_source_table(domain, columns=TYPE_ZOO_COLUMNS) as (table, cd_table):
        _insert_zoo_row(domain, table, 1)

        first = _CdcCtx(tmp_path / "run1", until=_loaded({1}))
        first.work_dir.mkdir()
        _run_plugin(first, cdc_src_conn(domain), _src([cd_table]))

        _insert_zoo_row(domain, table, 2)
        cdc_wait_cd_count(domain, cd_table, expected=2)

        state = first.persisted_state()
        second = _CdcCtx(tmp_path / "run2", until=_captured({2}), initial_state=state)
        second.work_dir.mkdir()
        _run_plugin(second, cdc_src_conn(domain), _src([cd_table]))

    hashes = set(_frame(first)[_column(_frame(first), SCH_COLUMN_SUFFIX)].to_list())
    hashes |= set(_frame(second)[_column(_frame(second), SCH_COLUMN_SUFFIX)].to_list())
    assert len(hashes) == 1, (
        f"the load and the stream normalized the schema differently: {hashes}"
    )


def _replay_strictly(ctxs: list[_CdcCtx], slot: int | None = None) -> dict[int, tuple]:
    """Apply every change `ctxs` staged for `slot` (every slot when none), in
    the order they staged it, the way a strict consumer does: an insert on
    an existing row, an update or delete of a missing one, or a key change
    onto a held key is a duplicate or a gap and fails."""
    rows: dict[int, tuple] = {}
    for ctx in ctxs:
        for staged in ctx.staged:
            for index, files in enumerate(staged.manifest):
                if slot is not None and index != slot:
                    continue
                for path in files:
                    frame = pl.from_arrow(pq.read_table(path))
                    op_column = _column(frame, OP_COLUMN_SUFFIX)
                    old_id = _column(frame, "old.ID")
                    for change in frame.iter_rows(named=True):
                        op, key = change[op_column], change["ID"]
                        old = change[old_id]
                        if op == LOAD:
                            rows.clear()
                        elif op == INSERT:
                            assert key not in rows, f"insert of existing row {key}"
                            rows[key] = (change["NAME"], change["AGE"])
                        elif op == UPDATE and old is not None and old != key:
                            rows.pop(old, None)
                            assert key not in rows, f"key change onto held row {key}"
                            rows[key] = (change["NAME"], change["AGE"])
                        elif op == UPDATE:
                            assert key in rows, f"update of missing row {key}"
                            rows[key] = (change["NAME"], change["AGE"])
                        elif op == DELETE:
                            assert key in rows, f"delete of missing row {key}"
                            del rows[key]
    return rows


def _source_rows(domain: Db2CdcDomain, table: str) -> dict[int, tuple]:
    engine = cdc_engine(domain)
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(f"select ID, NAME, AGE from {domain.schema.upper()}.{table}")
            )
            return {row[0]: (row[1], row[2]) for row in result}
    finally:
        engine.dispose()


@db2cdc_domain("db2cdc_employees_xs")
def test_changes_during_the_initial_load_are_applied_exactly_once(
    domain_db2cdc_employees_xs: Db2CdcDomain, tmp_path: Path
):
    """Updates, inserts and deletes committed while the load reads the table
    are in what it loaded, in the change stream, or both: the stage carries
    each of them exactly once, so a consumer that refuses duplicates and
    gaps ends at the table's contents."""
    domain = domain_db2cdc_employees_xs
    seeded = 2000
    with cdc_source_table(domain) as (table, cd_table):
        cdc_insert(
            domain,
            table,
            [{"id": i, "name": f"seed-{i}", "age": i} for i in range(1, seeded + 1)],
        )
        stop = threading.Event()

        def churn() -> None:
            # Several changes per row: updated, updated to the same contents,
            # back to its seeded contents, deleted and reinserted with the
            # same or other contents; and rows born, changed and killed.
            step = 0
            while not stop.is_set():
                step += 1
                key = step % seeded + 1
                cdc_update(domain, table, key, f"upd-{step}", step)
                cdc_update(domain, table, key, f"upd-{step}", step)
                cdc_update(domain, table, key, f"seed-{key}", key)
                born = seeded + step
                cdc_insert(domain, table, [{"id": born, "name": "new", "age": step}])
                cdc_update(domain, table, born, "changed", step)
                if step % 2 == 0:
                    cdc_delete(domain, table, born)
                if step % 3 == 0:
                    cdc_delete(domain, table, key)
                    cdc_insert(
                        domain, table, [{"id": key, "name": f"seed-{key}", "age": key}]
                    )
                if step % 5 == 0:
                    cdc_delete(domain, table, key)
                    cdc_insert(
                        domain, table, [{"id": key, "name": f"re-{step}", "age": step}]
                    )

        churner = threading.Thread(target=churn, name="db2-churn", daemon=True)
        churner.start()
        try:
            first = _CdcCtx(tmp_path / "run1", until=lambda ctx: bool(ctx.staged))
            first.work_dir.mkdir()
            _run_plugin(
                first,
                cdc_src_conn(domain),
                _src([cd_table], **{"tabsdata.cdc.fetch_size": 5}),
            )
        finally:
            stop.set()
            churner.join(timeout=60)

        expected = _source_rows(domain, table)
        second = _CdcCtx(
            tmp_path / "run2",
            until=lambda ctx: _replay_strictly([first, ctx]) == expected,
            initial_state=first.persisted_state(),
        )
        second.work_dir.mkdir()
        _run_plugin(second, cdc_src_conn(domain), _src([cd_table]))

    assert _replay_strictly([first, second]) == expected


# ---- changes at chosen moments of the load ------------------------------------
#
# The load reads the tracked tables one after another. Changes committed after
# it read the first table and before it reads the second are, for the first
# table, changes it has not loaded, and for the second, changes it has: so the
# changes the deduplication keeps are known exactly, on a real change stream.


@contextmanager
def _after_loading_the_first_table(changes: Callable[[], None]):
    from tabsdatak.conn.common.sql.cdc import _initial_load

    load_table = _initial_load.InitialLoader._load_table
    loaded: list[str] = []

    def load_then_change(self, tracked_table, connection, timestamp):
        load_table(self, tracked_table, connection, timestamp)
        loaded.append(tracked_table.qualified)
        if len(loaded) == 1:
            changes()

    with patch.object(_initial_load.InitialLoader, "_load_table", load_then_change):
        yield


def _move_key(domain: Db2CdcDomain, table: str, old: int, new: int) -> None:
    engine = cdc_engine(domain)
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    f"update {domain.schema.upper()}.{table} set id = :new "
                    f"where id = :old"
                ),
                {"new": new, "old": old},
            )
    finally:
        engine.dispose()


def _kept_changes(ctx: _CdcCtx, slot: int) -> list[tuple]:
    """The changes the load's stage carries for `slot`, besides its rows."""
    staged = ctx.staged[0]
    frames = [pl.from_arrow(pq.read_table(path)) for path in staged.manifest[slot]]
    frame = pl.concat(frames, how="diagonal_relaxed")
    seg, op = _column(frame, SEG_COLUMN_SUFFIX), _column(frame, OP_COLUMN_SUFFIX)
    window = frame.filter(pl.col(seg) != INITIAL_LOAD_SEG)
    return [(row[op], row["ID"], row["NAME"]) for row in window.iter_rows(named=True)]


SEEDED = [{"id": i, "name": f"seed-{i}", "age": i} for i in (1, 2, 3)]


def _changes_to(domain: Db2CdcDomain, table: str) -> None:
    cdc_update(domain, table, 1, "upd", 10)
    cdc_delete(domain, table, 2)
    cdc_insert(domain, table, [{"id": 4, "name": "new", "age": 4}])
    cdc_update(domain, table, 3, "flip", 30)
    cdc_update(domain, table, 3, "seed-3", 3)


@db2cdc_domain("db2cdc_employees_xs")
def test_changes_after_a_table_was_loaded_are_kept_and_before_dropped(
    domain_db2cdc_employees_xs: Db2CdcDomain, tmp_path: Path
):
    """The same changes, committed between reading the first table and the
    second: the first loaded none of them, so all apply; the second loaded
    all of them, so only the update that flips a row away from what it
    loaded and back applies, and every other change is dropped."""
    domain = domain_db2cdc_employees_xs
    with cdc_source_table(domain) as (first, first_cd):
        with cdc_source_table(domain) as (second, second_cd):
            cdc_insert(domain, first, SEEDED)
            cdc_insert(domain, second, SEEDED)

            def changes() -> None:
                _changes_to(domain, first)
                _changes_to(domain, second)
                _move_key(domain, first, 1, 9)

            ctx = _CdcCtx(tmp_path / "work", until=lambda ctx: bool(ctx.staged))
            ctx.work_dir.mkdir()
            with _after_loading_the_first_table(changes):
                _run_plugin(ctx, cdc_src_conn(domain), _src([first_cd, second_cd]))

            assert _kept_changes(ctx, 0) == [
                (UPDATE, 1, "upd"),
                (DELETE, 2, "seed-2"),
                (INSERT, 4, "new"),
                (UPDATE, 3, "flip"),
                (UPDATE, 3, "seed-3"),
                (UPDATE, 9, "upd"),
            ]
            assert _kept_changes(ctx, 1) == [
                (UPDATE, 3, "flip"),
                (UPDATE, 3, "seed-3"),
            ]
            assert _replay_strictly([ctx], slot=0) == _source_rows(domain, first)
            assert _replay_strictly([ctx], slot=1) == _source_rows(domain, second)


@db2cdc_domain("db2cdc_employees_xs")
def test_a_key_taken_again_after_its_row_moved_fails_the_load_safely(
    domain_db2cdc_employees_xs: Db2CdcDomain, tmp_path: Path
):
    """Before the second table is read, its row 1 moves to key 9 and a new
    row takes key 1: the load reads both keys held, so when the key change
    comes up it cannot tell this from a row read twice, and fails."""
    from tabsdatak.conn.common.sql.cdc.error import SqlCdcException

    domain = domain_db2cdc_employees_xs
    with cdc_source_table(domain) as (first, first_cd):
        with cdc_source_table(domain) as (second, second_cd):
            cdc_insert(domain, first, SEEDED)
            cdc_insert(domain, second, SEEDED)

            def changes() -> None:
                _move_key(domain, second, 1, 9)
                cdc_insert(domain, second, [{"id": 1, "name": "other", "age": 1}])

            ctx = _CdcCtx(tmp_path / "work", until=lambda ctx: bool(ctx.staged))
            ctx.work_dir.mkdir()
            with _after_loading_the_first_table(changes):
                with pytest.raises(SqlCdcException, match="CDC_23"):
                    _run_plugin(ctx, cdc_src_conn(domain), _src([first_cd, second_cd]))
            assert ctx.staged == []
