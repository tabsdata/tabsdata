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
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import patch

import polars as pl
import pyarrow.parquet as pq
import pytest
from tabsdatax.itest.fixture.backends.db2cdc import Db2CdcDomain, db2cdc_domain
from testi_tabsdatak.conn.db2.backend import (
    cdc_delete,
    cdc_insert,
    cdc_source_table,
    cdc_src_conn,
    cdc_update,
    cdc_wait_cd_count,
)

from tabsdatak.conn.db2 import Db2CdcStreamSrc, Db2CdcStreamSrcConn

# noinspection PyProtectedMember
from tabsdatak.conn.db2._cdc_plugin import Db2CdcStreamSrcPlugin
from tabsdatak.spi import StreamSrcPluginCtx

pytestmark = [
    pytest.mark.db2,
    pytest.mark.db2_seq,
    pytest.mark.xdist_group("db2cdc_seq"),
]

OP_COLUMN_SUFFIX = "cdc.meta.op"
SEG_COLUMN_SUFFIX = "cdc.meta.seg"
INSERT, UPDATE, DELETE, LOAD = "i", "u", "d", "l"

INITIAL_LOAD_SEG = -1

_SRC_CFG = {
    "tabsdata.cdc.buffer.max_rows": 1,
    "tabsdata.cdc.buffer.max_secs": 1,
    "tabsdata.cdc.poll_interval_seconds": 1,
    "db2.cdc.blocking_timeout_seconds": 3,
}


@dataclass
class _StagedManifest:
    manifest: list
    committed: bool = False


class _StageFailed(RuntimeError):
    pass


class _CdcCtx(StreamSrcPluginCtx):
    def __init__(
        self,
        work_dir: Path,
        *,
        active_polls: int,
        initial_state: dict | None = None,
        fail_on_stage: bool = False,
    ) -> None:
        self._work_dir = work_dir
        self._active_left = active_polls
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
        with self._lock:
            if self._active_left <= 0:
                return False
            self._active_left -= 1
            return True

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
    def flush_interval_mins(self) -> int:
        return 0

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

    def persisted_state(self) -> dict:
        with self._lock:
            return dict(self._state)


def _src(tables: list[str], **cfg) -> Db2CdcStreamSrc:
    return Db2CdcStreamSrc(tables=tables, initial_load=True, src_cfg=_SRC_CFG | cfg)


@contextmanager
def _patched_signal_handlers():
    def _fake() -> threading.Event:
        return threading.Event()

    with patch(
        "tabsdatak.conn.common.sql.cdc._runner._setup_signal_handlers",
        side_effect=_fake,
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
        pytest.fail(f"the plugin did not exit within {timeout_s:.0f}s")
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

        ctx = _CdcCtx(tmp_path / "work", active_polls=12)
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

        ctx = _CdcCtx(tmp_path / "work", active_polls=30)
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

        first = _CdcCtx(tmp_path / "run1", active_polls=12)
        first.work_dir.mkdir()
        _run_plugin(first, cdc_src_conn(domain), _src([cd_table]))

        assert _loaded_ids(_frame(first)) == {1}
        carried = first.persisted_state()
        assert carried.get("cdc.initial-load") is False

        cdc_insert(domain, table, [{"id": 2, "name": "while-stopped", "age": 2}])
        cdc_wait_cd_count(domain, cd_table, expected=2)

        second = _CdcCtx(tmp_path / "run2", active_polls=12, initial_state=carried)
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

        ctx = _CdcCtx(tmp_path / "work", active_polls=12)
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

            ctx = _CdcCtx(tmp_path / "work", active_polls=12)
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

        ctx = _CdcCtx(tmp_path / "work", active_polls=12)
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

        first = _CdcCtx(tmp_path / "run1", active_polls=12)
        first.work_dir.mkdir()
        _run_plugin(first, cdc_src_conn(domain), _src([cd_table]))
        assert _loaded_ids(_frame(first)) == {1, 2}
        carried = first.persisted_state()

        cdc_insert(domain, table, [{"id": 3, "name": "added-while-stopped", "age": 3}])
        cdc_wait_cd_count(domain, cd_table, expected=1)

        reset = dict(carried) | {"cdc.initial-load": True}
        second = _CdcCtx(tmp_path / "run2", active_polls=12, initial_state=reset)
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

        first = _CdcCtx(tmp_path / "run1", active_polls=12)
        first.work_dir.mkdir()
        _run_plugin(first, cdc_src_conn(domain), _src([cd_table]))
        assert _loaded_ids(_frame(first)) == {1}
        carried = first.persisted_state()

        cdc_insert(domain, table, [{"id": 2, "name": "between-runs", "age": 2}])
        cdc_wait_cd_count(domain, cd_table, expected=1)

        reset = dict(carried) | {"cdc.initial-load": True}
        second = _CdcCtx(tmp_path / "run2", active_polls=12, initial_state=reset)
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
            active_polls=12,
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

        aborted = _CdcCtx(tmp_path / "aborted", active_polls=12, fail_on_stage=True)
        aborted.work_dir.mkdir()
        with pytest.raises(_StageFailed):
            _run_plugin(aborted, cdc_src_conn(domain), _src([cd_table]))

        # The load stages as one call, so a failure there leaves no state and
        # the flag stays owed.
        assert aborted.persisted_state() == {}

        retry = _CdcCtx(tmp_path / "retry", active_polls=12)
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

        first = _CdcCtx(tmp_path / "run1", active_polls=12)
        first.work_dir.mkdir()
        _run_plugin(first, cdc_src_conn(domain), _src([cd_table]))
        assert _loaded_ids(_frame(first)) == {1}
        state = first.persisted_state()

        cdc_insert(domain, table, [{"id": 2, "name": "between-one-and-two", "age": 2}])
        cdc_wait_cd_count(domain, cd_table, expected=1)

        second = _CdcCtx(tmp_path / "run2", active_polls=12, initial_state=state)
        second.work_dir.mkdir()
        _run_plugin(second, cdc_src_conn(domain), _src([cd_table]))
        second_frame = _frame(second)
        state = second.persisted_state()

        cdc_insert(
            domain, table, [{"id": 3, "name": "between-two-and-three", "age": 3}]
        )
        cdc_wait_cd_count(domain, cd_table, expected=2)

        third = _CdcCtx(tmp_path / "run3", active_polls=12, initial_state=state)
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

        first = _CdcCtx(tmp_path / "run1", active_polls=12)
        first.work_dir.mkdir()
        _run_plugin(first, cdc_src_conn(domain), _src([cd_table]))
        assert _loaded_ids(_frame(first)) == {1, 2}
        state = first.persisted_state()

        cdc_update(domain, table, 1, "updated-while-stopped", 11)
        cdc_delete(domain, table, 2)
        cdc_wait_cd_count(domain, cd_table, expected=2)

        second = _CdcCtx(tmp_path / "run2", active_polls=18, initial_state=state)
        second.work_dir.mkdir()
        _run_plugin(second, cdc_src_conn(domain), _src([cd_table]))

        frame = _frame(second)
        assert _loaded_ids(frame) == set()
        assert {UPDATE, DELETE} <= set(_ops(frame))
        assert _captured_ids(frame) == {1, 2}


@db2cdc_domain("db2cdc_employees_xs")
def test_a_table_added_after_the_load_is_not_back_filled(
    domain_db2cdc_employees_xs: Db2CdcDomain, tmp_path: Path
):
    domain = domain_db2cdc_employees_xs
    with cdc_source_table(domain) as (first_table, first_cd):
        with cdc_source_table(domain) as (second_table, second_cd):
            cdc_insert(domain, first_table, [{"id": 1, "name": "tracked", "age": 1}])
            cdc_insert(domain, second_table, [{"id": 1, "name": "untracked", "age": 1}])

            first = _CdcCtx(tmp_path / "run1", active_polls=12)
            first.work_dir.mkdir()
            _run_plugin(first, cdc_src_conn(domain), _src([first_cd]))
            assert _loaded_ids(_frame(first)) == {1}
            state = first.persisted_state()

            cdc_insert(
                domain, second_table, [{"id": 2, "name": "while-tracked", "age": 2}]
            )
            cdc_wait_cd_count(domain, second_cd, expected=2)

            second = _CdcCtx(tmp_path / "run2", active_polls=18, initial_state=state)
            second.work_dir.mkdir()
            _run_plugin(second, cdc_src_conn(domain), _src([first_cd, second_cd]))

            # The load flag is per stream, not per table, so a table added later
            # is picked up from its own start_from and never snapshotted.
            added = _frame(second, slot=1)
            assert _loaded_ids(added) == set()


@db2cdc_domain("db2cdc_employees_xs")
def test_per_table_offsets_advance_independently_after_the_load(
    domain_db2cdc_employees_xs: Db2CdcDomain, tmp_path: Path
):
    domain = domain_db2cdc_employees_xs
    with cdc_source_table(domain) as (first_table, first_cd):
        with cdc_source_table(domain) as (second_table, second_cd):
            cdc_insert(domain, first_table, [{"id": 1, "name": "first", "age": 1}])
            cdc_insert(domain, second_table, [{"id": 1, "name": "second", "age": 1}])

            first = _CdcCtx(tmp_path / "run1", active_polls=12)
            first.work_dir.mkdir()
            _run_plugin(first, cdc_src_conn(domain), _src([first_cd, second_cd]))
            assert _loaded_ids(_frame(first, slot=0)) == {1}
            assert _loaded_ids(_frame(first, slot=1)) == {1}
            state = first.persisted_state()

            cdc_insert(
                domain, second_table, [{"id": 2, "name": "only-second", "age": 2}]
            )
            cdc_wait_cd_count(domain, second_cd, expected=2)

            second = _CdcCtx(tmp_path / "run2", active_polls=18, initial_state=state)
            second.work_dir.mkdir()
            _run_plugin(second, cdc_src_conn(domain), _src([first_cd, second_cd]))

            assert _captured_ids(_frame(second, slot=1)) == {2}
            assert (
                _files(second, slot=0) == []
                or _loaded_ids(_frame(second, slot=0)) == set()
            )
