#
# Copyright 2026 Tabsdata Inc.
#

"""Db2 CDC stream-source plugin integration tests using fake context.

Real IBM SQL Replication throughout: the ``db2cdc`` domain provides archive
logging, the ``ASN.IBMSNAP_*`` control tables and a running ``asncap``, and
registers each test's source table on request. Tests drive plain INSERT /
UPDATE / DELETE against that table and asncap propagates them into the CD
table, so every read polls -- propagation is asynchronous and takes seconds
to tens of seconds.
"""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import patch

import polars as pl
import pyarrow.parquet as pq
import pytest
from tabsdatax.itest.fixture.backends.db2cdc import Db2CdcDomain, db2cdc_domain
from testi_tabsdatak.conn.common.sql.cdc_interleave import (
    INTERLEAVED_IDS,
    OpenTransaction,
    first_stages,
    interleave,
    publish_in_order,
)
from testi_tabsdatak.conn.db2.backend import (
    cdc_commitseqs,
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
from tabsdatak.conn.db2 import (
    CommitSeqPosition,
    Db2CdcStreamSrc,
    Db2CdcStreamSrcConn,
)

# noinspection PyProtectedMember
from tabsdatak.conn.db2._cdc_plugin import Db2CdcStreamSrcPlugin
from tabsdatak.spi import StreamSrcPluginCtx

pytestmark = [
    pytest.mark.db2,
    pytest.mark.db2_seq,
    pytest.mark.xdist_group("db2cdc_seq"),
]

GRACE_POLLS = 2


@dataclass
class _StagedManifest:
    manifest: list
    committed: bool = False


class _CdcCtx(StreamSrcPluginCtx):
    """The trx scratch the plugin stages into.

    ``active`` holds the run open until ``until`` sees what the test expects
    staged, and then for ``GRACE_POLLS`` more polls, so whatever should not
    arrive has had its chance to; without ``until``, until the test calls
    ``stop``. ``stage`` records what was staged plus whichever offset the
    ingester handed over.
    """

    def __init__(
        self,
        work_dir: Path,
        *,
        until: Callable[["_CdcCtx"], bool] | None = None,
        initial_state: dict | None = None,
    ) -> None:
        self._work_dir = work_dir
        self._until = until
        self._grace_left = GRACE_POLLS
        self._stop = threading.Event()
        self._state: dict[str, Any] = dict(initial_state or {})
        self._tabsdata_owns_state = bool(initial_state)
        self.staged: list[_StagedManifest] = []
        self._lock = threading.Lock()

    @property
    def work_dir(self) -> Path:
        return self._work_dir

    @property
    def exec_plan_id(self) -> str:
        return "db2-cdc-integ-exec-plan"

    @property
    def trx_id(self) -> str:
        return "db2-cdc-integ-trx"

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
        if self._until is not None and self._until(self):
            if self._grace_left <= 0:
                return False
            self._grace_left -= 1
        return True

    def stop(self) -> None:
        self._stop.set()

    @property
    def first_stage(self) -> bool:
        with self._lock:
            return not self.staged and not self._state

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
            self.staged.append(
                _StagedManifest(manifest=list(tables_files), committed=True)
            )
            # A dict means Tabsdata owns the offset and persists it; a callable
            # means the external system does, and is invoked for its side effect.
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


_SRC_CFG = {
    "tabsdata.cdc.buffer.max_rows": 1,
    "tabsdata.cdc.buffer.max_secs": 1,
    "tabsdata.cdc.poll.idle_secs": 1,
    "tabsdata.cdc.poll.max_secs": 1,
}


def _src(cd_table: str, *, start_from: Any = "head") -> Db2CdcStreamSrc:
    return Db2CdcStreamSrc(
        tables=[cd_table], start_from=start_from, src_cfg=dict(_SRC_CFG)
    )


@contextmanager
def _patched_signal_handlers():
    """``run_cdc`` installs SIGINT / SIGTERM handlers, which only works on the
    main thread."""

    with patch(
        "tabsdatak.conn.common._stream._signal.StopSignal.install",
        side_effect=StopSignal,
    ):
        yield


def _run_plugin(
    ctx: _CdcCtx,
    conn: Db2CdcStreamSrcConn,
    src: Db2CdcStreamSrc,
    timeout_s: float = 120.0,
) -> None:
    err: list[BaseException] = []

    def _go():
        try:
            with _patched_signal_handlers():
                Db2CdcStreamSrcPlugin().run(ctx=ctx, conn=conn, stream_src=src)
        except BaseException as exc:  # noqa: BLE001 -- re-raised on the main thread
            err.append(exc)

    thread = threading.Thread(target=_go, name="db2-cdc-plugin", daemon=True)
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


def _staged_files(ctx: _CdcCtx) -> list[Path]:
    return [
        Path(path)
        for staged in ctx.staged
        if staged.committed
        for table_files in staged.manifest
        for path in table_files
    ]


def _staged_frame(ctx: _CdcCtx) -> pl.DataFrame:
    files = _staged_files(ctx)
    assert files, "the plugin staged no committed manifest files"
    return pl.concat([pl.from_arrow(pq.read_table(f)) for f in files])


def _staged_ids(ctx: _CdcCtx) -> set[int]:
    ids: set[int] = set()
    for f in _staged_files(ctx):
        frame = pl.from_arrow(pq.read_table(f))
        if "ID" in frame.columns:
            ids.update(v for v in frame["ID"].to_list() if v is not None)
    return ids


def _staged(ids: set[int]) -> Callable[[_CdcCtx], bool]:
    return lambda ctx: ids <= _staged_ids(ctx)


def _staged_ops(ops: set[str]) -> Callable[[_CdcCtx], bool]:
    def _ready(ctx: _CdcCtx) -> bool:
        if not _staged_files(ctx):
            return False
        frame = _staged_frame(ctx)
        op_column = next(c for c in frame.columns if c.endswith("cdc.meta.op"))
        return ops <= set(frame[op_column].to_list())

    return _ready


@db2cdc_domain("db2cdc_employees_xs")
def test_cdc_insert_event_stages_a_committed_manifest(
    domain_db2cdc_employees_xs: Db2CdcDomain, tmp_path: Path
):
    domain = domain_db2cdc_employees_xs
    with cdc_source_table(domain) as (table, cd_table):
        cdc_insert(domain, table, [{"id": 1, "name": "alice", "age": 30}])
        cdc_wait_cd_count(domain, cd_table, expected=1)

        ctx = _CdcCtx(tmp_path / "work", until=_staged({1}))
        ctx.work_dir.mkdir()
        _run_plugin(ctx, cdc_src_conn(domain), _src(cd_table))

        frame = _staged_frame(ctx)
        # The default columns_format="columns" + flatten_columns inlines the
        # after-image columns at top level.
        assert set(frame.columns) >= {"ID", "NAME", "AGE"}
        assert 1 in frame["ID"].to_list()


@db2cdc_domain("db2cdc_employees_xs")
def test_cdc_insert_update_delete_events_surface(
    domain_db2cdc_employees_xs: Db2CdcDomain, tmp_path: Path
):
    domain = domain_db2cdc_employees_xs
    with cdc_source_table(domain) as (table, cd_table):
        cdc_insert(domain, table, [{"id": 7, "name": "carol", "age": 40}])
        cdc_update(domain, table, 7, "carol-2", 41)
        cdc_delete(domain, table, 7)
        # The registration declares `image both`, so an UPDATE can arrive as two
        # CD rows; three is the floor either way.
        cdc_wait_cd_count(domain, cd_table, expected=3)

        ctx = _CdcCtx(tmp_path / "work", until=_staged_ops({"i", "u", "d"}))
        ctx.work_dir.mkdir()
        _run_plugin(ctx, cdc_src_conn(domain), _src(cd_table))

        frame = _staged_frame(ctx)
        op_column = next(c for c in frame.columns if c.endswith("cdc.meta.op"))
        assert {"i", "u", "d"} <= set(frame[op_column].to_list())


@db2cdc_domain("db2cdc_employees_xs")
def test_cdc_tail_skips_pre_existing_events(
    domain_db2cdc_employees_xs: Db2CdcDomain, tmp_path: Path
):
    domain = domain_db2cdc_employees_xs
    with cdc_source_table(domain) as (table, cd_table):
        cdc_insert(
            domain,
            table,
            [
                {"id": 100, "name": "old-a", "age": 1},
                {"id": 101, "name": "old-b", "age": 2},
            ],
        )
        cdc_wait_cd_count(domain, cd_table, expected=2)

        # `tail` anchors past whatever is already in the CD table, so only what
        # is produced after the stream attaches should surface.
        ctx = _CdcCtx(tmp_path / "work", until=_staged({200}))
        ctx.work_dir.mkdir()

        def _produce_after_attach() -> None:
            cdc_insert(domain, table, [{"id": 200, "name": "new", "age": 3}])

        threading.Timer(3.0, _produce_after_attach).start()
        _run_plugin(ctx, cdc_src_conn(domain), _src(cd_table, start_from="tail"))

        ids = _staged_ids(ctx)
        assert 200 in ids
        assert 100 not in ids
        assert 101 not in ids


@db2cdc_domain("db2cdc_employees_xs")
def test_cdc_resumes_from_commitseq_position(
    domain_db2cdc_employees_xs: Db2CdcDomain, tmp_path: Path
):
    domain = domain_db2cdc_employees_xs
    with cdc_source_table(domain) as (table, cd_table):
        for id_, name in ((50, "five"), (60, "six"), (70, "seven")):
            cdc_insert(domain, table, [{"id": id_, "name": name, "age": id_}])
        cdc_wait_cd_count(domain, cd_table, expected=3)

        seqs = cdc_commitseqs(domain, cd_table)
        assert len(seqs) >= 3, f"expected 3 CD rows, got {len(seqs)}"
        # Resume just past the second row: the first two are consumed, the third
        # is not.
        resume = seqs[1].hex().upper()

        ctx = _CdcCtx(tmp_path / "work", until=_staged({70}))
        ctx.work_dir.mkdir()
        _run_plugin(
            ctx,
            cdc_src_conn(domain),
            _src(cd_table, start_from=CommitSeqPosition(seq=resume)),
        )

        ids = _staged_ids(ctx)
        assert 70 in ids
        assert 50 not in ids
        assert 60 not in ids


@db2cdc_domain("db2cdc_employees_xs")
def test_cdc_persists_offset_to_ctx_state(
    domain_db2cdc_employees_xs: Db2CdcDomain, tmp_path: Path
):
    domain = domain_db2cdc_employees_xs
    with cdc_source_table(domain) as (table, cd_table):
        cdc_insert(domain, table, [{"id": 11, "name": "eleven", "age": 11}])
        cdc_wait_cd_count(domain, cd_table, expected=1)

        ctx = _CdcCtx(tmp_path / "work", until=_staged({11}))
        ctx.work_dir.mkdir()
        _run_plugin(ctx, cdc_src_conn(domain), _src(cd_table))

        offset = ctx.stream_state
        assert isinstance(offset, dict)
        assert "cdc.offset" in offset
        per_table = json.loads(offset["cdc.offset"])
        assert cd_table in per_table
        # A commitseq is a 16-byte value, carried as 32 hex characters.
        assert isinstance(per_table[cd_table], str)
        assert len(per_table[cd_table]) == 32


@db2cdc_domain("db2cdc_employees_xs")
def test_cdc_resumes_from_persisted_stream_state(
    domain_db2cdc_employees_xs: Db2CdcDomain, tmp_path: Path
):
    """Two runs sharing the Tabsdata-persisted offset.

    Run 1 consumes the first rows and persists its commit-seq map. Run 2 is
    seeded with that state, so although ``start_from='head'`` would replay from
    the oldest CD row on a cold start, the ingester resumes past run 1's
    commit-seq instead.
    """
    domain = domain_db2cdc_employees_xs
    with cdc_source_table(domain) as (table, cd_table):
        for id_, name in ((50, "five"), (60, "six")):
            cdc_insert(domain, table, [{"id": id_, "name": name, "age": id_}])
        cdc_wait_cd_count(domain, cd_table, expected=2)

        first = _CdcCtx(tmp_path / "run1", until=_staged({50, 60}))
        first.work_dir.mkdir()
        _run_plugin(first, cdc_src_conn(domain), _src(cd_table))

        assert {50, 60} <= _staged_ids(first)
        persisted = first.stream_state
        assert persisted is not None and "cdc.offset" in persisted

        cdc_insert(domain, table, [{"id": 70, "name": "seven", "age": 70}])
        cdc_wait_cd_count(domain, cd_table, expected=3)

        second = _CdcCtx(
            tmp_path / "run2", until=_staged({70}), initial_state=persisted
        )
        second.work_dir.mkdir()
        assert second.first_stage is False
        _run_plugin(second, cdc_src_conn(domain), _src(cd_table))

        ids = _staged_ids(second)
        assert 70 in ids, f"the post-resume row should surface, got {ids}"
        assert 50 not in ids, f"a consumed row was replayed, got {ids}"
        assert 60 not in ids, f"a consumed row was replayed, got {ids}"


@contextmanager
def _running(
    ctx: _CdcCtx,
    conn: Db2CdcStreamSrcConn,
    src: Db2CdcStreamSrc,
    exit_timeout_s: float = 120.0,
):
    err: list[BaseException] = []

    def _go():
        try:
            with _patched_signal_handlers():
                Db2CdcStreamSrcPlugin().run(ctx=ctx, conn=conn, stream_src=src)
        except BaseException as exc:  # noqa: BLE001 -- re-raised on the main thread
            err.append(exc)
            ctx.stop()

    thread = threading.Thread(target=_go, name="db2-cdc-plugin", daemon=True)
    thread.start()
    try:
        yield
    finally:
        ctx.stop()
        thread.join(timeout=exit_timeout_s)
        if err:
            raise err[0]
        if thread.is_alive():
            pytest.fail(f"the plugin did not exit within {exit_timeout_s:.0f}s")


def _await_staged(ctx: _CdcCtx, ids: set[int], timeout_s: float = 120.0) -> None:
    """asncap copies a commit into the CD table within its commit interval, and
    the stream stages it within a poll after that."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if ids <= _staged_ids(ctx):
            return
        time.sleep(0.5)
    pytest.fail(
        f"rows {sorted(ids)} were not staged within {timeout_s:.0f}s "
        f"(staged: {sorted(_staged_ids(ctx))})"
    )


@db2cdc_domain("db2cdc_employees_xs")
def test_cdc_publishes_every_row_of_interleaved_transactions(
    domain_db2cdc_employees_xs: Db2CdcDomain, tmp_path: Path
):
    """Transactions that overlap, and one nested inside another, each with a
    stage between the two commits: every row reaches the stage publisher past
    its cursor, so none is dropped."""
    domain = domain_db2cdc_employees_xs
    with cdc_source_table(domain) as (table, cd_table):
        engine = cdc_engine(domain)
        insert = (
            f"insert into {domain.schema.upper()}.{table} (id, name, age) "
            f"values (:id, 'interleaved', 0)"
        )
        ctx = _CdcCtx(tmp_path / "work")
        ctx.work_dir.mkdir()
        try:
            with _running(ctx, cdc_src_conn(domain), _src(cd_table)):
                cases = interleave(
                    lambda: OpenTransaction(engine, insert),
                    lambda ids: _await_staged(ctx, ids),
                )
        finally:
            engine.dispose()

    manifests = [staged.manifest for staged in ctx.staged if staged.committed]
    stages = first_stages(manifests, "ID")
    for case, (earlier, later) in cases.items():
        assert max(stages[i] for i in earlier) < min(stages[i] for i in later), case
    publish = tmp_path / "publish"
    publish.mkdir()
    assert publish_in_order(manifests, publish, "ID") == set(INTERLEAVED_IDS)
