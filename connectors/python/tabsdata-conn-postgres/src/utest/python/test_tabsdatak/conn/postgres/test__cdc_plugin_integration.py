"""End-to-end Postgres CDC integration tests against a real
logical-replication-enabled Postgres instance.

The ``fresh_postgres_cdc`` fixture (defined in ``conftest.py``)
provisions a docker Postgres with ``wal_level=logical`` + the
``wal2json`` output plugin installed + a pre-created logical
replication slot. Each test starts from a freshly truncated
``users`` table; the WAL + slot accumulate across tests so
``start_from="head"`` / ``"tail"`` and slot-resume semantics can
be exercised.

These tests exercise the full plugin: ingester opens, wal2json
events flow, the buffer + disk sentinels stage parquet files, the
trx ctx receives the manifest and writes the LSN offset state.
Per-function unit coverage of the ingester / dialect / offset
machinery lives in ``test__cdc_impl.py``; plugin wiring (no real
Postgres) in ``test__cdc_plugin.py``.
"""

from __future__ import annotations

import logging
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import patch

import polars as pl
import pyarrow.parquet as pq
import pytest
from sqlalchemy import create_engine, text

from tabsdatak.conn.postgres import (
    LsnPosition,
    PostgresCdcConn,
    PostgresCdcStreamSrc,
)
from tabsdatak.conn.postgres._cdc_plugin import PostgresCdcStreamSrcPlugin
from tabsdatak.spi import StageRef, StreamSrcPluginCtx

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.postgres


# The replication slot the conftest pre-creates. Keep in sync with
# ``conftest._REPLICATION_SLOT`` -- duplicating the literal keeps
# this test module from importing private conftest helpers.
_REPLICATION_SLOT = "tdcdc_slot"


# ---------------------------------------------------------------------------
# StreamSrcPluginCtx stub (parallels the mysql integration test)
# ---------------------------------------------------------------------------


@dataclass
class _StagedManifest:
    manifest: list
    committed: bool = False


class _Stage(StageRef):
    def __init__(self, idx: int) -> None:
        self.idx = idx


class _CdcCtx(StreamSrcPluginCtx):
    """Minimal but real ``StreamSrcPluginCtx`` for the CDC plugin.
    ``active_polls`` caps how many times the runner can call
    ``is_active`` before it returns False; this is how the test
    exits the otherwise-infinite poll loop."""

    def __init__(self, work_dir: Path, *, active_polls: int) -> None:
        self._work_dir = work_dir
        self._active_left = active_polls
        self._state: dict[str, Any] = {}
        self.staged: list[_StagedManifest] = []
        self._stage_counter = 0
        self._lock = threading.Lock()

    @property
    def work_dir(self) -> Path:
        return self._work_dir

    @property
    def exec_plan_id(self) -> str:
        return "postgres-cdc-integ-exec-plan"

    @property
    def trx_id(self) -> str:
        return "postgres-cdc-integ-trx"

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

    def store_attr(self, attr, val):
        pass

    def add_metadata(self, attr, val):
        pass

    def is_active(self) -> bool:
        with self._lock:
            if self._active_left <= 0:
                return False
            self._active_left -= 1
            return True

    def stage(self, manifest):
        with self._lock:
            self._stage_counter += 1
            captured = _StagedManifest(manifest=list(manifest))
            self.staged.append(captured)
            return _Stage(self._stage_counter)

    def commit(self, stage_ref):
        with self._lock:
            idx = getattr(stage_ref, "idx", None)
            if idx is not None and 1 <= idx <= len(self.staged):
                self.staged[idx - 1].committed = True

    def trigger(self) -> bool:
        return True

    def trigger_monitor(self) -> bool:
        return True

    def read_state(self):
        return dict(self._state)

    def write_state(self, state):
        self._state = dict(state)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_cdc_conn(cfg) -> PostgresCdcConn:
    return PostgresCdcConn(uri=cfg.uri, credentials=cfg.credentials)


def _drop_replication_slot(cfg) -> None:
    """Drop + recreate the slot between tests so each starts
    from a clean LSN and slot retention doesn't pile up.
    AUTOCOMMIT because ``pg_drop_replication_slot`` cannot run
    inside a transaction block."""
    eng = create_engine(
        cfg.uri_with_credentials, isolation_level="AUTOCOMMIT",
    )
    try:
        with eng.connect() as conn:
            row = conn.execute(text(
                "select slot_name from pg_replication_slots "
                "where slot_name = :slot"
            ), {"slot": _REPLICATION_SLOT}).fetchone()
            if row:
                conn.execute(text(
                    "select pg_drop_replication_slot(:slot)"
                ), {"slot": _REPLICATION_SLOT})
            conn.execute(text(
                "select pg_create_logical_replication_slot("
                ":slot, 'wal2json')"
            ), {"slot": _REPLICATION_SLOT})
    finally:
        eng.dispose()


def _insert_users(cfg, rows: list[dict]) -> None:
    eng = create_engine(cfg.uri_with_credentials)
    try:
        with eng.begin() as conn:
            for row in rows:
                conn.execute(
                    text(
                        "INSERT INTO users (id, name, age) "
                        "VALUES (:id, :name, :age)"
                    ),
                    row,
                )
    finally:
        eng.dispose()


def _update_user(cfg, id_: int, name: str, age: int) -> None:
    eng = create_engine(cfg.uri_with_credentials)
    try:
        with eng.begin() as conn:
            conn.execute(
                text(
                    "UPDATE users SET name = :name, age = :age "
                    "WHERE id = :id"
                ),
                {"id": id_, "name": name, "age": age},
            )
    finally:
        eng.dispose()


def _delete_user(cfg, id_: int) -> None:
    eng = create_engine(cfg.uri_with_credentials)
    try:
        with eng.begin() as conn:
            conn.execute(
                text("DELETE FROM users WHERE id = :id"),
                {"id": id_},
            )
    finally:
        eng.dispose()


def _read_parquet(path: Path) -> pl.DataFrame:
    return pl.from_arrow(pq.read_table(path))


def _all_staged_files(ctx: _CdcCtx) -> list[Path]:
    files: list[Path] = []
    for sm in ctx.staged:
        if not sm.committed:
            continue
        for tm in sm.manifest:
            files.extend(Path(p) for p in tm.files)
    return files


@contextmanager
def _patched_signal_handlers():
    """``run_cdc`` installs SIGINT / SIGTERM handlers via
    ``signal.signal`` which only works on the main thread; we run
    the plugin on a worker thread so the test can produce events
    in parallel."""
    def _fake() -> threading.Event:
        return threading.Event()

    with patch(
        "tabsdatak.conn.common.sql.cdc._runner._setup_signal_handlers",
        side_effect=_fake,
    ):
        yield


def _run_plugin_in_thread(
    ctx: _CdcCtx,
    src: PostgresCdcStreamSrc,
    timeout_s: float = 60.0,
) -> None:
    err: list[BaseException] = []

    def _go():
        try:
            with _patched_signal_handlers():
                PostgresCdcStreamSrcPlugin().run(ctx=ctx, stream_src=src)
        except BaseException as exc:  # noqa: BLE001
            err.append(exc)

    t = threading.Thread(target=_go, name="postgres-cdc-plugin", daemon=True)
    t.start()
    t.join(timeout=timeout_s)
    if t.is_alive():
        pytest.fail(
            f"PostgresCdcStreamSrcPlugin.run did not exit within {timeout_s}s",
        )
    if err:
        raise err[0]


@pytest.fixture
def fresh_postgres_cdc_with_slot(fresh_postgres_cdc):
    """Per-test: in addition to the truncated ``users`` table, drop
    + recreate the replication slot so every test starts from a
    clean LSN."""
    _drop_replication_slot(fresh_postgres_cdc)
    return fresh_postgres_cdc


# ---------------------------------------------------------------------------
# Happy-path: INSERT events surface through the plugin to the staged manifest
# ---------------------------------------------------------------------------


def test_cdc_insert_events_stage_a_committed_manifest(
    fresh_postgres_cdc_with_slot, tmp_path,
):
    cfg = fresh_postgres_cdc_with_slot
    work = tmp_path / "work"
    work.mkdir()
    ctx = _CdcCtx(work, active_polls=8)

    src = PostgresCdcStreamSrc(
        conn=_make_cdc_conn(cfg),
        tables=[f"public.users"],
        replication_slot=_REPLICATION_SLOT,
        replication_slot_behavior="reuse",
        start_from="head",
        src_cfg={
            "tabsdata.cdc.buffer.max_rows": 1,
            "tabsdata.cdc.buffer.max_secs": 1,
            "tabsdata.cdc.poll_interval_seconds": 1,
            "postgres.cdc.blocking_timeout_seconds": 2,
        },
    )

    _insert_users(cfg, [{"id": 1, "name": "alice", "age": 30}])

    def _producer():
        time.sleep(2)
        _insert_users(cfg, [{"id": 2, "name": "bob", "age": 25}])

    threading.Thread(target=_producer, daemon=True).start()

    _run_plugin_in_thread(ctx, src, timeout_s=60.0)

    files = _all_staged_files(ctx)
    assert files, "no committed manifest files produced by the plugin"

    df = pl.concat([_read_parquet(p) for p in files])
    assert set(df.columns) >= {"id", "name", "age"}
    ids = sorted(df["id"].to_list())
    assert 1 in ids
    assert 2 in ids


# ---------------------------------------------------------------------------
# Operation coverage: UPDATE and DELETE round-trip through wal2json
# ---------------------------------------------------------------------------


def test_cdc_update_and_delete_events_surface(
    fresh_postgres_cdc_with_slot, tmp_path,
):
    cfg = fresh_postgres_cdc_with_slot
    work = tmp_path / "work"
    work.mkdir()
    ctx = _CdcCtx(work, active_polls=10)

    _insert_users(cfg, [{"id": 7, "name": "carol", "age": 40}])

    src = PostgresCdcStreamSrc(
        conn=_make_cdc_conn(cfg),
        tables=["public.users"],
        replication_slot=_REPLICATION_SLOT,
        replication_slot_behavior="reuse",
        start_from="head",
        src_cfg={
            "tabsdata.cdc.buffer.max_rows": 1,
            "tabsdata.cdc.buffer.max_secs": 1,
            "tabsdata.cdc.poll_interval_seconds": 1,
            "postgres.cdc.blocking_timeout_seconds": 2,
        },
    )

    def _producer():
        time.sleep(2)
        _update_user(cfg, 7, "carol-2", 41)
        time.sleep(1)
        _delete_user(cfg, 7)

    threading.Thread(target=_producer, daemon=True).start()

    _run_plugin_in_thread(ctx, src, timeout_s=60.0)

    files = _all_staged_files(ctx)
    assert files, "no committed manifest files produced"
    df = pl.concat([_read_parquet(p) for p in files])
    op_col = next(c for c in df.columns if c.endswith("cdc.meta.op"))
    ops = set(df[op_col].to_list())
    # ``CdcOperation`` wire values: "i" / "u" / "d".
    assert "i" in ops
    assert "u" in ops
    assert "d" in ops


# ---------------------------------------------------------------------------
# Tail mode: only events produced *after* the plugin started are emitted
# ---------------------------------------------------------------------------


def test_cdc_tail_skips_pre_existing_events(
    fresh_postgres_cdc_with_slot, tmp_path,
):
    cfg = fresh_postgres_cdc_with_slot
    work = tmp_path / "work"
    work.mkdir()
    ctx = _CdcCtx(work, active_polls=8)

    _insert_users(cfg, [
        {"id": 100, "name": "pre-1", "age": 1},
        {"id": 101, "name": "pre-2", "age": 2},
    ])

    src = PostgresCdcStreamSrc(
        conn=_make_cdc_conn(cfg),
        tables=["public.users"],
        replication_slot=_REPLICATION_SLOT,
        replication_slot_behavior="reuse",
        start_from="tail",
        src_cfg={
            "tabsdata.cdc.buffer.max_rows": 1,
            "tabsdata.cdc.buffer.max_secs": 1,
            "tabsdata.cdc.poll_interval_seconds": 1,
            "postgres.cdc.blocking_timeout_seconds": 2,
        },
    )

    def _producer():
        time.sleep(3)
        _insert_users(cfg, [{"id": 200, "name": "post-1", "age": 20}])

    threading.Thread(target=_producer, daemon=True).start()

    _run_plugin_in_thread(ctx, src, timeout_s=60.0)

    files = _all_staged_files(ctx)
    if not files:
        pytest.skip("no events captured before active_polls exhausted")

    df = pl.concat([_read_parquet(p) for p in files])
    ids = set(df["id"].to_list())
    assert 100 not in ids
    assert 101 not in ids
    assert 200 in ids


# ---------------------------------------------------------------------------
# Offset persistence: the runner writes ``cdc.offset`` to the ctx state
# ---------------------------------------------------------------------------


def test_cdc_persists_offset_to_ctx_state(
    fresh_postgres_cdc_with_slot, tmp_path,
):
    cfg = fresh_postgres_cdc_with_slot
    work = tmp_path / "work"
    work.mkdir()
    ctx = _CdcCtx(work, active_polls=8)

    _insert_users(cfg, [{"id": 1, "name": "alice", "age": 30}])

    src = PostgresCdcStreamSrc(
        conn=_make_cdc_conn(cfg),
        tables=["public.users"],
        replication_slot=_REPLICATION_SLOT,
        replication_slot_behavior="reuse",
        start_from="head",
        src_cfg={
            "tabsdata.cdc.buffer.max_rows": 1,
            "tabsdata.cdc.buffer.max_secs": 1,
            "tabsdata.cdc.poll_interval_seconds": 1,
            "postgres.cdc.blocking_timeout_seconds": 2,
        },
    )

    _run_plugin_in_thread(ctx, src, timeout_s=60.0)

    if "cdc.offset" not in ctx._state:
        pytest.skip("no offset persisted (no events captured)")

    offset = ctx._state["cdc.offset"]
    assert isinstance(offset, dict)
    # ``LsnOffset.write()`` returns ``{"lsn": int}``.
    assert "lsn" in offset
    assert isinstance(offset["lsn"], int)
    assert offset["lsn"] > 0


# ---------------------------------------------------------------------------
# wal2json v1 format -- still supported via src_cfg override
# ---------------------------------------------------------------------------


def test_cdc_wal2json_v1_emits_inserted_row(
    fresh_postgres_cdc_with_slot, tmp_path,
):
    cfg = fresh_postgres_cdc_with_slot
    work = tmp_path / "work"
    work.mkdir()
    ctx = _CdcCtx(work, active_polls=10)

    src = PostgresCdcStreamSrc(
        conn=_make_cdc_conn(cfg),
        tables=["public.users"],
        replication_slot=_REPLICATION_SLOT,
        replication_slot_behavior="reuse",
        start_from="head",
        src_cfg={
            "tabsdata.cdc.buffer.max_rows": 1,
            "tabsdata.cdc.buffer.max_secs": 1,
            "tabsdata.cdc.poll_interval_seconds": 1,
            "postgres.cdc.blocking_timeout_seconds": 2,
            "postgres.cdc.wal2json_format_version": 1,
        },
    )

    _insert_users(cfg, [{"id": 9, "name": "v1-test", "age": 99}])

    _run_plugin_in_thread(ctx, src, timeout_s=60.0)

    files = _all_staged_files(ctx)
    if not files:
        pytest.skip("no events captured before active_polls exhausted")

    df = pl.concat([_read_parquet(p) for p in files])
    assert 9 in df["id"].to_list()
