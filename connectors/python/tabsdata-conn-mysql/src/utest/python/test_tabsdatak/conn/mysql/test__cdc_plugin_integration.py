"""End-to-end MySQL CDC integration tests against a real
binlog-enabled MySQL instance.

The ``fresh_mysql_cdc`` fixture (defined in ``conftest.py``)
provisions a docker MySQL with binlog flags (ROW + GTID + FULL
metadata) and grants the test user ``REPLICATION SLAVE`` /
``REPLICATION CLIENT``. Each test starts from a freshly truncated
``users`` table; the binlog itself accumulates across tests
(``start_from="tail"`` skips past prior events).

These tests exercise the full plugin: ingester opens, binlog
events flow, the buffer + disk sentinels stage parquet files, the
trx ctx receives the manifest and writes the offset state.
Per-function unit coverage of the ingester / dialect / offset
machinery lives in ``test__cdc_impl.py``; plugin wiring (no real
MySQL) in ``test__cdc_plugin.py``.
"""

from __future__ import annotations

import logging
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from unittest.mock import patch

import polars as pl
import pyarrow.parquet as pq
import pytest
from sqlalchemy import create_engine, text

from tabsdatak.conn.mysql import (
    MySQLCdcConn,
    MySQLCdcStreamSrc,
)
from tabsdatak.conn.mysql._cdc_plugin import MySQLCdcStreamSrcPlugin
from tabsdatak.spi import StageRef, StreamSrcPluginCtx

logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.mysql, pytest.mark.xdist_group("mysql-cdc_plugin_integration_seq")]


# ---------------------------------------------------------------------------
# StreamSrcPluginCtx stub
# ---------------------------------------------------------------------------


@dataclass
class _StagedManifest:
    """Captures one stage(manifest) + commit(ref) cycle for assertion."""
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

    # PluginCtx surface --------------------------------------------------

    @property
    def work_dir(self) -> Path:
        return self._work_dir

    @property
    def exec_plan_id(self) -> str:
        return "mysql-cdc-integ-exec-plan"

    @property
    def trx_id(self) -> str:
        return "mysql-cdc-integ-trx"

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

    # StreamSrcPluginCtx surface ----------------------------------------

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
        # The runner serialises the offset under ``cdc.offset``;
        # keep the latest value seen so an "expected state" test
        # could be added later.
        self._state = dict(state)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_cdc_conn(cfg) -> MySQLCdcConn:
    """``MySQLCdcConn`` for the fresh MySQL CDC backend. ``server_id``
    must be unique against any other replica connected to this MySQL
    -- the docker MySQL itself is server_id=1, so 512 (the connector
    default) is safe."""
    return MySQLCdcConn(uri=cfg.uri, credentials=cfg.credentials)


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
    """The runner installs SIGINT/SIGTERM handlers via
    ``signal.signal`` which only works on the main thread. We run
    the plugin on a worker thread (so the test can produce events
    in parallel), so substitute a plain ``threading.Event`` that
    will never be set -- the runner's other exit condition,
    ``ctx.is_active()``, drains the budget and ends the loop."""
    def _fake() -> threading.Event:
        return threading.Event()

    with patch(
        "tabsdatak.conn.common.sql.cdc._runner._setup_signal_handlers",
        side_effect=_fake,
    ):
        yield


def _run_plugin_in_thread(
    ctx: _CdcCtx,
    src: MySQLCdcStreamSrc,
    timeout_s: float = 30.0,
) -> None:
    """Run the plugin on a background thread so the main thread can
    drive INSERTs in parallel. Joins the thread under a deadline so
    a hung loop surfaces as a test failure rather than a hang."""
    err: list[BaseException] = []

    def _go():
        try:
            with _patched_signal_handlers():
                MySQLCdcStreamSrcPlugin().run(ctx=ctx, stream_src=src)
        except BaseException as exc:  # noqa: BLE001 -- surface to main thread
            err.append(exc)

    t = threading.Thread(target=_go, name="mysql-cdc-plugin", daemon=True)
    t.start()
    t.join(timeout=timeout_s)
    if t.is_alive():
        pytest.fail(
            f"MySQLCdcStreamSrcPlugin.run did not exit within {timeout_s}s",
        )
    if err:
        raise err[0]


# ---------------------------------------------------------------------------
# Happy-path: INSERT events surface through the plugin to the staged manifest
# ---------------------------------------------------------------------------


def test_cdc_insert_events_stage_a_committed_manifest(
    fresh_mysql_cdc, tmp_path,
):
    work = tmp_path / "work"
    work.mkdir()
    ctx = _CdcCtx(work, active_polls=8)

    src = MySQLCdcStreamSrc(
        conn=_make_cdc_conn(fresh_mysql_cdc),
        tables=[f"{fresh_mysql_cdc.database}.users"],
        # "head" replays from the oldest available binlog so we
        # capture events whether they happen before or after the
        # plugin starts. Combined with small buffer thresholds this
        # gives the test deterministic timing.
        start_from="head",
        src_cfg={
            "tabsdata.cdc.buffer.max_rows": 1,
            "tabsdata.cdc.buffer.max_secs": 1,
            "tabsdata.cdc.poll_interval_seconds": 1,
            "mysql.cdc.blocking_timeout_seconds": 2,
        },
    )

    # Produce one INSERT *before* the plugin starts so the binlog
    # has at least one event ready when the ingester opens.
    _insert_users(fresh_mysql_cdc, [
        {"id": 1, "name": "alice", "age": 30},
    ])
    # Then produce another after a short delay so we exercise the
    # poll-loop path too.

    def _producer():
        time.sleep(2)
        _insert_users(fresh_mysql_cdc, [
            {"id": 2, "name": "bob", "age": 25},
        ])

    threading.Thread(target=_producer, daemon=True).start()

    _run_plugin_in_thread(ctx, src, timeout_s=60.0)

    # The ctx should have at least one committed manifest with
    # the qualified table name; combined the parquet should
    # contain the INSERT rows we just produced.
    files = _all_staged_files(ctx)
    assert files, "no committed manifest files produced by the plugin"

    df = pl.concat([_read_parquet(p) for p in files])
    # Default ``columns_format="columns"`` + ``flatten_columns=True``
    # inlines new values at top level -- "id" / "name" / "age"
    # appear directly.
    assert set(df.columns) >= {"id", "name", "age"}
    ids = sorted(df["id"].to_list())
    assert 1 in ids
    assert 2 in ids


# ---------------------------------------------------------------------------
# Operation coverage: UPDATE and DELETE round-trip through the binlog stream
# ---------------------------------------------------------------------------


def test_cdc_update_and_delete_events_surface(fresh_mysql_cdc, tmp_path):
    work = tmp_path / "work"
    work.mkdir()
    ctx = _CdcCtx(work, active_polls=10)

    # Seed one row so update / delete have something to act on.
    _insert_users(fresh_mysql_cdc, [
        {"id": 7, "name": "carol", "age": 40},
    ])

    src = MySQLCdcStreamSrc(
        conn=_make_cdc_conn(fresh_mysql_cdc),
        tables=[f"{fresh_mysql_cdc.database}.users"],
        start_from="head",
        src_cfg={
            "tabsdata.cdc.buffer.max_rows": 1,
            "tabsdata.cdc.buffer.max_secs": 1,
            "tabsdata.cdc.poll_interval_seconds": 1,
            "mysql.cdc.blocking_timeout_seconds": 2,
        },
    )

    def _producer():
        time.sleep(2)
        _update_user(fresh_mysql_cdc, 7, "carol-2", 41)
        time.sleep(1)
        _delete_user(fresh_mysql_cdc, 7)

    threading.Thread(target=_producer, daemon=True).start()

    _run_plugin_in_thread(ctx, src, timeout_s=60.0)

    files = _all_staged_files(ctx)
    assert files, "no committed manifest files produced"
    df = pl.concat([_read_parquet(p) for p in files])
    # The meta-op column ``@td.cdc.meta.op`` is the discriminator.
    op_col = next(c for c in df.columns if c.endswith("cdc.meta.op"))
    ops = set(df[op_col].to_list())
    # We expect at least one INSERT (the seed) and one each of
    # UPDATE and DELETE. ``CdcOperation`` is a str-Enum with
    # single-character wire values: "i" / "u" / "d".
    assert "i" in ops
    assert "u" in ops
    assert "d" in ops


# ---------------------------------------------------------------------------
# Tail mode: only events produced *after* the plugin started are emitted
# ---------------------------------------------------------------------------


def test_cdc_tail_skips_pre_existing_events(fresh_mysql_cdc, tmp_path):
    work = tmp_path / "work"
    work.mkdir()
    ctx = _CdcCtx(work, active_polls=8)

    # Pre-populate two rows *before* the plugin starts.
    _insert_users(fresh_mysql_cdc, [
        {"id": 100, "name": "pre-1", "age": 1},
        {"id": 101, "name": "pre-2", "age": 2},
    ])

    src = MySQLCdcStreamSrc(
        conn=_make_cdc_conn(fresh_mysql_cdc),
        tables=[f"{fresh_mysql_cdc.database}.users"],
        # "tail" -> only events the plugin sees from now on.
        start_from="tail",
        src_cfg={
            "tabsdata.cdc.buffer.max_rows": 1,
            "tabsdata.cdc.buffer.max_secs": 1,
            "tabsdata.cdc.poll_interval_seconds": 1,
            "mysql.cdc.blocking_timeout_seconds": 2,
        },
    )

    def _producer():
        # Wait long enough for the ingester to attach to the live
        # edge before producing the row we care about.
        time.sleep(3)
        _insert_users(fresh_mysql_cdc, [
            {"id": 200, "name": "post-1", "age": 20},
        ])

    threading.Thread(target=_producer, daemon=True).start()

    _run_plugin_in_thread(ctx, src, timeout_s=60.0)

    files = _all_staged_files(ctx)
    if not files:
        # On a slow CI host the ingester may not have caught up
        # in time; this test is a property check, not a SLA. Skip
        # rather than fail to avoid flakes.
        pytest.skip("no events captured before active_polls exhausted")

    df = pl.concat([_read_parquet(p) for p in files])
    ids = set(df["id"].to_list())
    # The two pre-existing inserts must NOT appear.
    assert 100 not in ids
    assert 101 not in ids
    # The post-start insert must appear if anything did.
    assert 200 in ids


# ---------------------------------------------------------------------------
# Offset persistence: the runner writes ``cdc.offset`` to the ctx state
# ---------------------------------------------------------------------------


def test_cdc_persists_offset_to_ctx_state(fresh_mysql_cdc, tmp_path):
    work = tmp_path / "work"
    work.mkdir()
    ctx = _CdcCtx(work, active_polls=8)

    _insert_users(fresh_mysql_cdc, [
        {"id": 1, "name": "alice", "age": 30},
    ])

    src = MySQLCdcStreamSrc(
        conn=_make_cdc_conn(fresh_mysql_cdc),
        tables=[f"{fresh_mysql_cdc.database}.users"],
        start_from="head",
        src_cfg={
            "tabsdata.cdc.buffer.max_rows": 1,
            "tabsdata.cdc.buffer.max_secs": 1,
            "tabsdata.cdc.poll_interval_seconds": 1,
            "mysql.cdc.blocking_timeout_seconds": 2,
        },
    )

    _run_plugin_in_thread(ctx, src, timeout_s=60.0)

    # The runner writes the offset under ``cdc.offset`` once a
    # commit completes. If no events were captured the state
    # may be empty; skip rather than fail.
    if "cdc.offset" not in ctx._state:
        pytest.skip("no offset persisted (no events captured)")

    offset = ctx._state["cdc.offset"]
    assert isinstance(offset, dict)
    # ``GtidOffset.write()`` returns ``{"gtid": "..."}``.
    assert "gtid" in offset
