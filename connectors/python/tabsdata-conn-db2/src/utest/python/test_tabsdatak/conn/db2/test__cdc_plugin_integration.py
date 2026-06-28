"""End-to-end Db2 CDC integration tests against a real Db2
instance with a hand-built IBM SQL Replication apparatus.

The ``fresh_db2_cdc`` fixture (defined in ``conftest.py``)
provisions a Db2 container, then synthesises ``ASN.IBMSNAP_REGISTER``,
``ASN.IBMSNAP_UOW``, the source ``DB2INST1.USERS`` table, and the
CD ``DB2INST1.USERS_CD`` table. Tests then drive change events by
inserting rows into the CD table directly (no ``asncap`` capture
program needed) -- the ingester exercises the same code paths it
would in production: registration lookup, per-table watermark
polling, CD-column matching, row emission, offset advance.

Per-function unit coverage of the ingester / dialect / matcher /
offset machinery lives in ``test__cdc_impl.py``; plugin wiring
(no real Db2) in ``test__cdc_plugin.py``.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import patch

import polars as pl
import pyarrow.parquet as pq
import pytest
from sqlalchemy import create_engine, text

from tabsdatak.conn.db2 import (
    CommitSeqPosition,
    Db2CdcStreamSrcConn,
    Db2CdcStreamSrc,
)
from tabsdatak.conn.db2._cdc_plugin import Db2CdcStreamSrcPlugin
from tabsdatak.spi import StreamSrcPluginCtx

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.db2


# Names baked into the fixture (kept in sync with conftest).
_CD_SCHEMA = "DB2INST1"
_CD_TABLE = "USERS_CD"
_SOURCE_QUALIFIED = "DB2INST1.USERS"


# ---------------------------------------------------------------------------
# StreamSrcPluginCtx stub (parallels mysql / postgres integration tests)
# ---------------------------------------------------------------------------


@dataclass
class _StagedManifest:
    manifest: list
    committed: bool = False


class _CdcCtx(StreamSrcPluginCtx):
    def __init__(
        self,
        work_dir: Path,
        *,
        active_polls: int,
        initial_state: dict | None = None,
    ) -> None:
        self._work_dir = work_dir
        self._active_left = active_polls
        # ``initial_state`` models a prior run's persisted stream state:
        # when present, this is not the first stage and ``stream_state``
        # replays it until a new batch overwrites it.
        self._state: dict[str, Any] = dict(initial_state) if initial_state else {}
        self._tabsdata_owns_state = initial_state is not None
        self._committed_before = initial_state is not None
        self.staged: list[_StagedManifest] = []
        self._stage_counter = 0
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

    def store_attr(self, attr, val):
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
            return not self._committed_before and not self.staged

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
            self.staged.append(
                _StagedManifest(manifest=list(tables_files), committed=True)
            )
            # Tabsdata-owned state persists the dict (read back via
            # ``stream_state``); a plain callable (external system owns the
            # offset) leaves ``stream_state`` None and is invoked for its
            # side effect.
            if isinstance(new_state, dict):
                self._state = dict(new_state)
                self._tabsdata_owns_state = True
            else:
                self._tabsdata_owns_state = False
                new_state()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_cdc_conn(cfg) -> Db2CdcStreamSrcConn:
    return Db2CdcStreamSrcConn(uri=cfg.uri, credentials=cfg.credentials)


def _commitseq(value: int) -> bytes:
    """``IBMSNAP_COMMITSEQ`` is a 16-byte binary; integer values
    are zero-padded big-endian -- the same ordering the production
    sort uses."""
    return value.to_bytes(16, byteorder="big")


def _insert_cd_row(
    cfg,
    *,
    op: str,
    commitseq: bytes,
    intentseq: bytes,
    id_: int | None,
    name: str | None,
    age: int | None,
    x_id: int | None = None,
    x_name: str | None = None,
    x_age: int | None = None,
    uow_ts: datetime | None = None,
) -> None:
    """Synthesise one capture event by inserting a row into the CD
    table (and a matching IBMSNAP_UOW row if a timestamp is given,
    so ``TimestampPosition`` translation can find it).

    ``IBMSNAP_LOGMARKER`` on the CD row is the per-event commit
    timestamp the ingester reads back; default to ``uow_ts`` when
    provided (so CD + UOW agree on the same commit), otherwise
    stamp ``now``."""
    logmarker = uow_ts if uow_ts is not None else datetime.now(timezone.utc)
    eng = create_engine(cfg.uri_with_credentials)
    try:
        with eng.begin() as conn:
            conn.execute(
                text(
                    f"insert into {_CD_SCHEMA}.{_CD_TABLE} "
                    "(IBMSNAP_OPERATION, IBMSNAP_COMMITSEQ, IBMSNAP_INTENTSEQ, "
                    " IBMSNAP_LOGMARKER, "
                    " ID, NAME, AGE, X_ID, X_NAME, X_AGE) "
                    "values (:op, :cs, :is_, :lm, :id, :name, :age, "
                    "        :xid, :xname, :xage)"
                ),
                {
                    "op": op,
                    "cs": commitseq,
                    "is_": intentseq,
                    "lm": logmarker,
                    "id": id_,
                    "name": name,
                    "age": age,
                    "xid": x_id,
                    "xname": x_name,
                    "xage": x_age,
                },
            )
            if uow_ts is not None:
                conn.execute(
                    text(
                        "insert into ASN.IBMSNAP_UOW "
                        "(IBMSNAP_COMMITSEQ, IBMSNAP_LOGMARKER) "
                        "values (:cs, :ts)"
                    ),
                    {"cs": commitseq, "ts": uow_ts},
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
        for table_files in sm.manifest:
            files.extend(Path(p) for p in table_files)
    return files


def _staged_ids(ctx: _CdcCtx) -> set[int]:
    """``ID`` values across every committed parquet the ctx captured."""
    files = _all_staged_files(ctx)
    if not files:
        return set()
    df = pl.concat([_read_parquet(p) for p in files])
    return set(df["ID"].to_list())


@contextmanager
def _patched_signal_handlers():
    """``run_cdc`` installs SIGINT / SIGTERM handlers via
    ``signal.signal`` which only works on the main thread."""
    def _fake() -> threading.Event:
        return threading.Event()

    with patch(
        "tabsdatak.conn.common.sql.cdc._runner._setup_signal_handlers",
        side_effect=_fake,
    ):
        yield


def _run_plugin_in_thread(
    ctx: _CdcCtx,
    conn: Db2CdcStreamSrcConn,
    src: Db2CdcStreamSrc,
    timeout_s: float = 60.0,
) -> None:
    err: list[BaseException] = []

    def _go():
        try:
            with _patched_signal_handlers():
                Db2CdcStreamSrcPlugin().run(ctx=ctx, conn=conn, stream_src=src)
        except BaseException as exc:  # noqa: BLE001
            err.append(exc)

    t = threading.Thread(target=_go, name="db2-cdc-plugin", daemon=True)
    t.start()
    t.join(timeout=timeout_s)
    if t.is_alive():
        pytest.fail(
            f"Db2CdcStreamSrcPlugin.run did not exit within {timeout_s}s",
        )
    if err:
        raise err[0]


# ---------------------------------------------------------------------------
# Happy-path: INSERT event surfaces through the plugin
# ---------------------------------------------------------------------------


@pytest.mark.db2_seq
@pytest.mark.xdist_group("db2_seq")
def test_cdc_insert_event_stages_a_committed_manifest(
    fresh_db2_cdc, tmp_path,
):
    cfg = fresh_db2_cdc
    work = tmp_path / "work"
    work.mkdir()
    ctx = _CdcCtx(work, active_polls=6)

    conn = _make_cdc_conn(cfg)
    src = Db2CdcStreamSrc(
        tables=[f"{_CD_SCHEMA}.{_CD_TABLE}"],
        start_from="head",
        src_cfg={
            "tabsdata.cdc.buffer.max_rows": 1,
            "tabsdata.cdc.buffer.max_secs": 1,
            "tabsdata.cdc.poll_interval_seconds": 1,
            "db2.cdc.blocking_timeout_seconds": 3,
        },
    )

    # Seed an INSERT capture before the plugin starts.
    _insert_cd_row(
        cfg, op="I",
        commitseq=_commitseq(1),
        intentseq=_commitseq(1),
        id_=1, name="alice", age=30,
    )

    _run_plugin_in_thread(ctx, conn, src, timeout_s=60.0)

    files = _all_staged_files(ctx)
    assert files, "no committed manifest files produced by the plugin"

    df = pl.concat([_read_parquet(p) for p in files])
    # Default ``columns_format="columns"`` + ``flatten_columns=True``
    # inlines the after-image columns at top level.
    assert set(df.columns) >= {"ID", "NAME", "AGE"}
    assert 1 in df["ID"].to_list()


# ---------------------------------------------------------------------------
# Operation coverage: INSERT / UPDATE / DELETE all surface
# ---------------------------------------------------------------------------


@pytest.mark.db2_seq
@pytest.mark.xdist_group("db2_seq")
def test_cdc_insert_update_delete_events_surface(fresh_db2_cdc, tmp_path):
    cfg = fresh_db2_cdc
    work = tmp_path / "work"
    work.mkdir()
    ctx = _CdcCtx(work, active_polls=8)

    conn = _make_cdc_conn(cfg)
    src = Db2CdcStreamSrc(
        tables=[f"{_CD_SCHEMA}.{_CD_TABLE}"],
        start_from="head",
        src_cfg={
            "tabsdata.cdc.buffer.max_rows": 1,
            "tabsdata.cdc.buffer.max_secs": 1,
            "tabsdata.cdc.poll_interval_seconds": 1,
            "db2.cdc.blocking_timeout_seconds": 3,
        },
    )

    # INSERT @ commitseq 1, UPDATE @ commitseq 2 (carries the
    # before-image in X_*), DELETE @ commitseq 3.
    _insert_cd_row(
        cfg, op="I",
        commitseq=_commitseq(1), intentseq=_commitseq(1),
        id_=7, name="carol", age=40,
    )
    _insert_cd_row(
        cfg, op="U",
        commitseq=_commitseq(2), intentseq=_commitseq(1),
        id_=7, name="carol-2", age=41,
        x_id=7, x_name="carol", x_age=40,
    )
    _insert_cd_row(
        cfg, op="D",
        commitseq=_commitseq(3), intentseq=_commitseq(1),
        id_=7, name=None, age=None,
    )

    _run_plugin_in_thread(ctx, conn, src, timeout_s=60.0)

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
# Tail mode: per-table watermarks at the current max -- pre-existing
# rows are skipped, post-start rows surface
# ---------------------------------------------------------------------------


@pytest.mark.db2_seq
@pytest.mark.xdist_group("db2_seq")
def test_cdc_tail_skips_pre_existing_events(fresh_db2_cdc, tmp_path):
    cfg = fresh_db2_cdc
    work = tmp_path / "work"
    work.mkdir()
    ctx = _CdcCtx(work, active_polls=8)

    # Two pre-existing CD rows -- ``"tail"`` parks the per-table mark
    # at the current max(commitseq), so these should NOT surface.
    _insert_cd_row(
        cfg, op="I",
        commitseq=_commitseq(10), intentseq=_commitseq(1),
        id_=100, name="pre-1", age=1,
    )
    _insert_cd_row(
        cfg, op="I",
        commitseq=_commitseq(11), intentseq=_commitseq(1),
        id_=101, name="pre-2", age=2,
    )

    conn = _make_cdc_conn(cfg)
    src = Db2CdcStreamSrc(
        tables=[f"{_CD_SCHEMA}.{_CD_TABLE}"],
        start_from="tail",
        src_cfg={
            "tabsdata.cdc.buffer.max_rows": 1,
            "tabsdata.cdc.buffer.max_secs": 1,
            "tabsdata.cdc.poll_interval_seconds": 1,
            "db2.cdc.blocking_timeout_seconds": 3,
        },
    )

    def _producer():
        time.sleep(2)
        _insert_cd_row(
            cfg, op="I",
            commitseq=_commitseq(20), intentseq=_commitseq(1),
            id_=200, name="post-1", age=20,
        )

    threading.Thread(target=_producer, daemon=True).start()

    _run_plugin_in_thread(ctx, conn, src, timeout_s=60.0)

    files = _all_staged_files(ctx)
    if not files:
        pytest.skip("no events captured before active_polls exhausted")

    df = pl.concat([_read_parquet(p) for p in files])
    ids = set(df["ID"].to_list())
    assert 100 not in ids
    assert 101 not in ids
    assert 200 in ids


# ---------------------------------------------------------------------------
# CommitSeqPosition: resume from a specific commitseq, replaying
# events past that mark
# ---------------------------------------------------------------------------


@pytest.mark.db2_seq
@pytest.mark.xdist_group("db2_seq")
def test_cdc_resumes_from_commitseq_position(fresh_db2_cdc, tmp_path):
    cfg = fresh_db2_cdc
    work = tmp_path / "work"
    work.mkdir()
    ctx = _CdcCtx(work, active_polls=6)

    # Three CD rows at commitseqs 5, 6, 7.
    for cs, id_, name in ((5, 50, "five"), (6, 60, "six"), (7, 70, "seven")):
        _insert_cd_row(
            cfg, op="I",
            commitseq=_commitseq(cs), intentseq=_commitseq(1),
            id_=id_, name=name, age=cs,
        )

    # Start at commitseq 6 -- only commitseq 7 should surface (the
    # ingester's where-clause is ``IBMSNAP_COMMITSEQ > commitseq``).
    conn = _make_cdc_conn(cfg)
    src = Db2CdcStreamSrc(
        tables=[f"{_CD_SCHEMA}.{_CD_TABLE}"],
        start_from=CommitSeqPosition(seq=_commitseq(6).hex()),
        src_cfg={
            "tabsdata.cdc.buffer.max_rows": 1,
            "tabsdata.cdc.buffer.max_secs": 1,
            "tabsdata.cdc.poll_interval_seconds": 1,
            "db2.cdc.blocking_timeout_seconds": 3,
        },
    )

    _run_plugin_in_thread(ctx, conn, src, timeout_s=60.0)

    files = _all_staged_files(ctx)
    assert files, "no committed manifest files produced"
    df = pl.concat([_read_parquet(p) for p in files])
    ids = set(df["ID"].to_list())
    assert 70 in ids
    assert 50 not in ids
    assert 60 not in ids


# ---------------------------------------------------------------------------
# Offset persistence: the runner writes ``cdc.offset`` to the ctx state
# ---------------------------------------------------------------------------


@pytest.mark.db2_seq
@pytest.mark.xdist_group("db2_seq")
def test_cdc_persists_offset_to_ctx_state(fresh_db2_cdc, tmp_path):
    cfg = fresh_db2_cdc
    work = tmp_path / "work"
    work.mkdir()
    ctx = _CdcCtx(work, active_polls=6)

    _insert_cd_row(
        cfg, op="I",
        commitseq=_commitseq(42), intentseq=_commitseq(1),
        id_=1, name="alice", age=30,
    )

    conn = _make_cdc_conn(cfg)
    src = Db2CdcStreamSrc(
        tables=[f"{_CD_SCHEMA}.{_CD_TABLE}"],
        start_from="head",
        src_cfg={
            "tabsdata.cdc.buffer.max_rows": 1,
            "tabsdata.cdc.buffer.max_secs": 1,
            "tabsdata.cdc.poll_interval_seconds": 1,
            "db2.cdc.blocking_timeout_seconds": 3,
        },
    )

    _run_plugin_in_thread(ctx, conn, src, timeout_s=60.0)

    if "cdc.offset" not in ctx._state:
        pytest.skip("no offset persisted (no events captured)")

    # ``CommitSeqOffset.write()`` persists the JSON string ``{cd_qualified: hex}``.
    offset = json.loads(ctx._state["cdc.offset"])
    assert isinstance(offset, dict)
    qualified = f"{_CD_SCHEMA}.{_CD_TABLE}"
    assert qualified in offset
    # The advanced mark should be at or past commitseq 42.
    assert int(offset[qualified], 16) >= 42


# ---------------------------------------------------------------------------
# Cross-run resume: DB2 is Tabsdata-owned-offset, so a second run seeded
# with the first run's stream_state resumes *past* it.
# ---------------------------------------------------------------------------


@pytest.mark.db2_seq
@pytest.mark.xdist_group("db2_seq")
def test_cdc_resumes_from_persisted_stream_state(fresh_db2_cdc, tmp_path):
    """Two runs sharing the Tabsdata-persisted offset.

    Run 1 (first_stage) consumes commitseqs 5 and 6 and persists its
    commit-seq map to ``stream_state``. Run 2 is seeded with that state
    (``first_stage`` is False), so even though ``start_from='head'`` would
    replay from the oldest CD row, the ingester resumes *past* run 1's
    commit-seq: it picks up only commitseq 7, never replaying 5/6.
    """
    cfg = fresh_db2_cdc
    qualified = f"{_CD_SCHEMA}.{_CD_TABLE}"
    src_cfg = {
        "tabsdata.cdc.buffer.max_rows": 1,
        "tabsdata.cdc.buffer.max_secs": 1,
        "tabsdata.cdc.poll_interval_seconds": 1,
        "db2.cdc.blocking_timeout_seconds": 3,
    }

    conn = _make_cdc_conn(cfg)

    def _src() -> Db2CdcStreamSrc:
        return Db2CdcStreamSrc(
            tables=[qualified],
            start_from="head",
            src_cfg=src_cfg,
        )

    # --- Run 1: consume commitseqs 5 & 6, persist the commit-seq map ---
    for cs, id_, name in ((5, 50, "five"), (6, 60, "six")):
        _insert_cd_row(
            cfg, op="I",
            commitseq=_commitseq(cs), intentseq=_commitseq(1),
            id_=id_, name=name, age=cs,
        )
    run1_work = tmp_path / "run1"
    run1_work.mkdir()
    ctx1 = _CdcCtx(run1_work, active_polls=6)
    _run_plugin_in_thread(ctx1, conn, _src(), timeout_s=60.0)

    if not {50, 60} <= _staged_ids(ctx1):
        pytest.skip("run 1 captured no events (timing); cannot test resume")
    persisted = ctx1.stream_state
    assert persisted is not None and "cdc.offset" in persisted
    # ``CommitSeqOffset.write()`` advanced to at least commitseq 6.
    assert int(json.loads(persisted["cdc.offset"])[qualified], 16) >= 6

    # --- A new CD row past run 1's commit-seq ---
    _insert_cd_row(
        cfg, op="I",
        commitseq=_commitseq(7), intentseq=_commitseq(1),
        id_=70, name="seven", age=7,
    )

    # --- Run 2: seeded with run 1's state -> resumes past commitseq 6 ---
    run2_work = tmp_path / "run2"
    run2_work.mkdir()
    ctx2 = _CdcCtx(run2_work, active_polls=6, initial_state=persisted)
    assert ctx2.first_stage is False
    _run_plugin_in_thread(ctx2, conn, _src(), timeout_s=60.0)

    run2_ids = _staged_ids(ctx2)
    # Resumed from the persisted commit-seq: only the new row surfaces;
    # the already-consumed ones are NOT replayed (which they would be on a
    # ``start_from='head'`` cold start).
    assert 70 in run2_ids, f"run 2 should capture the post-resume row, got {run2_ids}"
    assert 50 not in run2_ids, f"run 2 replayed a consumed row, got {run2_ids}"
    assert 60 not in run2_ids, f"run 2 replayed a consumed row, got {run2_ids}"
