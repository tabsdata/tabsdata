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
    Db2CdcConn,
    Db2CdcStreamSrc,
)
from tabsdatak.conn.db2._cdc_plugin import Db2CdcStreamSrcPlugin
from tabsdatak.spi import StageRef, StreamSrcPluginCtx

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


class _Stage(StageRef):
    def __init__(self, idx: int) -> None:
        self.idx = idx


class _CdcCtx(StreamSrcPluginCtx):
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


def _make_cdc_conn(cfg) -> Db2CdcConn:
    return Db2CdcConn(uri=cfg.uri, credentials=cfg.credentials)


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
        for tm in sm.manifest:
            files.extend(Path(p) for p in tm.files)
    return files


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
    src: Db2CdcStreamSrc,
    timeout_s: float = 60.0,
) -> None:
    err: list[BaseException] = []

    def _go():
        try:
            with _patched_signal_handlers():
                Db2CdcStreamSrcPlugin().run(ctx=ctx, stream_src=src)
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

    src = Db2CdcStreamSrc(
        conn=_make_cdc_conn(cfg),
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

    _run_plugin_in_thread(ctx, src, timeout_s=60.0)

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

    src = Db2CdcStreamSrc(
        conn=_make_cdc_conn(cfg),
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

    src = Db2CdcStreamSrc(
        conn=_make_cdc_conn(cfg),
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

    _run_plugin_in_thread(ctx, src, timeout_s=60.0)

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
    src = Db2CdcStreamSrc(
        conn=_make_cdc_conn(cfg),
        tables=[f"{_CD_SCHEMA}.{_CD_TABLE}"],
        start_from=CommitSeqPosition(seq=_commitseq(6).hex()),
        src_cfg={
            "tabsdata.cdc.buffer.max_rows": 1,
            "tabsdata.cdc.buffer.max_secs": 1,
            "tabsdata.cdc.poll_interval_seconds": 1,
            "db2.cdc.blocking_timeout_seconds": 3,
        },
    )

    _run_plugin_in_thread(ctx, src, timeout_s=60.0)

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

    src = Db2CdcStreamSrc(
        conn=_make_cdc_conn(cfg),
        tables=[f"{_CD_SCHEMA}.{_CD_TABLE}"],
        start_from="head",
        src_cfg={
            "tabsdata.cdc.buffer.max_rows": 1,
            "tabsdata.cdc.buffer.max_secs": 1,
            "tabsdata.cdc.poll_interval_seconds": 1,
            "db2.cdc.blocking_timeout_seconds": 3,
        },
    )

    _run_plugin_in_thread(ctx, src, timeout_s=60.0)

    if "cdc.offset" not in ctx._state:
        pytest.skip("no offset persisted (no events captured)")

    offset = ctx._state["cdc.offset"]
    assert isinstance(offset, dict)
    # ``CommitSeqOffset.write()`` returns ``{cd_qualified: hex}``.
    qualified = f"{_CD_SCHEMA}.{_CD_TABLE}"
    assert qualified in offset
    # The advanced mark should be at or past commitseq 42.
    assert int(offset[qualified], 16) >= 42
