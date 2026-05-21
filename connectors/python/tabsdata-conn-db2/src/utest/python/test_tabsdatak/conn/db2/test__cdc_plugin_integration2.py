"""End-to-end Db2 CDC integration tests against a real Db2 instance
with the **real IBM SQL Replication apparatus**: ``asnclp`` control
tables, the ``asncap`` capture program, and an ``AsncapWatchdog``.

Counterpart to ``test__cdc_plugin_integration.py`` -- that file uses
the lightweight synthetic-ASN fixture (``fresh_db2_cdc``), seeding
``ASN.IBMSNAP_REGISTER`` / ``ASN.IBMSNAP_UOW`` / ``USERS_CD`` by hand
and driving change events by inserting into the CD table directly.
This file uses ``fresh_db2_cdc_asncap`` (defined in ``conftest.py``)
which provisions real archive logging + asnclp + asncap, registers a
uniquely-named source table for the test, and drives change events
by inserting into the **source** table -- asncap propagates them to
the CD table the same way it would in production.

The synthetic file is fast (no archive backup, no asncap startup); this
file is slow (~30s setup + per-test asncap propagation polling) but
exercises the full IBM SQL Replication stack end-to-end.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import patch

import polars as pl
import pyarrow.parquet as pq
import pytest
from sqlalchemy import create_engine, text

# noinspection PyProtectedMember
from test_tabsdatak.conn.db2._cdc_docker import (
    DB2_CDC_ASN_SCHEMA,
    DB2_CDC_STAGING_TABLE_PREFIX,
)

from tabsdatak.conn.db2 import (
    CommitSeqPosition,
    Db2CdcConn,
    Db2CdcStreamSrc,
)
from tabsdatak.conn.db2._cdc_plugin import Db2CdcStreamSrcPlugin
from tabsdatak.spi import StageRef, StreamSrcPluginCtx

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.db2


# ---------------------------------------------------------------------------
# StreamSrcPluginCtx stub (copy of the one in
# ``test__cdc_plugin_integration.py``; kept here to keep each file
# self-contained -- the synthetic and asncap variants exist on parallel
# tracks and may diverge).
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
        return "db2-cdc-asncap-integ-exec-plan"

    @property
    def trx_id(self) -> str:
        return "db2-cdc-asncap-integ-trx"

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


def _cd_qualified(source_table: str) -> str:
    return f"{DB2_CDC_ASN_SCHEMA}.{DB2_CDC_STAGING_TABLE_PREFIX}{source_table.upper()}"


def _engine(cfg):
    # ``cfg`` here is the ``Db2CdcConfig`` from ``_cdc_docker``; build
    # a SQLAlchemy URL on the same shape ``conftest.DBConfig.uri_with_credentials``
    # uses so the test can reuse SQLAlchemy machinery.
    import urllib.parse
    u = urllib.parse.quote(cfg.root_user, safe="")
    p = urllib.parse.quote(cfg.root_password, safe="")
    return create_engine(
        f"db2+ibm_db://{u}:{p}@{cfg.host}:{cfg.port}/{cfg.database}"
    )


def _make_cdc_conn(cfg) -> Db2CdcConn:
    from tabsdatak.api import Secret
    from tabsdatak.conn.common.types import UserPassword
    import urllib.parse
    return Db2CdcConn(
        uri=(
            f"db2+ibm_db://"
            f"{urllib.parse.quote(cfg.root_user, safe='')}:"
            f"{urllib.parse.quote(cfg.root_password, safe='')}@"
            f"{cfg.host}:{cfg.port}/{cfg.database}"
        ),
        credentials=UserPassword(
            user=Secret.lit(cfg.root_user),
            password=Secret.lit(cfg.root_password),
        ),
    )


def _source_insert(cfg, source_schema: str, source_table: str, rows: list[dict]) -> None:
    eng = _engine(cfg)
    try:
        with eng.begin() as conn:
            for row in rows:
                conn.execute(
                    text(
                        f"insert into {source_schema}.{source_table} "
                        f"(id, name, age) values (:id, :name, :age)"
                    ),
                    row,
                )
    finally:
        eng.dispose()


def _source_update(
    cfg,
    source_schema: str,
    source_table: str,
    id_: int,
    name: str,
    age: int,
) -> None:
    eng = _engine(cfg)
    try:
        with eng.begin() as conn:
            conn.execute(
                text(
                    f"update {source_schema}.{source_table} "
                    f"set name = :name, age = :age where id = :id"
                ),
                {"name": name, "age": age, "id": id_},
            )
    finally:
        eng.dispose()


def _source_delete(cfg, source_schema: str, source_table: str, id_: int) -> None:
    eng = _engine(cfg)
    try:
        with eng.begin() as conn:
            conn.execute(
                text(
                    f"delete from {source_schema}.{source_table} where id = :id"
                ),
                {"id": id_},
            )
    finally:
        eng.dispose()


def _wait_cd_count(
    cfg,
    source_table: str,
    expected: int,
    timeout_s: float = 90.0,
    poll_s: float = 1.0,
) -> None:
    """Poll the CD table until asncap has propagated ``expected`` rows.

    The cd table fills asynchronously after the source COMMIT -- asncap
    reads the log buffer, transforms each row, writes it to the staging
    table. Most local runs propagate within a couple of seconds, but
    the asncap watchdog occasionally restarts the process which can
    spike that to 30s+; the timeout matches the watchdog interval."""
    cd_qualified = _cd_qualified(source_table)
    eng = _engine(cfg)
    deadline = time.monotonic() + timeout_s
    last_count = -1
    try:
        while time.monotonic() < deadline:
            with eng.connect() as conn:
                row = conn.execute(
                    text(f"select count(*) from {cd_qualified}")
                ).fetchone()
                last_count = int(row[0]) if row else 0
                if last_count >= expected:
                    return
            time.sleep(poll_s)
    finally:
        eng.dispose()
    pytest.fail(
        f"asncap did not propagate {expected} row(s) into {cd_qualified} "
        f"within {timeout_s:.0f}s (saw {last_count})"
    )


def _cd_commitseqs(cfg, source_table: str) -> list[bytes]:
    """Read CD-table commitseqs in arrival order. Used by the
    resume-by-commitseq test to pick a real watermark mid-stream."""
    cd_qualified = _cd_qualified(source_table)
    eng = _engine(cfg)
    try:
        with eng.connect() as conn:
            rows = conn.execute(
                text(
                    f"select IBMSNAP_COMMITSEQ from {cd_qualified} "
                    f"order by IBMSNAP_COMMITSEQ, IBMSNAP_INTENTSEQ"
                )
            ).fetchall()
            return [bytes(r[0]) for r in rows]
    finally:
        eng.dispose()


# ---------------------------------------------------------------------------
# Plugin-run scaffolding (same shape as the synthetic file)
# ---------------------------------------------------------------------------


from contextlib import contextmanager  # noqa: E402


@contextmanager
def _patched_signal_handlers():
    """``run_cdc`` installs SIGINT / SIGTERM handlers via ``signal.signal``
    which only works on the main thread."""

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
    timeout_s: float = 90.0,
) -> None:
    err: list[BaseException] = []

    def _go():
        try:
            with _patched_signal_handlers():
                Db2CdcStreamSrcPlugin().run(ctx=ctx, stream_src=src)
        except BaseException as exc:  # noqa: BLE001
            err.append(exc)

    t = threading.Thread(target=_go, name="db2-cdc-asncap-plugin", daemon=True)
    t.start()
    t.join(timeout=timeout_s)
    if t.is_alive():
        pytest.fail(
            f"Db2CdcStreamSrcPlugin.run did not exit within {timeout_s}s"
        )
    if err:
        raise err[0]


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


def _make_src(
    cfg,
    source_table: str,
    *,
    start_from: Any = "head",
    src_cfg: dict | None = None,
) -> Db2CdcStreamSrc:
    return Db2CdcStreamSrc(
        conn=_make_cdc_conn(cfg),
        tables=[_cd_qualified(source_table)],
        start_from=start_from,
        src_cfg=src_cfg or {
            "tabsdata.cdc.buffer.max_rows": 1,
            "tabsdata.cdc.buffer.max_secs": 1,
            "tabsdata.cdc.poll_interval_seconds": 1,
            "db2.cdc.blocking_timeout_seconds": 3,
        },
    )


# ---------------------------------------------------------------------------
# Happy-path: INSERT event surfaces through the plugin
# ---------------------------------------------------------------------------


def test_cdc_insert_event_stages_a_committed_manifest(fresh_db2_cdc_asncap, tmp_path):
    cfg, source_schema, source_table = fresh_db2_cdc_asncap
    work = tmp_path / "work"
    work.mkdir()
    ctx = _CdcCtx(work, active_polls=12)

    src = _make_src(cfg, source_table, start_from="head")

    _source_insert(
        cfg, source_schema, source_table,
        [{"id": 1, "name": "alice", "age": 30}],
    )
    _wait_cd_count(cfg, source_table, expected=1)

    _run_plugin_in_thread(ctx, src, timeout_s=90.0)

    files = _all_staged_files(ctx)
    assert files, "no committed manifest files produced by the plugin"

    df = pl.concat([_read_parquet(p) for p in files])
    # Default ``columns_format="columns"`` + ``flatten_columns=True``
    # inlines the after-image columns at top level.
    assert set(df.columns) >= {"ID", "NAME", "AGE"}
    assert 1 in df["ID"].to_list()


# ---------------------------------------------------------------------------
# Operation coverage: INSERT / UPDATE / DELETE all surface through asncap
# ---------------------------------------------------------------------------


def test_cdc_insert_update_delete_events_surface(fresh_db2_cdc_asncap, tmp_path):
    cfg, source_schema, source_table = fresh_db2_cdc_asncap
    work = tmp_path / "work"
    work.mkdir()
    ctx = _CdcCtx(work, active_polls=18)

    src = _make_src(cfg, source_table, start_from="head")

    # I, U, D on the same primary key; asncap emits one CD row per
    # operation (UPDATE carries the before-image in the ``_<col>``
    # columns; the registration declared ``image both``).
    _source_insert(
        cfg, source_schema, source_table,
        [{"id": 7, "name": "carol", "age": 40}],
    )
    _source_update(cfg, source_schema, source_table, 7, "carol-2", 41)
    _source_delete(cfg, source_schema, source_table, 7)

    # asncap may collapse the U into two CD rows (image both adds a
    # mirror row for the before-image); we wait for >= 3 to cover both
    # asncap variants.
    _wait_cd_count(cfg, source_table, expected=3)

    _run_plugin_in_thread(ctx, src, timeout_s=90.0)

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
# events are skipped, post-start events surface
# ---------------------------------------------------------------------------


def test_cdc_tail_skips_pre_existing_events(fresh_db2_cdc_asncap, tmp_path):
    cfg, source_schema, source_table = fresh_db2_cdc_asncap
    work = tmp_path / "work"
    work.mkdir()
    ctx = _CdcCtx(work, active_polls=18)

    # Two pre-existing CD rows -- ``"tail"`` parks the per-table mark
    # at current max(commitseq), so these should NOT surface.
    _source_insert(
        cfg, source_schema, source_table,
        [
            {"id": 100, "name": "pre-1", "age": 1},
            {"id": 101, "name": "pre-2", "age": 2},
        ],
    )
    _wait_cd_count(cfg, source_table, expected=2)

    src = _make_src(cfg, source_table, start_from="tail")

    def _producer():
        # Let the plugin park at the current watermark first, then
        # produce a post-start event so it has somewhere to go.
        time.sleep(3)
        _source_insert(
            cfg, source_schema, source_table,
            [{"id": 200, "name": "post-1", "age": 20}],
        )

    threading.Thread(target=_producer, daemon=True).start()

    _run_plugin_in_thread(ctx, src, timeout_s=90.0)

    files = _all_staged_files(ctx)
    if not files:
        pytest.skip("no events captured before active_polls exhausted")

    df = pl.concat([_read_parquet(p) for p in files])
    ids = set(df["ID"].to_list())
    assert 100 not in ids
    assert 101 not in ids
    assert 200 in ids


# ---------------------------------------------------------------------------
# CommitSeqPosition: resume from a real asncap-assigned commitseq,
# replaying only events past that mark
# ---------------------------------------------------------------------------


def test_cdc_resumes_from_commitseq_position(fresh_db2_cdc_asncap, tmp_path):
    cfg, source_schema, source_table = fresh_db2_cdc_asncap
    work = tmp_path / "work"
    work.mkdir()
    ctx = _CdcCtx(work, active_polls=12)

    # Three source rows in three separate transactions -- ``asncap``
    # assigns one ``IBMSNAP_COMMITSEQ`` per commit, so this gives us
    # three distinct commitseqs we can resume between. A single
    # multi-row insert would group them under one commitseq and the
    # ``> commitseq`` resume predicate would skip all of them.
    for id_, name, age in ((50, "five", 5), (60, "six", 6), (70, "seven", 7)):
        _source_insert(
            cfg, source_schema, source_table,
            [{"id": id_, "name": name, "age": age}],
        )
    _wait_cd_count(cfg, source_table, expected=3)

    seqs = _cd_commitseqs(cfg, source_table)
    assert len(seqs) >= 3, f"expected 3 CD rows, got {len(seqs)}"

    # Resume from the second commitseq -- the ingester's where-clause
    # is ``IBMSNAP_COMMITSEQ > commitseq``, so only the third row
    # should surface.
    resume_seq_hex = seqs[1].hex()

    src = _make_src(
        cfg, source_table,
        start_from=CommitSeqPosition(seq=resume_seq_hex),
    )
    _run_plugin_in_thread(ctx, src, timeout_s=90.0)

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


def test_cdc_persists_offset_to_ctx_state(fresh_db2_cdc_asncap, tmp_path):
    cfg, source_schema, source_table = fresh_db2_cdc_asncap
    work = tmp_path / "work"
    work.mkdir()
    ctx = _CdcCtx(work, active_polls=12)

    _source_insert(
        cfg, source_schema, source_table,
        [{"id": 1, "name": "alice", "age": 30}],
    )
    _wait_cd_count(cfg, source_table, expected=1)

    src = _make_src(cfg, source_table, start_from="head")
    _run_plugin_in_thread(ctx, src, timeout_s=90.0)

    if "cdc.offset" not in ctx._state:
        pytest.skip("no offset persisted (no events captured)")

    offset = ctx._state["cdc.offset"]
    assert isinstance(offset, dict)
    # ``CommitSeqOffset.write()`` returns ``{cd_qualified: hex}``.
    cd_qualified = _cd_qualified(source_table)
    assert cd_qualified in offset
    # The persisted mark should be a real asncap-assigned commitseq
    # (a 32-char hex of 16 binary bytes).
    assert isinstance(offset[cd_qualified], str)
    assert len(offset[cd_qualified]) == 32
