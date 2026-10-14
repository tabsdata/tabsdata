#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from tabsdatak.api import BatchThresholds, PollThresholds, StreamThresholds

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream import _plugin

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._signal import StopSignal
from tabsdatak.conn.common.sql.cdc._ingester import CDC_OFFSET_STATE_KEY
from tabsdatak.conn.common.sql.cdc._model import TableObject, TrackedTable

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql.cdc._runner import CdcRunConfig, _PendingLoad
from tabsdatak.conn.db2 import _cdc_impl
from tabsdatak.conn.db2._cdc_impl import CommitSeqOffset, SqlReplicaIngester

# noinspection PyProtectedMember
from tabsdatak.conn.db2._cdc_plugin import Db2CdcStreamSrcPlugin


def _commitseq(flush: int, lsn: int) -> bytes:
    return flush.to_bytes(8, byteorder="big") + lsn.to_bytes(8, byteorder="big")


CAPTURE_START = _commitseq(1, 0x10)
LOAD_START = _commitseq(2, 0x200)
WINDOW_END = _commitseq(3, 0x300)
ROW = _commitseq(5, 0x500)
CAPTURED = _commitseq(6, 0x600)

TABLES = {"SRC.T": "T_CD", "SRC.Q": "Q_CD"}


@pytest.fixture(autouse=True)
def _no_signal_handlers(monkeypatch):
    monkeypatch.setattr(_plugin.StopSignal, "install", lambda: StopSignal())


class _Result:
    def __init__(self, value):
        self._value = value

    def scalar(self):
        return self._value[0]

    def fetchall(self):
        return self._value


class _Connection:
    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def execute(self, statement, parameters=None):
        sql = str(statement)
        if "ibmsnap_register r" in sql:
            return _Result([
                ("ASN", cd, "A", None, CAPTURE_START, "N", CAPTURE_START)
                for cd in TABLES.values()
            ])
        if "IBMSNAP_RESTART" in sql:
            return _Result((CAPTURED,))
        raise AssertionError(f"unexpected statement: {sql}")


class _Session:
    def connect(self):
        return _Connection()

    def dispose(self):
        pass


@dataclass
class _Row:
    offset: CommitSeqOffset


class _Encoder:
    def slot(self, record: _Row) -> str:
        return "SRC.T"

    def rows(self, record: _Row) -> int:
        return 1

    def size(self, record: _Row) -> int:
        return 1

    def offset(self, record: _Row) -> CommitSeqOffset:
        return record.offset

    def encode(self, slot: str, records: list[_Row]):
        return [pa.table({"id": list(range(len(records)))})]


class _Plugin(Db2CdcStreamSrcPlugin):
    def build_encoder(self, run):
        return _Encoder()


class _LoadedPlugin(_Plugin):
    """The initial load already read its rows from LOAD_START, and the
    changes committed while it ran end at WINDOW_END."""

    def seed(self, run):
        run.ingester.seed_offset(
            CommitSeqOffset(commits={f"ASN.{cd}": LOAD_START for cd in TABLES.values()})
        )
        run.pending_load = _PendingLoad(
            manifest=[[] for _ in TABLES],
            window_end=CommitSeqOffset(
                commits={f"ASN.{cd}": WINDOW_END for cd in TABLES.values()}
            ),
        )


NO_IDLE = PollThresholds(max_secs=1, idle_secs=0)


class _Ctx:
    def __init__(self, work_dir: Path, stream_state: dict | None) -> None:
        self.work_dir = work_dir
        self.flush_interval_mins = 0
        self.thresholds = StreamThresholds(batch=BatchThresholds(max_rows=1 << 62))
        self.stream_state = stream_state
        self.first_stage = stream_state is None
        self._active = iter([True, False])

    @property
    def active(self) -> bool:
        return next(self._active, False)

    def stage(self, tables_files, new_state) -> None:
        self.stream_state = dict(new_state)
        self.first_stage = False

    def checkpoint(self, new_state) -> None:
        self.stream_state = dict(new_state)


def _ingester(monkeypatch) -> SqlReplicaIngester:
    """An ingester over T, which has one row at ROW past the load window,
    and Q, which is quiet."""
    monkeypatch.setattr(_cdc_impl.time, "sleep", lambda seconds: None)
    bi = SqlReplicaIngester(conn=MagicMock(), tables=list(TABLES), start_from="tail")
    tracked = {
        data: TrackedTable(
            data_table=TableObject(schema="SRC", table=data.split(".")[1]),
            cdc_table=TableObject(schema="ASN", table=cd),
        )
        for data, cd in TABLES.items()
    }
    bi._tracked_tables = tracked
    bi._before_img_prefixes = dict.fromkeys(TABLES)
    bi._schema_registry = MagicMock()
    bi._poll_max_secs = 0.05
    monkeypatch.setattr(bi, "open_session", lambda work_dir: _Session())
    monkeypatch.setattr(bi, "harvest_tables", lambda **kwargs: tracked)
    monkeypatch.setattr(bi, "_extract_raw_schema", lambda *a: ())
    monkeypatch.setattr(bi, "_query_table_watermarks", lambda c: {"ASN.T_CD": ROW})

    def changes(**kwargs):
        if kwargs["watermark"] == ROW:
            yield _Row(offset=CommitSeqOffset(commits={"ASN.T_CD": ROW}))

    monkeypatch.setattr(bi, "_query_table_changes", changes)
    return bi


def _staged_commits(ctx: _Ctx) -> dict[str, bytes]:
    return CommitSeqOffset.read(ctx.stream_state[CDC_OFFSET_STATE_KEY]).commits


def test_a_table_quiet_since_the_start_stays_in_the_saved_state(monkeypatch, tmp_path):
    """CDC-1: a stream resumes with both tables at LOAD_START; T gets a row
    and Q stays quiet, and the stream stages T's row. The saved state must
    still hold a position for Q, so the next start can resume it; today the
    buffer takes its offset from the ingester before the first poll, when Db2
    has not read its positions yet, so Q is dropped and the next start fails
    DB2_33."""
    resumed = CommitSeqOffset(
        commits={f"ASN.{cd}": LOAD_START for cd in TABLES.values()}
    )
    ctx = _Ctx(tmp_path, {CDC_OFFSET_STATE_KEY: resumed.write()})

    _Plugin().stream(
        ctx=ctx,
        ingester=_ingester(monkeypatch),
        cfg=CdcRunConfig(poll=NO_IDLE),
    )

    assert _staged_commits(ctx).get("ASN.T_CD", b"") >= ROW
    restarted = _ingester(monkeypatch)
    restarted.seed_offset(
        restarted.resume_offset(first_stage=False, stream_state=ctx.stream_state)
    )
    restarted._init_reader(_Session())
    assert restarted.base_offset().commits["ASN.Q_CD"] >= LOAD_START


def test_a_table_quiet_after_the_initial_load_resumes_past_its_window(
    monkeypatch, tmp_path
):
    """CDC-2: an initial load read from LOAD_START and staged the changes
    committed while it ran, up to WINDOW_END; then T gets a row and Q stays
    quiet, and the stream stages T's row. Q's saved position must be at or
    past WINDOW_END: its changes up to there are already in the load, and a
    restart from an earlier position publishes them again. Today the stream
    starts its offset from the load start, so Q saves LOAD_START."""
    ctx = _Ctx(tmp_path, None)

    _LoadedPlugin().stream(
        ctx=ctx,
        ingester=_ingester(monkeypatch),
        cfg=CdcRunConfig(poll=NO_IDLE, initial_load=True),
    )

    staged = _staged_commits(ctx)
    assert staged.get("ASN.T_CD", b"") >= ROW
    assert staged.get("ASN.Q_CD", b"") >= WINDOW_END, staged.get("ASN.Q_CD")


def test_a_table_read_but_not_staged_keeps_its_starting_position(monkeypatch):
    """A stage covering none of T's rows saves T where the stream started it,
    never where reading has got to: those rows are not staged yet, and a
    restart from past them would skip them."""
    bi = _ingester(monkeypatch)
    started = {f"ASN.{cd}": LOAD_START for cd in TABLES.values()}
    bi._base_offset = CommitSeqOffset(commits=started)
    bi._mark = CommitSeqOffset(commits={**started, "ASN.T_CD": ROW})

    staged = bi.staged_offset(CommitSeqOffset())

    assert staged.commits == started
