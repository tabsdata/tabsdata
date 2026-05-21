"""``DiskSentinel`` end-to-end on a real tmp working dir but a mock
``StreamSrcPluginCtx``. Asserts parquet files are written per
table, manifests are built for stage(), commit + trigger are
called in order, and the trigger-pending retry loop tracks
``trigger_monitor``."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import sqlalchemy as sa

from tabsdatak.conn.common.sql.cdc._column import TrackedColumn
from tabsdatak.conn.common.sql.cdc._dictionary import SchemaVersion, build_pa_schema
from tabsdatak.conn.common.sql.cdc._model import (
    CdcDataChange,
    TableObject,
    TrackedTable,
)
from tabsdatak.conn.common.sql.cdc._sentinel import CdcBuffer
from tabsdatak.conn.common.sql.cdc._sentinel.disk import DiskSentinel
from tabsdatak.conn.common.sql.cdc.typing import CdcFormat
from tabsdatak.spi import ParquetFile, TableManifest


# --- Fixtures ---------------------------------------------------------


def _sv() -> SchemaVersion:
    cols = (
        TrackedColumn(name="id", satype=sa.types.Integer(), patype=pa.int64()),
        TrackedColumn(name="name", satype=sa.types.String(), patype=pa.utf8()),
    )
    return SchemaVersion(
        version=0,
        raw=(("id", "INT", None), ("name", "VARCHAR", None)),
        hash="schemahash",
        columns=cols,
        pa_schema=build_pa_schema(cols, CdcFormat()),
    )


def _tracked(name: str) -> TrackedTable:
    return TrackedTable(
        data_table=TableObject(schema="s", table=name),
        cdc_table=TableObject(schema="s", table=name),
    )


_TS = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def _change(table: str, pid: int) -> CdcDataChange:
    return CdcDataChange(
        operation="i",
        transaction="tx",
        sequence=pid,
        timestamp=_TS,
        table=table,
        schema=_sv(),
        new_values={"id": pid, "name": f"n{pid}"},
        old_values=None,
    )


def _ctx(work_dir: Path, trigger_returns: bool = True, monitor_returns: bool = True):
    ctx = MagicMock()
    ctx.work_dir = work_dir
    ctx.stage = MagicMock(return_value="stage-ref-1")
    ctx.commit = MagicMock()
    ctx.trigger = MagicMock(return_value=trigger_returns)
    ctx.trigger_monitor = MagicMock(return_value=monitor_returns)
    return ctx


def _disk(ctx, tables, max_rows=10, max_secs=1000, max_bytes=None) -> DiskSentinel:
    return DiskSentinel(
        ctx=ctx,
        landing=Path(ctx.work_dir),
        max_rows=max_rows,
        max_bytes=max_bytes,
        max_secs=max_secs,
        tables=tables,
        cdc_format=CdcFormat(),
    )


# --- Initial state ---------------------------------------------------


def test_starts_empty(tmp_path):
    ds = _disk(_ctx(tmp_path), {"s.t1": _tracked("t1")})
    assert ds.is_empty is True
    assert ds.has_data is False
    assert ds.trigger_pending is False


# --- Buffer drain writes parquet files -------------------------------


def test_process_buffers_writes_one_parquet_per_table(tmp_path):
    tables = {"s.t1": _tracked("t1"), "s.t2": _tracked("t2")}
    ds = _disk(_ctx(tmp_path), tables)
    buffers = {
        "s.t1": CdcBuffer(rows=[_change("s.t1", 1), _change("s.t1", 2)]),
        "s.t2": CdcBuffer(rows=[_change("s.t2", 1)]),
    }
    ds.process_buffers(buffers)

    files = sorted(tmp_path.glob("*.parquet"))
    assert len(files) == 2
    # Parquet contents are readable and contain the expected row counts.
    table_rows = {pq.read_table(f).num_rows for f in files}
    assert table_rows == {1, 2}


def test_process_buffer_segments_by_schema_hash(tmp_path):
    tables = {"s.t1": _tracked("t1")}
    ds = _disk(_ctx(tmp_path), tables)
    a = _change("s.t1", 1)
    b = _change("s.t1", 2)
    # Mutate one row's schema hash to a different version.
    new_sv = SchemaVersion(
        version=1, raw=b.schema.raw, hash="othersha",
        columns=b.schema.columns, pa_schema=b.schema.pa_schema,
    )
    b.schema = new_sv
    ds.process_buffers({"s.t1": CdcBuffer(rows=[a, b])})
    # Two segments -> two parquet files for the same table.
    assert len(list(tmp_path.glob("*.parquet"))) == 2


# --- stage_data: builds manifest, commits, fires trigger ------------


def test_stage_data_builds_table_manifest(tmp_path):
    ctx = _ctx(tmp_path)
    tables = {"s.t1": _tracked("t1"), "s.t2": _tracked("t2")}
    ds = _disk(ctx, tables)
    ds.process_buffers({
        "s.t1": CdcBuffer(rows=[_change("s.t1", 1)]),
        "s.t2": CdcBuffer(rows=[_change("s.t2", 1)]),
    })
    ds.stage_data()

    # ctx.stage called once with a list of TableManifest entries
    # (one per table that had data).
    assert ctx.stage.call_count == 1
    manifest = ctx.stage.call_args.args[0]
    assert len(manifest) == 2
    assert all(isinstance(m, TableManifest) for m in manifest)
    tables_in_manifest = {m.table for m in manifest}
    # The disk sentinel emits the bare table name (not the
    # ``schema.table`` qualified form) so it fits the framework's
    # ``TableNameSpec`` validator.
    assert tables_in_manifest == {"t1", "t2"}
    # Files are ParquetFile wrappers around the on-disk paths.
    for m in manifest:
        for f in m.files:
            assert isinstance(f, ParquetFile)


def test_stage_data_commits_then_triggers(tmp_path):
    ctx = _ctx(tmp_path)
    tables = {"s.t1": _tracked("t1")}
    ds = _disk(ctx, tables)
    ds.process_buffers({"s.t1": CdcBuffer(rows=[_change("s.t1", 1)])})
    ds.stage_data()

    # commit happens before trigger.
    assert ctx.commit.call_args.args == ("stage-ref-1",)
    assert ctx.trigger.call_count == 1


def test_stage_data_with_no_data_is_noop(tmp_path):
    ctx = _ctx(tmp_path)
    tables = {"s.t1": _tracked("t1")}
    ds = _disk(ctx, tables)
    ds.stage_data()
    assert ctx.stage.call_count == 0
    assert ctx.commit.call_count == 0
    assert ctx.trigger.call_count == 0


def test_stage_data_marks_pending_when_trigger_returns_false(tmp_path):
    ctx = _ctx(tmp_path, trigger_returns=False)
    tables = {"s.t1": _tracked("t1")}
    ds = _disk(ctx, tables)
    ds.process_buffers({"s.t1": CdcBuffer(rows=[_change("s.t1", 1)])})
    ds.stage_data()
    assert ds.trigger_pending is True


def test_retry_trigger_clears_pending_when_monitor_acks(tmp_path):
    ctx = _ctx(tmp_path, trigger_returns=False, monitor_returns=True)
    tables = {"s.t1": _tracked("t1")}
    ds = _disk(ctx, tables)
    ds.process_buffers({"s.t1": CdcBuffer(rows=[_change("s.t1", 1)])})
    ds.stage_data()
    assert ds.trigger_pending is True
    ds.retry_trigger()
    assert ds.trigger_pending is False


def test_retry_trigger_no_op_when_not_pending(tmp_path):
    ctx = _ctx(tmp_path)
    ds = _disk(ctx, {"s.t1": _tracked("t1")})
    ds.retry_trigger()
    assert ctx.trigger_monitor.call_count == 0


# --- flush() decision -----------------------------------------------


def test_flush_force_drains_even_below_thresholds(tmp_path):
    ctx = _ctx(tmp_path)
    tables = {"s.t1": _tracked("t1")}
    ds = _disk(ctx, tables, max_rows=1000, max_secs=1000)
    ds.process_buffers({"s.t1": CdcBuffer(rows=[_change("s.t1", 1)])})
    assert ds.flush(force=True) is True
    assert ctx.stage.call_count == 1


def test_flush_no_data_returns_false(tmp_path):
    ctx = _ctx(tmp_path)
    ds = _disk(ctx, {"s.t1": _tracked("t1")})
    assert ds.flush(force=False) is False
    assert ds.flush(force=True) is False


def test_flush_rows_threshold(tmp_path):
    ctx = _ctx(tmp_path)
    tables = {"s.t1": _tracked("t1")}
    ds = _disk(ctx, tables, max_rows=1, max_secs=1000)
    ds.process_buffers({"s.t1": CdcBuffer(rows=[_change("s.t1", 1)])})
    assert ds.flush() is True
    assert ctx.stage.call_count == 1
