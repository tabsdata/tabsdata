#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

# noinspection PyProtectedMember
import tabsdatak.conn.dev._plugin as plugin
from tabsdatak._handoff import DestinationContract, SourceContract
from tabsdatak.conn.dev import (
    MAX_DATA_BYTES,
    Data,
    DataGenSrc,
    DataGenSrcConn,
    DevFrameDest,
    DevFrameDestConn,
    DevFrameSrc,
    DevFrameSrcConn,
    TrashDest,
    TrashDestConn,
)
from tabsdatak.conn.dev.error import DevRuntimeException, DevValidateException

pytestmark = pytest.mark.conn_dev


class FakeSrcCtx:
    """Minimal SrcPluginCtx double: a working directory plus per-flow attributes."""

    def __init__(self, work_dir: Path):
        self._work_dir = work_dir
        self._state: dict = {}

    @property
    def work_dir(self) -> Path:
        return self._work_dir

    def get_attr(self, attr):
        return self._state.get(attr)

    def set_attr(self, attr, val):
        self._state[attr] = val


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------


def test_data_accepts_polars_and_pandas_and_normalizes_to_polars():
    from_pl = Data(pl.DataFrame({"a": [1, 2]}))
    from_pd = Data(pd.DataFrame({"a": [1, 2]}))
    assert isinstance(from_pl.frame, pl.DataFrame)
    assert isinstance(from_pd.frame, pl.DataFrame)
    assert from_pl.frame.equals(from_pd.frame)


def test_data_default_chunk_size_is_none():
    assert Data(pl.DataFrame({"a": [1]})).chunk_size is None


@pytest.mark.parametrize("bad", [0, -1, 1.5, "3", True])
def test_data_rejects_non_positive_int_chunk_size(bad):
    with pytest.raises(DevValidateException):
        Data(pl.DataFrame({"a": [1]}), chunk_size=bad)


@pytest.mark.parametrize("bad", [[1, 2, 3], {"a": 1}, "df", 42])
def test_data_rejects_non_dataframe(bad):
    with pytest.raises(DevValidateException):
        Data(bad)


def test_data_round_trips_frame_and_chunk_size_through_json():
    src = DataGenSrc(
        data=[
            Data(pl.DataFrame({"a": [1, 2, 3]})),
            Data(pd.DataFrame({"b": [4.0, 5.0]}), chunk_size=2),
        ]
    )
    loaded = DataGenSrc.from_json(src.to_json())
    assert loaded == src
    assert [d.chunk_size for d in loaded.data] == [None, 2]
    assert loaded.data[0].frame.equals(pl.DataFrame({"a": [1, 2, 3]}))
    assert loaded.data[1].frame.equals(pl.DataFrame({"b": [4.0, 5.0]}))


def test_data_serializes_as_base64_parquet_dict():
    d = Data(pl.DataFrame({"a": [1]}))
    spec = DataGenSrc(data=[d]).to_dict()["spec"]
    entry = spec["data"][0]
    assert set(entry) == {"parquet", "chunk_size"}
    assert entry["chunk_size"] is None
    assert isinstance(entry["parquet"], str)


# ---------------------------------------------------------------------------
# DataGenSrc size budget
# ---------------------------------------------------------------------------


def _incompressible_df(rows: int) -> pl.DataFrame:
    return pl.DataFrame({"x": [os.urandom(16).hex() for _ in range(rows)]})


def test_datagen_src_accepts_data_under_budget():
    src = DataGenSrc(data=[Data(pl.DataFrame({"a": list(range(1000))}))])
    assert len(src.data) == 1


def test_datagen_src_rejects_combined_data_over_budget():
    half = _incompressible_df(300_000)
    single = len(Data(half).parquet_b64())
    assert single < MAX_DATA_BYTES < 2 * single
    with pytest.raises(DevValidateException):
        DataGenSrc(data=[Data(half), Data(half)])


# ---------------------------------------------------------------------------
# DataGenSrcPlugin.read_in
# ---------------------------------------------------------------------------


def _read(ctx, src) -> list:
    slots = plugin.DataGenSrcPlugin().read_in(ctx, DataGenSrcConn(), src)
    out = []
    for slot in slots:
        tables = list(slot)
        out.append(None if not tables else tables[0].frame.collect())
    return out


def test_datagen_returns_lazy_frame_inputs():
    assert SourceContract(plugin.DataGenSrcPlugin).frames()


def test_read_in_unchunked_emits_whole_frame_every_run(tmp_path):
    ctx = FakeSrcCtx(tmp_path)
    frame = pl.DataFrame({"a": [1, 2, 3]})
    src = DataGenSrc(data=[Data(frame)])
    for _ in range(3):
        (only,) = _read(ctx, src)
        assert only.equals(frame)


def test_read_in_chunked_advances_offset_then_returns_an_empty_frame(tmp_path):
    ctx = FakeSrcCtx(tmp_path)
    frame = pl.DataFrame({"b": [10, 20, 30, 40, 50]})
    src = DataGenSrc(data=[Data(frame, chunk_size=2)])

    (r1,) = _read(ctx, src)
    (r2,) = _read(ctx, src)
    (r3,) = _read(ctx, src)
    (r4,) = _read(ctx, src)

    assert r1.to_dict(as_series=False) == {"b": [10, 20]}
    assert r2.to_dict(as_series=False) == {"b": [30, 40]}
    assert r3.to_dict(as_series=False) == {"b": [50]}
    assert r4.schema == frame.schema
    assert r4.height == 0


def test_read_in_returns_one_slot_per_data_independently(tmp_path):
    ctx = FakeSrcCtx(tmp_path)
    full = pl.DataFrame({"a": [1, 2, 3]})
    chunked = pl.DataFrame({"b": [7, 8]})
    src = DataGenSrc(data=[Data(full), Data(chunked, chunk_size=1)])

    first = _read(ctx, src)
    assert first[0].equals(full)
    assert first[1].to_dict(as_series=False) == {"b": [7]}

    second = _read(ctx, src)
    assert second[0].equals(full)
    assert second[1].to_dict(as_series=False) == {"b": [8]}

    third = _read(ctx, src)
    assert third[0].equals(full)
    assert third[1].schema == chunked.schema
    assert third[1].height == 0


# ---------------------------------------------------------------------------
# DevFrameSrc
# ---------------------------------------------------------------------------


def _read_frames(ctx, src) -> list:
    slots = plugin.DevFrameSrcPlugin().read_in(ctx, DevFrameSrcConn(), src)
    out = []
    for slot in slots:
        tables = list(slot)
        out.append(None if not tables else tables[0].frame.collect())
    return out


def test_frame_src_returns_lazy_frame_inputs():
    assert SourceContract(plugin.DevFrameSrcPlugin).frames()


def test_frame_src_round_trips_its_data():
    src = DevFrameSrc(data=[Data(pl.DataFrame({"a": [1, 2]}), chunk_size=1)])
    assert DevFrameSrc.from_json(src.to_json()) == src


def test_frame_src_rejects_combined_data_over_budget():
    half = _incompressible_df(300_000)
    with pytest.raises(DevValidateException, match="DevFrameSrc"):
        DevFrameSrc(data=[Data(half), Data(half)])


def test_frame_src_emits_what_datagen_emits(tmp_path):
    frames_ctx = FakeSrcCtx(tmp_path)
    files_ctx = FakeSrcCtx(tmp_path)
    full = pl.DataFrame({"a": [1, 2, 3]})
    chunked = pl.DataFrame({"b": [7, 8]})
    data = [Data(full), Data(chunked, chunk_size=1)]
    for _ in range(3):
        frames = _read_frames(frames_ctx, DevFrameSrc(data=data))
        files = _read(files_ctx, DataGenSrc(data=data))
        assert [None if f is None else f.to_dicts() for f in frames] == [
            None if f is None else f.to_dicts() for f in files
        ]


# ---------------------------------------------------------------------------
# TrashDest
# ---------------------------------------------------------------------------


def test_trash_dest_has_no_configuration_and_round_trips():
    dest = TrashDest()
    assert TrashDest.from_json(dest.to_json()) == dest


def test_trash_dest_write_out_discards_without_reading():
    unreadable = pl.LazyFrame({"a": ["x"]}).select(pl.col("a").str.to_integer())
    out = plugin.TrashDestPlugin().write_out(
        object(), TrashDestConn(), TrashDest(), [unreadable]
    )
    assert out is None


def test_trash_dest_write_out_discards_none_tables():
    out = plugin.TrashDestPlugin().write_out(
        object(), TrashDestConn(), TrashDest(), [None]
    )
    assert out is None


def test_trash_dest_takes_lazy_frames():
    assert DestinationContract(plugin.TrashDestPlugin).frames()


# ---------------------------------------------------------------------------
# Connections
# ---------------------------------------------------------------------------


def test_conns_share_kind_but_are_distinct_classes():
    assert DataGenSrcConn is not TrashDestConn
    assert DataGenSrcConn.__kind__ == TrashDestConn.__kind__ == "connectionDef"


# ---------------------------------------------------------------------------
# DevFrameDest
# ---------------------------------------------------------------------------


class TransactionContext:
    transaction_id = "trx-1"


def test_frame_dest_round_trips_and_defaults_to_one_output():
    dest = DevFrameDest()
    assert dest.outputs == 1
    assert DevFrameDest.from_json(DevFrameDest(outputs=2).to_json()) == DevFrameDest(
        outputs=2
    )


@pytest.mark.parametrize("bad", [0, -1])
def test_frame_dest_rejects_a_non_positive_output_count(bad):
    with pytest.raises(DevValidateException):
        DevFrameDest(outputs=bad).validate()


def test_frame_dest_receives_lazy_frames():
    assert DestinationContract(plugin.DevFrameDestPlugin).frames()


def test_frame_dest_reports_what_each_table_holds(tmp_path):
    plugin.DevFrameDestPlugin().write_out(
        TransactionContext(),
        DevFrameDestConn(folder=str(tmp_path)),
        DevFrameDest(outputs=2),
        [
            pl.LazyFrame({"a": [1, 2], "b": ["x", "y"]}),
            pl.LazyFrame({"c": [3]}),
        ],
    )
    assert json.loads((tmp_path / "trx-1.json").read_text()) == {
        "tables": [
            {"type": "LazyFrame", "rows": 2, "columns": ["a", "b"]},
            {"type": "LazyFrame", "rows": 1, "columns": ["c"]},
        ],
    }


def test_frame_dest_reports_a_none_output_as_null(tmp_path):
    plugin.DevFrameDestPlugin().write_out(
        TransactionContext(),
        DevFrameDestConn(folder=str(tmp_path)),
        DevFrameDest(outputs=2),
        [None, pl.LazyFrame({"c": [3]})],
    )
    assert json.loads((tmp_path / "trx-1.json").read_text(encoding="utf-8")) == {
        "tables": [None, {"type": "LazyFrame", "rows": 1, "columns": ["c"]}],
    }


def test_frame_dest_reports_every_output_as_null_when_all_are_none(tmp_path):
    plugin.DevFrameDestPlugin().write_out(
        TransactionContext(),
        DevFrameDestConn(folder=str(tmp_path)),
        DevFrameDest(outputs=2),
        [None, None],
    )
    assert json.loads((tmp_path / "trx-1.json").read_text(encoding="utf-8")) == {
        "tables": [None, None],
    }


def test_frame_dest_reports_an_unwritable_folder(tmp_path):
    blocked = tmp_path / "file"
    blocked.write_text("")
    with pytest.raises(DevRuntimeException):
        plugin.DevFrameDestPlugin().write_out(
            TransactionContext(),
            DevFrameDestConn(folder=str(blocked)),
            DevFrameDest(),
            [],
        )
