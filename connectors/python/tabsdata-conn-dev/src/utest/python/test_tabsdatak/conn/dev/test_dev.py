#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

# noinspection PyProtectedMember
import tabsdatak.conn.dev._plugin as plugin
from tabsdatak.conn.dev import (
    MAX_DATA_BYTES,
    Data,
    DataGenSrc,
    DataGenSrcConn,
    TrashDest,
    TrashDestConn,
)
from tabsdatak.conn.dev.error import DevValidateException

pytestmark = pytest.mark.conn_dev


class FakeSrcCtx:
    """Minimal SrcPluginCtx double: a work dir plus persistent scratch."""

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
        out.append(None if not slot else pl.read_parquet(slot[0].file))
    return out


def test_read_in_unchunked_emits_whole_frame_every_run(tmp_path):
    ctx = FakeSrcCtx(tmp_path)
    frame = pl.DataFrame({"a": [1, 2, 3]})
    src = DataGenSrc(data=[Data(frame)])
    for _ in range(3):
        (only,) = _read(ctx, src)
        assert only.equals(frame)


def test_read_in_chunked_advances_offset_then_returns_none(tmp_path):
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
    assert r4 is None


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
    assert third[1] is None


# ---------------------------------------------------------------------------
# TrashDest
# ---------------------------------------------------------------------------


def test_trash_dest_has_no_configuration_and_round_trips():
    dest = TrashDest()
    assert TrashDest.from_json(dest.to_json()) == dest


def test_trash_dest_write_out_discards_and_returns_none(tmp_path):
    parquet = tmp_path / "in.parquet"
    pl.DataFrame({"a": [1]}).write_parquet(parquet)
    out = plugin.TrashDestPlugin().write_out(
        object(), TrashDestConn(), TrashDest(), [parquet]
    )
    assert out is None
    assert parquet.exists()


# ---------------------------------------------------------------------------
# Connections
# ---------------------------------------------------------------------------


def test_conns_share_kind_but_are_distinct_classes():
    assert DataGenSrcConn is not TrashDestConn
    assert DataGenSrcConn.__kind__ == TrashDestConn.__kind__ == "connectionDef"
