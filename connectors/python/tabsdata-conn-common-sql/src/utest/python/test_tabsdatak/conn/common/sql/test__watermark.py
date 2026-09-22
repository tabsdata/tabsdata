#
#  Copyright 2026 Tabsdata Inc.
#

"""Unit tests for the shared watermark helpers.

The batch rewrite every bulk load stages through, and the checks a destination
makes on the watermark it names against the columns its merge already reads.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge import MergeCfg

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._watermark import (
    validate_watermark_column,
    watermarked_batch,
)
from tabsdatak.conn.common.sql.error import (
    SqlCommonErrorCode,
    SqlCommonValidateException,
)

pytestmark = pytest.mark.sql_common

_TRX = "TRX_1"


class _UpperMergeCfg(MergeCfg):
    """A config whose dialect upper-folds an identifier."""

    __kind__ = "upperWatermarkMergeCfg"

    def _fold(self, name: str) -> str:
        return name.upper()


def _batch(path: Path, **columns) -> Path:
    pl.DataFrame(columns or {"id": [1, 2]}).write_parquet(path)
    return path


def _cfg(**overrides) -> MergeCfg:
    kwargs: dict = dict(on=["order_id"], op_column="op")
    kwargs.update(overrides)
    return MergeCfg(**kwargs)


# --- watermarked_batch -----------------------------------------------


class TestWatermarkedBatch:
    def test_every_row_carries_the_trx_id(self, tmp_path):
        with watermarked_batch(_batch(tmp_path / "in.parquet"), "wm", _TRX) as out:
            assert pl.read_parquet(out)["wm"].to_list() == [_TRX, _TRX]

    def test_the_column_is_a_string(self, tmp_path):
        """A trx id is an opaque token, and every dialect declares the column a
        string type, so the batch has to agree."""
        with watermarked_batch(_batch(tmp_path / "in.parquet"), "wm", _TRX) as out:
            assert pl.read_parquet(out).schema["wm"] == pl.String

    def test_the_batch_columns_are_kept(self, tmp_path):
        batch = _batch(tmp_path / "in.parquet", order_id=[1, 2], status=["a", "b"])
        with watermarked_batch(batch, "wm", _TRX) as out:
            assert pl.read_parquet(out).columns == ["order_id", "status", "wm"]

    def test_the_basename_is_kept(self, tmp_path):
        """A connector may name the staged object after it."""
        batch = _batch(tmp_path / "in.parquet")
        with watermarked_batch(batch, "wm", _TRX) as out:
            assert out.name == batch.name

    def test_a_copy_is_yielded_not_the_original(self, tmp_path):
        batch = _batch(tmp_path / "in.parquet")
        with watermarked_batch(batch, "wm", _TRX) as out:
            assert out != batch
        assert pl.read_parquet(batch).columns == ["id"]

    def test_the_copy_is_removed_on_the_way_out(self, tmp_path):
        with watermarked_batch(_batch(tmp_path / "in.parquet"), "wm", _TRX) as out:
            staged = out
            assert staged.exists()
        assert not staged.exists()

    def test_no_column_yields_the_original_untouched(self, tmp_path):
        batch = _batch(tmp_path / "in.parquet")
        with watermarked_batch(batch, None, _TRX) as out:
            assert out == batch
            assert pl.read_parquet(out).columns == ["id"]

    def test_an_empty_batch_still_gains_the_column(self, tmp_path):
        path = tmp_path / "empty.parquet"
        pl.DataFrame({"id": []}).write_parquet(path)
        with watermarked_batch(path, "wm", _TRX) as out:
            frame = pl.read_parquet(out)
        assert frame.height == 0
        assert "wm" in frame.columns

    def test_the_adapt_rewrite_rides_the_stamp(self, tmp_path):
        """One sink, not two -- which is the point of passing the rewrite in
        rather than making it beforehand."""
        batch = _batch(tmp_path / "in.parquet", id=[1, 2])
        with watermarked_batch(
            batch, "wm", _TRX, adapt=lambda frame: frame.with_columns(pl.col("id") * 10)
        ) as out:
            frame = pl.read_parquet(out)
        assert frame["id"].to_list() == [10, 20]
        assert frame["wm"].to_list() == [_TRX, _TRX]

    def test_an_adapt_alone_is_enough_to_rewrite(self, tmp_path):
        batch = _batch(tmp_path / "in.parquet", id=[1])
        with watermarked_batch(
            batch, None, _TRX, adapt=lambda frame: frame.with_columns(pl.col("id") * 10)
        ) as out:
            assert out != batch
            assert pl.read_parquet(out)["id"].to_list() == [10]

    def test_neither_a_column_nor_an_adapt_yields_the_original(self, tmp_path):
        batch = _batch(tmp_path / "in.parquet")
        with watermarked_batch(batch, None, _TRX, adapt=None) as out:
            assert out == batch


# --- validate_watermark_column ---------------------------------------


class TestValidateWatermarkColumn:
    def test_a_name_beside_the_keys_and_the_marker_is_allowed(self):
        assert validate_watermark_column(_cfg(), "wm") is None

    def test_no_watermark_is_allowed(self):
        assert validate_watermark_column(_cfg(), None) is None

    def test_a_key_is_rejected(self):
        """A key is what a row is matched by; a watermark is written onto every
        row the merge touches, so one column cannot be both."""
        with pytest.raises(SqlCommonValidateException) as ei:
            validate_watermark_column(_cfg(on=["order_id", "wm"]), "wm")
        assert ei.value.api_error is SqlCommonErrorCode.SQL_COMMON_7
        assert "'wm'" in str(ei.value)

    def test_the_marker_is_rejected(self):
        """The marker is read and never written, so it can carry no stamp."""
        with pytest.raises(SqlCommonValidateException) as ei:
            validate_watermark_column(_cfg(op_column="op"), "op")
        assert ei.value.api_error is SqlCommonErrorCode.SQL_COMMON_8
        assert "'op'" in str(ei.value)

    def test_a_collision_only_folding_reveals_is_caught(self):
        """The stored form is what the target is probed for, so a dialect that
        folds case has to compare folded names."""
        cfg = _UpperMergeCfg(on=["ORDER_ID"], op_column="op")
        with pytest.raises(SqlCommonValidateException) as ei:
            validate_watermark_column(cfg, "order_id")
        assert ei.value.api_error is SqlCommonErrorCode.SQL_COMMON_7

    def test_case_differing_names_are_distinct_without_folding(self):
        """The base folds nothing, so a case-sensitive dialect has two columns
        here and this is no collision."""
        assert validate_watermark_column(_cfg(on=["WM"]), "wm") is None
