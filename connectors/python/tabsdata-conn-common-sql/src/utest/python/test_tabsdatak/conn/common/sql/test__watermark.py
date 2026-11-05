#
#  Copyright 2026 Tabsdata Inc.
#

"""Unit tests for the shared watermark helpers.

The batch rewrite every bulk load stages through, and the checks a destination
makes on the watermark it names against the columns its merge already reads.
"""

from __future__ import annotations

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


def _batch(**columns) -> pl.LazyFrame:
    return pl.LazyFrame(columns or {"id": [1, 2]})


def _cfg(**overrides) -> MergeCfg:
    kwargs: dict = dict(on=["order_id"], op_column="op")
    kwargs.update(overrides)
    return MergeCfg(**kwargs)


# --- watermarked_batch -----------------------------------------------


class TestWatermarkedBatch:
    def test_every_row_carries_the_trx_id(self):
        out = watermarked_batch(_batch(), "wm", _TRX).collect()
        assert out["wm"].to_list() == [_TRX, _TRX]

    def test_the_column_is_a_string(self):
        """A trx id is an opaque token, and every dialect declares the column a
        string type, so the batch has to agree."""
        out = watermarked_batch(_batch(), "wm", _TRX)
        assert out.collect_schema()["wm"] == pl.String

    def test_the_batch_columns_are_kept(self):
        batch = _batch(order_id=[1, 2], status=["a", "b"])
        out = watermarked_batch(batch, "wm", _TRX)
        assert out.collect_schema().names() == ["order_id", "status", "wm"]

    def test_no_column_hands_back_the_batch_itself(self):
        batch = _batch()
        assert watermarked_batch(batch, None, _TRX) is batch

    def test_an_empty_batch_still_gains_the_column(self):
        out = watermarked_batch(pl.LazyFrame({"id": []}), "wm", _TRX).collect()
        assert out.height == 0
        assert "wm" in out.columns

    def test_the_adapt_rewrite_rides_the_stamp(self):
        out = watermarked_batch(
            _batch(id=[1, 2]),
            "wm",
            _TRX,
            adapt=lambda frame: frame.with_columns(pl.col("id") * 10),
        ).collect()
        assert out["id"].to_list() == [10, 20]
        assert out["wm"].to_list() == [_TRX, _TRX]

    def test_an_adapt_alone_rewrites(self):
        out = watermarked_batch(
            _batch(id=[1]),
            None,
            _TRX,
            adapt=lambda frame: frame.with_columns(pl.col("id") * 10),
        )
        assert out.collect()["id"].to_list() == [10]


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
