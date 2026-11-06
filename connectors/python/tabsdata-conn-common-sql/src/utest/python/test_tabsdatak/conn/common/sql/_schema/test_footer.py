#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from decimal import Decimal

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._schema.footer import column_range


def _footer(tmp_path, frame: pl.DataFrame, name: str = "f.parquet", **options):
    path = tmp_path / name
    frame.write_parquet(path, **options)
    return pq.read_metadata(path)


def test_the_range_spans_every_row_group_and_file(tmp_path):
    first = _footer(
        tmp_path, pl.DataFrame({"n": [5, -3, 9, 1]}), "a.parquet", row_group_size=2
    )
    second = _footer(tmp_path, pl.DataFrame({"n": [40]}), "b.parquet")
    assert first.num_row_groups == 2
    assert column_range([first, second], "n") == (-3, 40)


def test_a_decimal_range_is_exact(tmp_path):
    frame = pl.DataFrame(
        {"d": [Decimal("-12345678.25"), Decimal("99999999.99")]},
        schema={"d": pl.Decimal(38, 2)},
    )
    assert column_range([_footer(tmp_path, frame)], "d") == (
        Decimal("-12345678.25"),
        Decimal("99999999.99"),
    )


def test_nulls_do_not_widen_the_range(tmp_path):
    frame = pl.DataFrame({"n": [None, 7, None]}, schema={"n": pl.Int64})
    assert column_range([_footer(tmp_path, frame)], "n") == (7, 7)


def test_a_column_of_nulls_only_has_no_range(tmp_path):
    frame = pl.DataFrame({"n": [None, None]}, schema={"n": pl.Int64})
    assert column_range([_footer(tmp_path, frame)], "n") == (None, None)


def test_a_column_without_statistics_cannot_be_bounded(tmp_path):
    path = tmp_path / "plain.parquet"
    pq.write_table(pa.table({"n": [1, 2]}), path, write_statistics=False)
    assert column_range([pq.read_metadata(path)], "n") is None


def test_no_footer_or_no_column_cannot_be_bounded(tmp_path):
    footer = _footer(tmp_path, pl.DataFrame({"n": [1]}))
    assert column_range([], "n") is None
    assert column_range([footer], "other") is None
