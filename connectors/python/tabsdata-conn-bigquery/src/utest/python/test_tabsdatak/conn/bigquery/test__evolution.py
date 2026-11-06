#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from decimal import Decimal

import polars as pl
import pyarrow.parquet as pq
import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.bigquery._evolution import narrowed

pytestmark = pytest.mark.bigquery


def _footer(tmp_path, values: list, precision: int = 38, scale: int = 2):
    path = tmp_path / "batch.parquet"
    pl.DataFrame(
        {"amount": [None if v is None else Decimal(v) for v in values]},
        schema={"amount": pl.Decimal(precision, scale)},
    ).write_parquet(path)
    return pq.read_metadata(path)


@pytest.mark.parametrize(
    "stored,values",
    [
        ("NUMERIC", ["-" + "9" * 29 + ".99", "9" * 29 + ".99"]),
        ("NUMERIC", [None]),
        ("NUMERIC(10, 2)", ["-99999999.99", "99999999.99"]),
    ],
)
def test_a_narrower_column_holding_every_value_is_taken(tmp_path, stored, values):
    footer = _footer(tmp_path, values)
    assert narrowed([("amount", stored)], footer) == {"amount": stored}


@pytest.mark.parametrize(
    "stored,values,scale",
    [
        ("NUMERIC", ["1" + "0" * 29], 2),
        ("NUMERIC(10, 2)", ["100000000.00"], 2),
        # NUMERIC keeps 9 fractional digits: a tenth would be rounded away.
        ("NUMERIC", ["1.0000000000"], 10),
    ],
)
def test_a_narrower_column_not_holding_every_value_is_not(
    tmp_path, stored, values, scale
):
    footer = _footer(tmp_path, values, scale=scale)
    assert narrowed([("amount", stored)], footer) == {}


@pytest.mark.parametrize("stored", ["BIGNUMERIC", "FLOAT64", "STRING"])
def test_a_column_the_batch_type_already_fits_is_left_alone(tmp_path, stored):
    footer = _footer(tmp_path, ["1.25"])
    assert narrowed([("amount", stored)], footer) == {}


def test_a_column_the_target_lacks_is_left_alone(tmp_path):
    assert narrowed([("other", "NUMERIC")], _footer(tmp_path, ["1.25"])) == {}
