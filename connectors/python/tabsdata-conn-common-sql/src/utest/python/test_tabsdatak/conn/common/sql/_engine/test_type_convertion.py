from __future__ import annotations

import pytest

from tabsdatak.conn.common.types import DataType
# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine import type_convertion as tc


# ---------------------------------------------------------------------------
# polars_dtype
# ---------------------------------------------------------------------------


def test_polars_dtype_unmapped_raises_value_error():
    class _BogusDataType:
        def __repr__(self) -> str:
            return "BOGUS"

    with pytest.raises(ValueError, match="unsupported DataType"):
        tc.polars_dtype(_BogusDataType())


def test_polars_dtype_known_values_round_trip_to_polars():
    import polars as pl

    assert tc.polars_dtype(DataType.BOOL) is pl.Boolean
    assert tc.polars_dtype(DataType.INT) is pl.Int64
    assert tc.polars_dtype(DataType.FLOAT) is pl.Float64
    assert tc.polars_dtype(DataType.STRING) is pl.Utf8
    assert tc.polars_dtype(DataType.BYTES) is pl.Binary
    assert tc.polars_dtype(DataType.DATE) is pl.Date
    assert tc.polars_dtype(DataType.TIME) is pl.Time
    assert tc.polars_dtype(DataType.DATETIME) is pl.Datetime


# ---------------------------------------------------------------------------
# resolve_schema_overrides
# ---------------------------------------------------------------------------


def test_resolve_schema_overrides_none_returns_none():
    assert tc.resolve_schema_overrides(None) is None


def test_resolve_schema_overrides_maps_each_column():
    import polars as pl

    out = tc.resolve_schema_overrides(
        {"a": DataType.INT, "b": DataType.STRING},
    )
    assert out == {"a": pl.Int64, "b": pl.Utf8}


def test_resolve_schema_overrides_empty_dict_returns_empty_dict():
    assert tc.resolve_schema_overrides({}) == {}
