#
# Copyright 2026 Tabsdata Inc.
#

"""A column of driver values built as a Polars series.

A described column is built straight into its dtype. When a driver hands back
values Polars cannot build that dtype from -- a ``memoryview`` for bytes, a
``dict`` for JSON text, a ``set`` for a MySQL SET, an interval object, a NaN
decimal -- they are converted to the values the dtype holds, and only then, as
a last resort, built as they come and cast, as the reader always did. A column
nothing describes is inferred over all its values.
"""

from __future__ import annotations

import base64
import datetime as dt
import json
from collections.abc import Sequence
from decimal import Decimal
from typing import Any, NamedTuple

import polars as pl

_DAYS_PER_YEAR = 365
_DAYS_PER_MONTH = 30

# What Polars infers a column from as it is; anything else is made text.
_BUILDABLE = (
    str,
    bytes,
    bool,
    int,
    float,
    Decimal,
    dt.date,
    dt.time,
    dt.timedelta,
    list,
    tuple,
    dict,
)

# A placeholder no JSON document holds: json.dumps escapes the NUL characters.
_NUMBER = "\x00{}\x00"


class Built(NamedTuple):
    """A column's series; how many of its decimals were not finite -- NaN or an
    infinity -- and so became null; and whether its decimals, inferred, were
    too wide for a decimal column and so became text."""

    series: pl.Series
    nulled: int = 0
    wide: bool = False


def build(name: str, values: Sequence[Any], dtype: pl.DataType | None) -> Built:
    decimals = isinstance(dtype, pl.Decimal) or (
        dtype is None and isinstance(_first(values), Decimal)
    )
    nulled = 0
    if decimals:
        values, nulled = _finite(values)
    if dtype is None:
        series = _inferred(name, values)
        if series is None:
            return Built(
                pl.Series(name, [_text_or_none(v) for v in values]), nulled, True
            )
        return Built(series, nulled)
    first = _first(values)
    if (
        isinstance(dtype, pl.String)
        and first is not None
        and not isinstance(first, str)
    ):
        # Building text from a parsed document can panic Polars on a NaN
        # inside it, so the values are made text before anything is built.
        values = [_text_or_none(value) for value in values]
    try:
        series = pl.Series(name, values, dtype=dtype, strict=True)
    except Exception:  # noqa: BLE001 -- Polars raises several kinds; all mean "convert"
        try:
            converted = [convert(value, dtype) for value in values]
            series = pl.Series(name, converted, dtype=dtype, strict=True)
        except Exception:  # noqa: BLE001 -- the cast below reports what cannot fit
            series = pl.Series(name, values, strict=False).cast(dtype)
    if series.dtype != dtype:
        series = series.cast(dtype)
    return Built(series, nulled)


def _inferred(name: str, values: Sequence[Any]) -> pl.Series | None:
    """The column inferred from its values; ``None`` when they are decimals too
    wide for a decimal column."""
    decimals = isinstance(_first(values), Decimal)
    try:
        series = pl.Series(name, values, strict=False)
    except Exception:  # noqa: BLE001 -- a decimal too wide fails; anything else re-raises
        if decimals:
            return None
        raise
    if series.dtype == pl.Object:
        if decimals:
            return None
        series = pl.Series(name, [convert(value) for value in values], strict=False)
    return series


def _first(values: Sequence[Any]) -> Any:
    return next((value for value in values if value is not None), None)


def _text_or_none(value: Any) -> str | None:
    return None if value is None else _text(value)


def _finite(values: Sequence[Any]) -> tuple[Sequence[Any], int]:
    """A decimal column with every NaN and infinity made null: Polars decimals
    hold neither, and building one from them panics instead of failing."""
    nulled = sum(
        1 for value in values if isinstance(value, Decimal) and not value.is_finite()
    )
    if not nulled:
        return values, 0
    finite = [
        None if isinstance(value, Decimal) and not value.is_finite() else value
        for value in values
    ]
    return finite, nulled


def convert(value: Any, dtype: pl.DataType | None = None) -> Any:
    """``value`` as a value ``dtype`` is built from; with no ``dtype``, only
    what no dtype can be built from (bytes views, identifiers, addresses) is
    converted."""
    if value is None:
        return None
    if isinstance(value, (memoryview, bytearray)):
        value = bytes(value)
    if dtype is None:
        return value if isinstance(value, _BUILDABLE) else _text(value)
    if isinstance(dtype, pl.String):
        return _text(value)
    if dtype.is_integer() and isinstance(value, bytes):
        return int.from_bytes(value, "big")
    if isinstance(dtype, pl.Duration) and _is_interval(value):
        return _timedelta(value)
    if isinstance(dtype, pl.Struct) and hasattr(value, "_asdict"):
        return value._asdict()
    if isinstance(dtype, pl.List) and not isinstance(value, (list, str, bytes)):
        return list(value)
    return value


def _text(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (dict, list, tuple)):
        return to_json(value)
    if isinstance(value, (set, frozenset)):
        return ",".join(sorted(str(member) for member in value))
    if isinstance(value, (dt.date, dt.time)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _is_interval(value: Any) -> bool:
    """pg8000's interval: an object with a field per unit, months included."""
    return not isinstance(value, dt.timedelta) and all(
        hasattr(value, unit) for unit in ("years", "months", "days", "microseconds")
    )


def _timedelta(value: Any) -> dt.timedelta:
    """An interval with months as a timedelta, a month taken as 30 days and a
    year as 365, as psycopg2 converts them."""

    def unit(name: str) -> float:
        return getattr(value, name, None) or 0

    years = (
        unit("millennia") * 1000
        + unit("centuries") * 100
        + unit("decades") * 10
        + unit("years")
    )
    return dt.timedelta(
        days=years * _DAYS_PER_YEAR
        + unit("months") * _DAYS_PER_MONTH
        + unit("weeks") * 7
        + unit("days"),
        hours=unit("hours"),
        minutes=unit("minutes"),
        seconds=unit("seconds"),
        microseconds=unit("microseconds"),
    )


def to_json(value: Any) -> str:
    """JSON text for a document a driver parsed. Its decimals are written as
    the numbers they are, not as floats: Oracle hands every JSON number back as
    a ``Decimal``."""
    numbers: list[str] = []

    def plain(item: Any) -> Any:
        if isinstance(item, dict):
            return {str(key): plain(member) for key, member in item.items()}
        if isinstance(item, (list, tuple)):
            return [plain(member) for member in item]
        if isinstance(item, Decimal):
            if not item.is_finite():
                return None
            numbers.append(format(item, "f"))
            return _NUMBER.format(len(numbers) - 1)
        if isinstance(item, (dt.date, dt.time)):
            return item.isoformat()
        if isinstance(item, dt.timedelta):
            return str(item)
        if isinstance(item, (bytes, bytearray, memoryview)):
            return base64.b64encode(bytes(item)).decode("ascii")
        return item

    text = json.dumps(plain(value), ensure_ascii=False)
    for index, number in enumerate(numbers):
        text = text.replace(json.dumps(_NUMBER.format(index)), number, 1)
    return text
