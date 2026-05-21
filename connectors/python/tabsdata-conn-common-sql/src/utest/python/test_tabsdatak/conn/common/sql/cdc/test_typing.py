"""Public typing primitives for SQL-CDC -- ``CdcFormat`` and
``TimestampPosition`` validate via the framework's dataclass
machinery; ``RelativePositionSpec`` is a Literal alias."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from tabsdatak.conn.common.sql.cdc.error import SqlCdcInitException
from tabsdatak.conn.common.sql.cdc.typing import (
    CdcFormat,
    RelativePositionSpec,
    TimestampPosition,
)


# --- CdcFormat -------------------------------------------------------


def test_cdc_format_defaults():
    fmt = CdcFormat()
    assert fmt.values_format == "columns"
    assert fmt.flatten_values is True


def test_cdc_format_kw_only_columns():
    fmt = CdcFormat(values_format="columns", flatten_values=False)
    assert fmt.values_format == "columns"
    assert fmt.flatten_values is False


def test_cdc_format_accepts_struct_and_map():
    assert CdcFormat(values_format="struct").values_format == "struct"
    assert CdcFormat(values_format="map").values_format == "map"


def test_cdc_format_rejects_unknown_values_format():
    with pytest.raises(SqlCdcInitException):
        CdcFormat(values_format="bogus")  # type: ignore[arg-type]


def test_cdc_format_is_frozen():
    fmt = CdcFormat()
    # Frozen pydantic dataclass: setattr raises (FrozenInstanceError
    # or pydantic-flavoured equivalent wrapped by ApiException).
    with pytest.raises(Exception):
        fmt.values_format = "struct"  # type: ignore[misc]


# --- TimestampPosition ----------------------------------------------


def test_timestamp_position_accepts_aware_datetime():
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    tp = TimestampPosition(ts=ts)
    assert tp.ts == ts


def test_timestamp_position_rejects_non_datetime():
    # Pydantic happily parses ISO strings into datetimes, so use a
    # shape it cannot coerce.
    with pytest.raises(SqlCdcInitException):
        TimestampPosition(ts=object())  # type: ignore[arg-type]


# --- RelativePositionSpec -------------------------------------------


def test_relative_position_literal_values():
    # `RelativePositionSpec` is a Literal["head", "tail"]; runtime
    # values are just strings, the framework enforces the literal
    # via field-site annotations on consumer dataclasses.
    head: RelativePositionSpec = "head"
    tail: RelativePositionSpec = "tail"
    assert head == "head"
    assert tail == "tail"
