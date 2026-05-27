#
# Copyright 2026 Tabs Data Inc.
#

"""Tests for `tabsdatak.server._tdrunner.function._lib.system_columns`.

Constants, UUID v7 generator, and the `stamp_system_columns` helper
that `stamp-inputs` calls per parquet.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

import polars as pl
import pytest

from tabsdatak.server._tdrunner.function._lib.system_columns import (
    EMPTY_EXECUTION,
    EMPTY_TIMESTAMP,
    EMPTY_TRANSACTION,
    EMPTY_VERSION,
    SYSTEM_COLUMNS,
    TD_IDENTIFIER,
    TD_VER_EXECUTION,
    TD_VER_TIMESTAMP,
    TD_VER_TRANSACTION,
    TD_VER_VERSION,
    _uuid7,
    stamp_system_columns,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


# --- constants ----------------------------------------------------------


def test_column_names_match_legacy_schema():
    """Names must match `tabsdata._utils.tableframe._constants.
    StandardSystemColumns` so the on-wire shape stays compatible."""
    assert TD_IDENTIFIER == "$td.id"
    assert TD_VER_EXECUTION == "$td.ver.execution"
    assert TD_VER_TRANSACTION == "$td.ver.transaction"
    assert TD_VER_VERSION == "$td.ver.version"
    assert TD_VER_TIMESTAMP == "$td.ver.timestamp"


def test_system_columns_tuple_lists_all_five():
    assert set(SYSTEM_COLUMNS) == {
        TD_IDENTIFIER,
        TD_VER_EXECUTION,
        TD_VER_TRANSACTION,
        TD_VER_VERSION,
        TD_VER_TIMESTAMP,
    }


def test_empty_uuid_v7_defaults_are_canonical_zero_uuid():
    zero = "00000000-0000-0000-0000-000000000000"
    assert EMPTY_EXECUTION == zero
    assert EMPTY_TRANSACTION == zero
    assert EMPTY_VERSION == zero


def test_empty_timestamp_is_epoch_utc():
    assert EMPTY_TIMESTAMP == datetime(1970, 1, 1, tzinfo=timezone.utc)


# --- _uuid7 -------------------------------------------------------------

# UUID v7 canonical form: 8-4-4-4-12 hex digits.
_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)


def test_uuid7_format_canonical():
    u = _uuid7()
    assert _UUID_RE.match(u), f"not canonical UUID format: {u}"


def test_uuid7_version_nibble_is_7():
    """Per RFC 9562 §5.7, the 13th hex digit (first nibble of group 3)
    is the version, which must be 7 for v7."""
    u = _uuid7()
    # u looks like xxxxxxxx-xxxx-Vxxx-xxxx-xxxxxxxxxxxx; V is index 14 (after 2 hyphens)
    parts = u.split("-")
    assert parts[2][0] == "7", f"version nibble not 7: {u}"


def test_uuid7_variant_bits():
    """Per RFC 9562, the first 2 bits of group 4 (the `N` digit) must
    be 0b10, i.e. the hex value is 8, 9, a, or b."""
    u = _uuid7()
    parts = u.split("-")
    assert parts[3][0] in "89ab", f"variant nibble out of range: {u}"


def test_uuid7_values_are_unique():
    """Two consecutive calls should never collide."""
    ids = {_uuid7() for _ in range(100)}
    assert len(ids) == 100


def test_uuid7_values_are_time_ordered():
    """The 48-bit timestamp prefix sorts lexicographically. Two ids
    generated back-to-back must satisfy `earlier <= later` when
    compared as strings."""
    earlier = _uuid7()
    later = _uuid7()
    # Compare the first 8 hex chars (32 bits of the timestamp prefix).
    assert earlier[:8] <= later[:8]


# --- stamp_system_columns -----------------------------------------------


def _user_df() -> pl.DataFrame:
    return pl.DataFrame({
        "id": [1, 2, 3],
        "name": ["alice", "bob", "carol"],
    })


def _stamp(df: pl.DataFrame, *, ts=None) -> pl.DataFrame:
    return stamp_system_columns(
        df,
        execution="exec-1",
        transaction="tx-1",
        version=EMPTY_VERSION,
        timestamp=ts or EMPTY_TIMESTAMP,
    )


def test_adds_all_five_system_columns():
    stamped = _stamp(_user_df())
    for col in SYSTEM_COLUMNS:
        assert col in stamped.columns, f"missing system column: {col}"


def test_preserves_user_columns():
    stamped = _stamp(_user_df())
    assert "id" in stamped.columns
    assert "name" in stamped.columns
    assert stamped["id"].to_list() == [1, 2, 3]
    assert stamped["name"].to_list() == ["alice", "bob", "carol"]


def test_idx_column_is_per_row_unique():
    stamped = _stamp(_user_df())
    ids = stamped[TD_IDENTIFIER].to_list()
    assert len(ids) == 3
    assert len(set(ids)) == 3


def test_idx_column_values_are_uuid_v7_strings():
    stamped = _stamp(_user_df())
    for v in stamped[TD_IDENTIFIER].to_list():
        assert _UUID_RE.match(v), f"not UUID format: {v}"


def test_scalar_columns_broadcast_uniformly():
    stamped = _stamp(_user_df())
    exec_values = set(stamped[TD_VER_EXECUTION].to_list())
    tx_values = set(stamped[TD_VER_TRANSACTION].to_list())
    ver_values = set(stamped[TD_VER_VERSION].to_list())
    assert exec_values == {"exec-1"}
    assert tx_values == {"tx-1"}
    assert ver_values == {EMPTY_VERSION}


def test_timestamp_dtype_is_us_utc():
    stamped = _stamp(_user_df())
    dtype = stamped.schema[TD_VER_TIMESTAMP]
    assert isinstance(dtype, pl.Datetime)
    assert dtype.time_unit == "us"
    assert dtype.time_zone == "UTC"


def test_existing_system_columns_are_dropped_and_replaced():
    """If the input already has any framework column, the stamper
    must overwrite — not duplicate or merge."""
    df = _user_df().with_columns(
        pl.lit("stale", dtype=pl.String).alias(TD_VER_EXECUTION),
        pl.lit("garbage", dtype=pl.String).alias(TD_IDENTIFIER),
    )
    stamped = _stamp(df)
    # Stale execution value is gone.
    assert set(stamped[TD_VER_EXECUTION].to_list()) == {"exec-1"}
    # Idx values are fresh per-row UUIDs, not the stale "garbage".
    ids = stamped[TD_IDENTIFIER].to_list()
    assert all(v != "garbage" for v in ids)
    assert all(_UUID_RE.match(v) for v in ids)


def test_empty_dataframe_still_gets_columns():
    """A zero-row input yields a zero-row output but the schema must
    still include all five system columns."""
    df = pl.DataFrame({"id": [], "name": []}, schema={"id": pl.Int64, "name": pl.String})
    stamped = _stamp(df)
    for col in SYSTEM_COLUMNS:
        assert col in stamped.columns
    assert stamped.height == 0


def test_stamping_does_not_mutate_input():
    df = _user_df()
    original_cols = set(df.columns)
    _stamp(df)
    assert set(df.columns) == original_cols
