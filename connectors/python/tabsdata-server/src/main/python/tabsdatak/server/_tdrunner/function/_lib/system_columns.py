#
# Copyright 2026 Tabs Data Inc.
#

# ============================================================================
# TODO (MIGRATION CLEANUP — DO NOT FORGET): DELETE THIS ENTIRE FILE
# ============================================================================
# This module is a TEMPORARY pure-Python port of a subset of:
#   - `tabsdata._utils.tableframe._constants.StandardSystemColumns`
#   - `tabsdata._utils.tableframe._constants` (EMPTY_* sentinel values)
#   - `tabsdata._utils.tableframe._generators` (UUID v7 generator)
#
# Why temporary: per the in-flight migration plan, tabsdatak must be
# standalone (no imports from the legacy `tabsdata` namespace). At the
# time of writing, the proper `tabsdatak.tableframe` package has not
# yet been ported, but `stamp-inputs` (this module's only consumer)
# needs system-column constants + a UUID v7 generator NOW to stamp the
# enriched parquets in a way that matches the legacy
# `store_source_raw_data` shape.
#
# WHEN `tabsdatak.tableframe` lands:
#   1. Delete this entire file.
#   2. Update every importer of `tabsdatak.server._tdrunner.function._lib.
#      system_columns` (currently just `commands/stamp_inputs/`) to
#      import the same names from `tabsdatak.tableframe`.
#   3. The Rust-side UUID v7 generator (legacy
#      `td_generators.IdGenerator`, `Language.RUST`) replaces the
#      pure-Python `_uuid7()` helper below — see the second TODO in
#      this file. The Rust generator produces canonical UUID v7 values
#      with the same wire format but better throughput.
# ============================================================================

"""Framework system-column names, defaults, and stamping helper.

`stamp-inputs` calls `stamp_system_columns()` on each parquet the
source plugin produced, adding the five framework-managed system
columns the legacy `store_source_raw_data` adds:

    $td.id              — per-row UUID v7 (time-ordered identifier)
    $td.ver.execution   — request.info.execution_id, broadcast
    $td.ver.transaction — request.info.transaction_id, broadcast
    $td.ver.version     — EMPTY_VERSION constant, broadcast
    $td.ver.timestamp   — request triggered_on timestamp, broadcast

Until the proper `tabsdatak.tableframe` port lands (see file-level
TODO above), this module owns those names + defaults + the temporary
pure-Python UUID v7 generator.
"""

from __future__ import annotations

import secrets
import time
from datetime import datetime, timezone

import polars as pl

# --- system-column names (ported verbatim from legacy) --------------------

TD_IDENTIFIER = "$td.id"
TD_VER_EXECUTION = "$td.ver.execution"
TD_VER_TRANSACTION = "$td.ver.transaction"
TD_VER_VERSION = "$td.ver.version"
TD_VER_TIMESTAMP = "$td.ver.timestamp"

SYSTEM_COLUMNS: tuple[str, ...] = (
    TD_IDENTIFIER,
    TD_VER_EXECUTION,
    TD_VER_TRANSACTION,
    TD_VER_VERSION,
    TD_VER_TIMESTAMP,
)

# --- empty / sentinel defaults --------------------------------------------
#
# Used when a real value isn't available yet (e.g. fixture parquets in
# tests, or downstream consumers that need a stable "no data" sentinel).

_EMPTY_UUID_V7 = "00000000-0000-0000-0000-000000000000"

EMPTY_EXECUTION = _EMPTY_UUID_V7
EMPTY_TRANSACTION = _EMPTY_UUID_V7
EMPTY_VERSION = _EMPTY_UUID_V7
EMPTY_TIMESTAMP = datetime(1970, 1, 1, tzinfo=timezone.utc)

# Polars dtype for `$td.ver.timestamp`. The legacy uses
# `pl.Datetime(time_unit="us", time_zone=timezone.utc)`; we mirror that
# so the on-wire parquet schema matches byte-for-byte.
_TIMESTAMP_DTYPE = pl.Datetime(time_unit="us", time_zone="UTC")


# --- UUID v7 generator ----------------------------------------------------


def _uuid7() -> str:
    """Generate one UUID v7 (time-ordered, 128-bit) per RFC 9562 §5.7.

    Format:
      * 48 bits  : Unix timestamp in milliseconds (sortable prefix)
      * 4  bits  : version (0b0111 = 7)
      * 12 bits  : random
      * 2  bits  : variant (0b10)
      * 62 bits  : random

    -------------------------------------------------------------------
    TODO (MIGRATION CLEANUP — DO NOT FORGET): REPLACE WITH RUST GENERATOR
    -------------------------------------------------------------------
    This is a temporary pure-Python implementation. The legacy stack
    uses a Rust-side UUID v7 generator (`td_generators.IdGenerator`,
    `Language.RUST` in `StandardSystemColumnsMetadata.TD_IDENTIFIER`)
    for throughput and consistency with the rest of the framework's
    rust ID-generation path.

    When `tabsdatak.tableframe` is ported (see file-level TODO at the
    top of this module), drop this function and have `stamp-inputs`
    use the ported Rust-backed generator instead. The wire format is
    identical (canonical UUID v7), so consumers of the resulting
    parquets won't change.
    -------------------------------------------------------------------
    """
    ts_ms = int(time.time() * 1000)
    rand_a = secrets.randbits(12)
    rand_b = secrets.randbits(62)

    # 128 bits packed: [ts48][ver4][randA12][var2][randB62]
    value = (ts_ms << 80) | (0x7 << 76) | (rand_a << 64) | (0b10 << 62) | rand_b
    hex_str = f"{value:032x}"
    return (
        f"{hex_str[:8]}-{hex_str[8:12]}-{hex_str[12:16]}-"
        f"{hex_str[16:20]}-{hex_str[20:]}"
    )


# --- the stamping helper -------------------------------------------------


def stamp_system_columns(
    df: pl.DataFrame,
    *,
    execution: str,
    transaction: str,
    version: str,
    timestamp: datetime,
) -> pl.DataFrame:
    """Add (or overwrite) the five framework system columns on `df`.

    Any pre-existing columns with the same names are dropped first so
    the framework-stamped values always win — matches legacy
    `store_source_raw_data`'s "mode='raw'" re-stamping semantics.

    The four version columns (`execution` / `transaction` / `version` /
    `timestamp`) are broadcast across all rows. `$td.id` is a per-row
    UUID v7 (time-ordered).

    Args:
        df: The parquet contents.
        execution: Scalar value for `$td.ver.execution` (string).
        transaction: Scalar value for `$td.ver.transaction` (string).
        version: Scalar value for `$td.ver.version` (string).
        timestamp: Scalar value for `$td.ver.timestamp` (timezone-aware
            datetime; cast to `Datetime[us, UTC]` on the way in).

    Returns:
        A new `pl.DataFrame` with the system columns appended/replaced.
    """
    to_drop = [c for c in SYSTEM_COLUMNS if c in df.columns]
    if to_drop:
        df = df.drop(to_drop)

    ids = [_uuid7() for _ in range(df.height)]

    return df.with_columns(
        pl.Series(TD_IDENTIFIER, ids, dtype=pl.String),
        pl.lit(execution, dtype=pl.String).alias(TD_VER_EXECUTION),
        pl.lit(transaction, dtype=pl.String).alias(TD_VER_TRANSACTION),
        pl.lit(version, dtype=pl.String).alias(TD_VER_VERSION),
        pl.lit(timestamp).cast(_TIMESTAMP_DTYPE).alias(TD_VER_TIMESTAMP),
    )
