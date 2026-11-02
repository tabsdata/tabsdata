#
# Copyright 2026 Tabsdata Inc.
#

import logging
from datetime import timezone

import polars as pl

from tabsdatak._medallion.bronze.cdc import OP_DELETE, RESET_OPS
from tabsdatak._medallion.constants import (
    MEDALLION_SYSTEM_COLUMN_BRONZE_TS,
    MEDALLION_SYSTEM_COLUMN_OP,
    MEDALLION_SYSTEM_COLUMN_PK,
    MEDALLION_SYSTEM_COLUMN_SOURCE_TS,
    MEDALLION_SYSTEM_COLUMN_VALID_FROM,
    MEDALLION_SYSTEM_COLUMN_VALID_TO,
)
from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe.functions import col, concat

logger = logging.getLogger(__name__)

_POSITION = "_position"
_SEGMENT = "_segment"
_CLOSING_SEGMENT = "_closing_segment"
_RESET_TS = "_reset_ts"
_NEXT_TS = "_next_ts"
_HELPER_COLUMNS = [_POSITION, _SEGMENT, _CLOSING_SEGMENT, _RESET_TS, _NEXT_TS]


def _is_empty(tf: TableFrame) -> bool:
    """Return True when ``tf`` has no rows (gen-2 has no ``is_empty``)."""
    return tf.first()._to_polars_df().is_empty()


def build_scd2_silver(
    bronze: list[TableFrame],
    valid_head: TableFrame | None,
    expired_head: TableFrame | None,
) -> tuple[TableFrame, TableFrame]:
    """
    SCD2 change-detect for the silver layer.

    Folds CDC bronze rows (I/U/D/T ops) into two SCD2-shaped raw
    tables:

      _valid_raw   — one row per currently-active PK
                     (``@td.medallion.valid_to = None`` on every row).
      _expired_raw — closed versions, with ``@td.medallion.valid_to`` set to the
                     source_ts at which they were superseded.

    The helper is called by a stage-1 silver transformer that writes
    ``_valid_raw`` in REPLACE mode (the helper emits the full new
    state) and ``_expired_raw`` in APPEND mode (only newly-closed
    rows).

    The PK column list is read from the bronze rows' ``@td.medallion.pk`` column
    (stamped by the bronze helpers) — no ``pk`` arg needed.

    Ordering: rows are applied by ``@td.medallion.source_ts``; rows with
    equal timestamps keep their arrival order (commit order, then row order
    within a commit), which is the source's log order for CDC bronze.

    Reset behavior (T sentinel or CDC L load row):
      - A reset empties the table at its position in that order. Every
        version open at that point — prior valid rows and versions opened
        earlier in the batch — closes with
        ``@td.medallion.valid_to = reset.source_ts`` and moves to
        ``_expired_raw``. Rows after the reset start from an empty table.
      - Rows before the reset are applied first, so their versions are kept
        in ``_expired_raw``; nothing is dropped. A batch may hold several
        resets.
      - Reset rows never become versions.

    Versions that close at the instant they open (``valid_from ==
    valid_to``, e.g. two ops on a key in one transaction) are dropped: no
    point in time falls inside them, and keeping them would give two versions
    of a key the same ``valid_from``.

    Bronze metadata (``@td.medallion.op`` / ``@td.medallion.bronze_ts`` /
    ``@td.medallion.pk`` /
    ``@td.medallion.source_ts``) rides through to BOTH outputs unchanged. The
    stage-2 cleansing transformer is responsible for dropping it.

    Args:
        bronze: bronze CDC commits landed since the last silver fire
            (typically read via ``<bronze>@NEW``). Must carry the
            ``@td.medallion.pk`` column the bronze helper stamps.
        valid_head: latest committed ``_valid_raw`` state. None on
            the first-ever silver fire.
        expired_head: latest committed ``_expired_raw`` state. None on the first-ever
            silver fire.

    Returns:
        Tuple of (new_valid_state, new_expired_rows):
          - new_valid_state: full new ``_valid_raw`` state (REPLACE).
          - new_expired_rows: newly-closed versions (APPEND).
    """
    logger.debug(f"bronze length: {len(bronze)}")
    # Drop absent commits (``None`` entries); an empty batch yields no
    # frame to concat (gen-2 ``concat`` rejects an empty list).
    frames = [frame for frame in (bronze or []) if frame is not None]
    incoming = concat(frames, how="diagonal_relaxed") if frames else None

    # No new bronze rows this fire → there's nothing to fold. Return the
    # prior state unchanged. An empty valid_head is legitimate (a reset
    # emptied the table); only a missing one leaves nothing to return.
    if incoming is None or _is_empty(incoming):
        if valid_head is None:
            raise ValueError(
                "build_scd2_silver: bronze input is empty and valid_head is "
                "None — nothing to process."
            )
        return valid_head, expired_head

    incoming = _in_arrival_order(incoming)
    pk_cols = _pk_cols(incoming)

    is_reset = col(MEDALLION_SYSTEM_COLUMN_OP).is_in(list(RESET_OPS))
    ops = incoming.filter(~is_reset)
    # Segment k (k >= 1) starts at the k-th reset; each reset closes the
    # segment before it.
    resets = incoming.filter(is_reset).select([
        col(_SEGMENT).alias(_CLOSING_SEGMENT),
        col(MEDALLION_SYSTEM_COLUMN_SOURCE_TS).alias(_RESET_TS),
    ])
    first_reset_ts = (
        None
        if _is_empty(resets)
        else resets.select(col(_RESET_TS).min())._to_polars_df().item()
    )

    # Each version ends at the next op on its key in the same segment, else
    # at the reset that closes its segment, else it stays open.
    versions = (
        ops
        .sort(*pk_cols, MEDALLION_SYSTEM_COLUMN_SOURCE_TS, _POSITION)
        .with_columns([
            pl
            .col(MEDALLION_SYSTEM_COLUMN_SOURCE_TS)
            .shift(-1)
            .over([*pk_cols, _SEGMENT])
            .alias(_NEXT_TS),
            (pl.col(_SEGMENT) + 1).alias(_CLOSING_SEGMENT),
        ])
        .join(resets, on=_CLOSING_SEGMENT, how="left")
        .filter(col(MEDALLION_SYSTEM_COLUMN_OP) != OP_DELETE)
        .with_columns([
            col(MEDALLION_SYSTEM_COLUMN_SOURCE_TS).alias(
                MEDALLION_SYSTEM_COLUMN_VALID_FROM
            ),
            pl
            .coalesce(pl.col(_NEXT_TS), pl.col(_RESET_TS))
            .cast(pl.Datetime(time_zone=timezone.utc))
            .alias(MEDALLION_SYSTEM_COLUMN_VALID_TO),
        ])
        .drop(_HELPER_COLUMNS)
    )
    # A D op closes the key's previous version (through _next_ts above) and
    # opens nothing.
    new_valid = versions.filter(col(MEDALLION_SYSTEM_COLUMN_VALID_TO).is_null())
    batch_expired = _drop_zero_length(
        versions.filter(col(MEDALLION_SYSTEM_COLUMN_VALID_TO).is_not_null())
    )

    # Bridge into prior valid state: a prior row closes at its key's first
    # op before the first reset, else at the first reset. With no reset,
    # untouched keys carry through.
    prior_valid: TableFrame | None = valid_head
    if prior_valid is None or _is_empty(prior_valid):
        logger.debug("No prior valid state; skipping bridge logic")
        prior_carried: TableFrame | None = None
        bridge_expired: TableFrame | None = None
    else:
        first_op_per_pk = (
            ops
            .filter(col(_SEGMENT) == 0)
            .sort(*pk_cols, MEDALLION_SYSTEM_COLUMN_SOURCE_TS, _POSITION)
            .unique(subset=pk_cols, keep="first")
            .select([*pk_cols, col(MEDALLION_SYSTEM_COLUMN_SOURCE_TS).alias(_NEXT_TS)])
        )
        prior_joined = prior_valid.join(first_op_per_pk, on=pk_cols, how="left")
        if first_reset_ts is not None:
            logger.debug("Reset detected; closing all prior valid rows")
            prior_joined = prior_joined.with_columns(
                pl.col(_NEXT_TS).fill_null(pl.lit(first_reset_ts))
            )
        bridge_expired = _drop_zero_length(
            prior_joined
            .filter(col(_NEXT_TS).is_not_null())
            .with_columns(col(_NEXT_TS).alias(MEDALLION_SYSTEM_COLUMN_VALID_TO))
            .drop(_NEXT_TS)
        )
        prior_carried = prior_joined.filter(col(_NEXT_TS).is_null()).drop(_NEXT_TS)

    # Combine for the two outputs.
    valid_pieces: list[TableFrame] = [new_valid]
    if prior_carried is not None:
        valid_pieces.insert(0, prior_carried)
    new_valid_state = concat(valid_pieces, how="diagonal_relaxed")

    expired_pieces: list[TableFrame] = [batch_expired]
    if bridge_expired is not None:
        expired_pieces.insert(0, bridge_expired)
    if expired_head is not None:
        expired_pieces.insert(0, expired_head)
    new_expired_rows = concat(expired_pieces, how="diagonal_relaxed")

    return new_valid_state, new_expired_rows


def build_scd1_silver(
    bronze: list[TableFrame],
    scd1_head: TableFrame | None,
) -> TableFrame:
    """
    SCD1 change-detect for the silver layer (reference data).

    Folds CDC bronze rows (I/U/D ops, T/L resets) into a single SCD1-shaped
    raw table: one row per PK, always reflecting the latest observed state.
    I/U → upsert; D → drop; a reset (T sentinel or CDC L load row) empties
    the table, so only the ops after the last reset are folded, in the order
    ``build_scd2_silver`` describes.

    The helper is called by a stage-1 silver transformer that writes
    ``_scd1_raw`` in REPLACE mode — the helper emits the full new
    state each fire.

    The PK column list is read from the bronze rows' ``@td.medallion.pk`` column.

    Bronze metadata (``@td.medallion.op`` / ``@td.medallion.bronze_ts`` /
    ``@td.medallion.pk`` /
    ``@td.medallion.source_ts``) rides through unchanged. The stage-2 cleansing
    transformer is responsible for dropping it.

    Args:
        bronze: bronze CDC commits landed since the last silver fire
            (typically read via ``<bronze>@NEW``). Must carry the
            ``@td.medallion.pk`` column the bronze helper stamps.
        scd1_head: latest committed ``_scd1_raw`` state. None on the
            first-ever silver fire.

    Returns:
        Full new ``_scd1_raw`` state (REPLACE).
    """
    frames = [frame for frame in (bronze or []) if frame is not None]
    incoming = concat(frames, how="diagonal_relaxed") if frames else None

    # No new bronze rows this fire → return the prior scd1 state
    # unchanged. An empty scd1_head is legitimate (a reset emptied the
    # table); only a missing one leaves nothing to return.
    if incoming is None or _is_empty(incoming):
        if scd1_head is None:
            raise ValueError(
                "build_scd1_silver: bronze input is empty and scd1_head is "
                "None — nothing to process."
            )
        return scd1_head

    incoming = _in_arrival_order(incoming)
    pk_cols = _pk_cols(incoming)

    last_segment = incoming.select(col(_SEGMENT).max())._to_polars_df().item()
    ops = incoming.filter(
        (col(_SEGMENT) == last_segment)
        & ~col(MEDALLION_SYSTEM_COLUMN_OP).is_in(list(RESET_OPS))
    )
    prior = scd1_head if last_segment == 0 else None

    # SCD1: only the LAST op per PK in this batch matters. I→U→D
    # collapses to "deleted"; I→D→I collapses to "inserted".
    latest = ops.unique(subset=pk_cols, keep="last").drop([_POSITION, _SEGMENT])

    upserts = latest.filter(col(MEDALLION_SYSTEM_COLUMN_OP) != OP_DELETE)
    affected_pks = latest.select(pk_cols)

    if prior is None or _is_empty(prior):
        return upserts

    survivors = prior.join(affected_pks, on=pk_cols, how="anti")
    return concat([survivors, upserts], how="diagonal_relaxed")


def _in_arrival_order(incoming: TableFrame) -> TableFrame:
    """Order bronze rows for folding and number their reset segments.

    Sorts by ``@td.medallion.source_ts``; rows with equal timestamps keep
    their arrival order (``_POSITION``: commit order, then row order), so a
    reset sharing a timestamp with its neighbours stays between them.
    ``_SEGMENT`` counts the resets at or before each row: segment 0 is
    everything before the first reset.
    """
    return (
        incoming
        .with_columns(pl.int_range(pl.len()).alias(_POSITION))
        .sort(MEDALLION_SYSTEM_COLUMN_SOURCE_TS, _POSITION)
        .with_columns(
            pl
            .col(MEDALLION_SYSTEM_COLUMN_OP)
            .is_in(list(RESET_OPS))
            .cast(pl.Int64)
            .cum_sum()
            .alias(_SEGMENT)
        )
    )


def _pk_cols(incoming: TableFrame) -> list[str]:
    """PK column names, read from the first bronze row's ``@td.medallion.pk``.

    ``.item()`` can't be used here — its implementation aggregates via
    ``.min()``, which polars rejects for List dtypes — so we go through
    pandas to extract the cell.
    """
    return list(
        incoming
        .select(col(MEDALLION_SYSTEM_COLUMN_PK))
        ._to_pandas()[MEDALLION_SYSTEM_COLUMN_PK]
        .iloc[0]
    )


def _drop_zero_length(expired: TableFrame) -> TableFrame:
    """Drop versions that close at the instant they open."""
    return expired.filter(
        col(MEDALLION_SYSTEM_COLUMN_VALID_FROM) != col(MEDALLION_SYSTEM_COLUMN_VALID_TO)
    )


def _strip_bronze_metadata(tf: TableFrame) -> TableFrame:
    """
    Drop bronze-only metadata that silver shouldn't carry forward:
    ``@td.medallion.op``, ``@td.medallion.bronze_ts``, and any ``@td.cdc.meta.*``
    columns
    inherited from the upstream CDC trigger (e.g. ``@td.cdc.meta.txid``,
    ``@td.cdc.meta.lsn``). The CDC bronze helper renames the two
    meaningful ones (``.op`` / ``.ts``) into the ``@td.medallion.*`` namespace
    but passes the rest through verbatim — silver strips them here.

    Args:
        tf: TableFrame with bronze metadata columns to drop.
    Returns:
        TableFrame with the bronze metadata columns dropped.
    """
    cdc_meta_cols = [c for c in tf.columns if c.startswith("@td.cdc")]
    # Periodic-snapshot bronze carries no @td.medallion.op, so drop only what's present.
    drop_cols = [
        c
        for c in (
            MEDALLION_SYSTEM_COLUMN_OP,
            MEDALLION_SYSTEM_COLUMN_BRONZE_TS,
            *cdc_meta_cols,
        )
        if c in tf.columns
    ]
    return tf.drop(drop_cols)


def build_scd2_silver_cleanse(
    valid_raw: list[TableFrame],
    expired_raw: list[TableFrame],
) -> tuple[TableFrame, TableFrame]:
    """
    SCD2 silver cleansing — pure projection: drops bronze metadata
    from both ``_scd2_valid_raw`` and ``_scd2_expired_raw`` inputs.

    Strips ``@td.medallion.op``, ``@td.medallion.bronze_ts``, and any leftover
    ``@td.cdc.*`` columns the upstream CDC trigger carried into
    bronze. Per-source cleansing (casts, renames, projections) is the
    caller's responsibility on top of the helper's output.

    Args:
        valid_raw: ``_scd2_valid_raw@NEW`` commits.
        expired_raw: ``_scd2_expired_raw@NEW`` commits.

    Returns:
        Tuple of (valid_cleaned, expired_cleaned).
    """
    valid = _strip_bronze_metadata(concat(valid_raw, how="diagonal_relaxed"))
    expired = _strip_bronze_metadata(concat(expired_raw, how="diagonal_relaxed"))
    return valid, expired


def build_scd1_silver_cleanse(
    scd1_raw: list[TableFrame],
) -> TableFrame:
    """
    SCD1 silver cleansing — pure projection: drops bronze metadata
    from the ``_scd1_raw`` input.

    Strips ``@td.medallion.op``, ``@td.medallion.bronze_ts``, and any leftover
    ``@td.cdc.meta.*`` columns the upstream CDC trigger carried into
    bronze. Per-source cleansing (casts, renames, projections) is the
    caller's responsibility on top of the helper's output.

    Args:
        scd1_raw: ``_scd1_raw@NEW`` commits.

    Returns:
        Cleansed SCD1 silver TableFrame.
    """
    return _strip_bronze_metadata(concat(scd1_raw, how="diagonal_relaxed"))
