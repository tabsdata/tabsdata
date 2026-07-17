#
# Copyright 2026 Tabsdata Inc.
#

import logging
from datetime import timezone

import polars as pl

from tabsdatak._medallion._bronze._cdc import OP_DELETE, OP_TOMBSTONE
from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe.functions import col, concat, lit

logger = logging.getLogger(__name__)


def _is_empty(tf: TableFrame) -> bool:
    """Return True when ``tf`` has no rows (gen-2 has no ``is_empty``)."""
    return tf.first().to_polars_df().is_empty()


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
                     (``$m.valid_to = None`` on every row).
      _expired_raw — closed versions, with ``$m.valid_to`` set to the
                     source_ts at which they were superseded.

    The helper is called by a stage-1 silver transformer that writes
    ``_valid_raw`` in REPLACE mode (the helper emits the full new
    state) and ``_expired_raw`` in APPEND mode (only newly-closed
    rows).

    The PK column list is read from the bronze rows' ``$m.pk`` column
    (stamped by the bronze helpers) — no ``pk`` arg needed.

    T-sentinel behavior:
      - The latest T in the batch acts as a hard reset of the valid
        state. Ops at-or-before the latest T's source_ts are dropped;
        ops after are re-folded into the new ``_valid_raw``.
      - ALL prior-valid rows are moved into ``_expired_raw`` with
        ``$m.valid_to = T.source_ts``. Nothing disappears — the T
        is recorded as the closing event for every active row.

    Bronze metadata (``$m.op`` / ``$m.bronze_ts`` / ``$m.pk`` /
    ``$m.source_ts``) rides through to BOTH outputs unchanged. The
    stage-2 cleansing transformer is responsible for dropping it.

    Args:
        bronze: bronze CDC commits landed since the last silver fire
            (typically read via ``<bronze>@NEW``). Must carry the
            ``$m.pk`` column the bronze helper stamps.
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

    # No new bronze rows this fire → there's nothing to fold. Return
    # the prior valid state unchanged plus an empty expired frame
    # (same schema as valid_head so the caller's write of
    # `_expired_raw` doesn't drift from the existing schema).
    if incoming is None or _is_empty(incoming):
        if valid_head is None or _is_empty(valid_head):
            raise ValueError(
                "build_scd2_silver: bronze input is empty and valid_head is "
                "None/empty — nothing to process and no schema reference for "
                "the empty expired output."
            )
        return valid_head, expired_head

    incoming = incoming.sort("$m.source_ts", maintain_order=True)

    # PK column names ride on every bronze row in `$m.pk`. Read from
    # the first row. ``.item()`` can't be used here — its
    # implementation aggregates via ``.min()``, which polars rejects
    # for List dtypes — so we go through pandas to extract the cell.
    pk_cols = list(incoming.select(col("$m.pk")).to_pandas()["$m.pk"].iloc[0])

    # T sentinels signal a state reset. Drop ops at-or-before the
    # latest T's source_ts; re-fold from ops after. ALL prior valid
    # rows are closed at the T's source_ts and moved to expired_raw
    # via the bridge logic below (no rows disappear).
    t_rows = incoming.filter(col("$m.op") == OP_TOMBSTONE)
    has_erase = not _is_empty(t_rows)
    if has_erase:
        cutoff = t_rows.select(col("$m.source_ts").max()).to_polars_df().item()
        ops = incoming.filter(
            (col("$m.op") != OP_TOMBSTONE) & (col("$m.source_ts") >= cutoff)
        )
        t_close_ts = cutoff
    else:
        ops = incoming.filter(col("$m.op") != OP_TOMBSTONE)
        t_close_ts = None
    prior_valid: TableFrame | None = valid_head

    # Per-PK shift: for each op, look up the source_ts of the NEXT op
    # in this PK's history. That source_ts becomes the row's $m.valid_to.
    # Tabsdata's sort(by, *more_by) wraps `by` and prepends — pass the
    # columns positionally so each becomes its own sort key rather
    # than a single list-literal expression.
    ops_sorted = ops.sort(*pk_cols, "$m.source_ts")
    enriched = ops_sorted.with_columns([
        pl.col("$m.source_ts").shift(-1).over(pk_cols).alias("_next_ts")
    ])

    # New EXPIRED from intra-batch transitions: every I/U row whose
    # next op exists (U or D) was closed within this batch.
    intra_batch_expired = (
        enriched
        .filter(col("$m.op") != OP_DELETE)
        .filter(col("_next_ts").is_not_null())
        .with_columns([
            col("$m.source_ts").alias("$m.valid_from"),
            col("_next_ts").alias("$m.valid_to"),
        ])
        .drop("_next_ts")
    )

    # New VALID: the last I/U op per PK in the batch (no next op).
    # If the last op for a PK is D, no row lands here — the PK has no
    # valid version anymore. The prior I/U row's _next_ts already
    # captured the close in intra_batch_expired.
    new_valid = (
        enriched
        .filter(col("$m.op") != OP_DELETE)
        .filter(col("_next_ts").is_null())
        .with_columns([
            col("$m.source_ts").alias("$m.valid_from"),
            pl.lit(None, dtype=pl.Datetime(time_zone=timezone.utc)).alias(
                "$m.valid_to"
            ),
        ])
        .drop("_next_ts")
    )

    # Bridge into prior valid state. Two cases:
    #   - T fired: ALL prior valid rows close at the T's source_ts.
    #     No rows carry forward — every row in prior_valid moves to
    #     expired_raw with $m.valid_to = T.source_ts.
    #   - Normal: only PKs whose first batch op exists close their
    #     prior valid row, stamwped with $m.valid_to = that first batch
    #     op's source_ts. PKs untouched in this batch carry through.
    if prior_valid is None or _is_empty(prior_valid):
        logger.debug("No prior valid state; skipping bridge logic")
        prior_carried: TableFrame | None = None
        bridge_expired: TableFrame | None = None
    elif has_erase:
        logger.debug(
            "T sentinel detected; closing ALL prior valid rows at %s", t_close_ts
        )
        bridge_expired = prior_valid.with_columns(lit(t_close_ts).alias("$m.valid_to"))
        prior_carried = None
    else:
        first_op_per_pk = ops_sorted.unique(subset=pk_cols, keep="first").select([
            *pk_cols,
            col("$m.source_ts").alias("_close_ts"),
        ])
        prior_joined = prior_valid.join(first_op_per_pk, on=pk_cols, how="left")
        # Touched → move to expired with the new $m.valid_to.
        bridge_expired = (
            prior_joined
            .filter(col("_close_ts").is_not_null())
            .with_columns(col("_close_ts").alias("$m.valid_to"))
            .drop("_close_ts")
        )
        # Untouched → still valid; carry through.
        prior_carried = prior_joined.filter(col("_close_ts").is_null()).drop(
            "_close_ts"
        )

    # Combine for the two outputs.
    valid_pieces: list[TableFrame] = [new_valid]
    if prior_carried is not None:
        valid_pieces.insert(0, prior_carried)
    new_valid_state = concat(valid_pieces, how="diagonal_relaxed")

    expired_pieces: list[TableFrame] = [intra_batch_expired]
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

    Folds CDC bronze rows (I/U/D/T ops) into a single SCD1-shaped raw
    table: one row per PK, always reflecting the latest observed state.
    I/U → upsert; D → drop; T → reset prior state and re-fold from ops
    after the latest T's source_ts.

    The helper is called by a stage-1 silver transformer that writes
    ``_scd1_raw`` in REPLACE mode — the helper emits the full new
    state each fire.

    The PK column list is read from the bronze rows' ``$m.pk`` column.

    Bronze metadata (``$m.op`` / ``$m.bronze_ts`` / ``$m.pk`` /
    ``$m.source_ts``) rides through unchanged. The stage-2 cleansing
    transformer is responsible for dropping it.

    Args:
        bronze: bronze CDC commits landed since the last silver fire
            (typically read via ``<bronze>@NEW``). Must carry the
            ``$m.pk`` column the bronze helper stamps.
        scd1_head: latest committed ``_scd1_raw`` state. None on the
            first-ever silver fire.

    Returns:
        Full new ``_scd1_raw`` state (REPLACE).
    """
    frames = [frame for frame in (bronze or []) if frame is not None]
    incoming = concat(frames, how="diagonal_relaxed") if frames else None

    # No new bronze rows this fire → return the prior scd1 state
    # unchanged. If there's no prior state either there's nothing to
    # return — surface that as a precondition violation rather than
    # silently emitting an empty frame with an unknown schema.
    if incoming is None or _is_empty(incoming):
        if scd1_head is None or _is_empty(scd1_head):
            raise ValueError(
                "build_scd1_silver: bronze input is empty and scd1_head is "
                "None/empty — nothing to process."
            )
        return scd1_head

    incoming = incoming.sort("$m.source_ts", maintain_order=True)
    pk_cols = list(incoming.select(col("$m.pk")).to_pandas()["$m.pk"].iloc[0])

    # T sentinels reset prior state.
    t_rows = incoming.filter(col("$m.op") == OP_TOMBSTONE)
    has_erase = not _is_empty(t_rows)
    if has_erase:
        cutoff = t_rows.select(col("$m.source_ts").max()).to_polars_df().item()
        ops = incoming.filter(
            (col("$m.op") != OP_TOMBSTONE) & (col("$m.source_ts") >= cutoff)
        )
        prior: TableFrame | None = None
    else:
        ops = incoming.filter(col("$m.op") != OP_TOMBSTONE)
        prior = scd1_head

    # SCD1: only the LAST op per PK in this batch matters. I→U→D
    # collapses to "deleted"; I→D→I collapses to "inserted".
    latest = ops.unique(subset=pk_cols, keep="last")

    upserts = latest.filter(col("$m.op") != OP_DELETE)
    affected_pks = latest.select(pk_cols)

    if prior is None or _is_empty(prior):
        return upserts

    survivors = prior.join(affected_pks, on=pk_cols, how="anti")
    return concat([survivors, upserts], how="diagonal_relaxed")


def _strip_bronze_metadata(tf: TableFrame) -> TableFrame:
    """
    Drop bronze-only metadata that silver shouldn't carry forward:
    ``$m.op``, ``$m.bronze_ts``, and any ``@td..cdc.meta.*`` columns
    inherited from the upstream CDC trigger (e.g. ``@td..cdc.meta.txid``,
    ``@td..cdc.meta.lsn``). The CDC bronze helper renames the two
    meaningful ones (``.op`` / ``.ts``) into the ``$m.*`` namespace
    but passes the rest through verbatim — silver strips them here.

    Args:
        tf: TableFrame with bronze metadata columns to drop.
    Returns:
        TableFrame with the bronze metadata columns dropped.
    """
    cdc_meta_cols = [c for c in tf.columns if c.startswith("@td..cdc")]
    # Periodic-snapshot bronze carries no $m.op, so drop only what's present.
    drop_cols = [
        c for c in ("$m.op", "$m.bronze_ts", *cdc_meta_cols) if c in tf.columns
    ]
    return tf.drop(drop_cols)


def build_scd2_silver_cleanse(
    valid_raw: list[TableFrame],
    expired_raw: list[TableFrame],
) -> tuple[TableFrame, TableFrame]:
    """
    SCD2 silver cleansing — pure projection: drops bronze metadata
    from both ``_scd2_valid_raw`` and ``_scd2_expired_raw`` inputs.

    Strips ``$m.op``, ``$m.bronze_ts``, and any leftover
    ``@td..cdc.*`` columns the upstream CDC trigger carried into
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

    Strips ``$m.op``, ``$m.bronze_ts``, and any leftover
    ``@td..cdc.meta.*`` columns the upstream CDC trigger carried into
    bronze. Per-source cleansing (casts, renames, projections) is the
    caller's responsibility on top of the helper's output.

    Args:
        scd1_raw: ``_scd1_raw@NEW`` commits.

    Returns:
        Cleansed SCD1 silver TableFrame.
    """
    return _strip_bronze_metadata(concat(scd1_raw, how="diagonal_relaxed"))
