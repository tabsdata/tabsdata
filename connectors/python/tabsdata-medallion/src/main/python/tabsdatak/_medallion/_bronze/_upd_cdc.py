#
# Copyright 2026 Tabsdata Inc.
#

import logging
from datetime import datetime, timezone

import polars as pl

import tabsdatak.tableframe.datatypes as td_dtypes
from tabsdatak._medallion._bronze._cdc import (
    OP_DELETE,
    OP_INSERT,
    OP_TOMBSTONE,
    OP_UPDATE,
)
from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe.functions import col, concat, lit

logger = logging.getLogger(__name__)


def _to_utc_datetime(tf: TableFrame, col_name: str) -> TableFrame:
    """Validate ``tf[col_name]`` is a Datetime and normalize it to UTC.

    Mirrors the time-zone branch in ``build_append_only_cdc_bronze``:
      - None (naive)        → ``dt.replace_time_zone("UTC")`` attaches
                              UTC, wall-clock preserved.
      - "UTC"                → no-op.
      - Any other tz-aware   → ``dt.convert_time_zone("UTC")`` preserves
                              the instant, wall-clock changes to UTC.
    """
    ts_dtype = tf.schema[col_name]
    if not isinstance(ts_dtype, pl.Datetime):
        raise TypeError(
            f"{col_name!r} must reference a Datetime column; got dtype {ts_dtype!r}"
        )
    ts_tz = ts_dtype.time_zone
    if ts_tz is None:
        return tf.with_columns(col(col_name).dt.replace_time_zone("UTC"))
    if str(ts_tz) == "UTC":
        return tf
    return tf.with_columns(col(col_name).dt.convert_time_zone("UTC"))


def build_upd_cdc_bronze(
    full_commits: list[TableFrame],
    upd_commits: list[TableFrame],
    prior_bronze: TableFrame | None,
    pk: list[dict],
    updated_ts_col: str,
    is_delete_col: str,
) -> TableFrame:
    """
    Generate synthetic I/U/D CDC bronze events for the bootstrap +
    update-stream pattern. The transformer fires whenever EITHER the
    full-snapshot publisher OR the incremental-update publisher commits,
    so on any single run exactly one of ``full_commits`` /
    ``upd_commits`` is non-empty.

    Args:
        full_commits: Full snapshots of the source. Non-empty when the
            full-snapshot publisher just fired (initial onboarding or an
            operational resync). Only the latest snapshot is used.
        upd_commits: Incremental update batches. Non-empty when the
            update-stream publisher just fired. Each row carries
            ``updated_ts_col`` and ``is_delete_col``.
        prior_bronze: The bronze state from the prior transformer fire.
            None on the first-ever fire (no bootstrap yet).
        pk: Ordered list of single-key dicts mapping each primary-key
            column name to its tabsdata dtype. Example:
                [{"tenant_id": td.String}, {"entity_id": td.Int64}]
        updated_ts_col: Name of the source-side timestamp column that
            marks when the row was last mutated. Mirrored into
            ``$m.source_ts`` on every emitted bronze row.
        is_delete_col: Name of the source-side boolean soft-delete
            column. True → emit ``D``; False → emit ``I`` or ``U``
            depending on whether bronze has seen the PK before.

    Returns:
        A TableFrame of synthetic CDC events stamped with the bronze
        metadata columns ``$m.op`` / ``$m.source_ts`` / ``$m.bronze_ts``
        / ``$m.pk``.
    """
    pk_schema: dict[str, type] = {
        name: dtype for entry in pk for name, dtype in entry.items()
    }
    pk_cols = list(pk_schema.keys())
    run_ts = datetime.now(tz=timezone.utc)

    if full_commits:
        # Backfill: wipe prior bronze with an erase sentinel and re-seed
        # from the latest full commit. Drop any soft-deleted source rows
        # outright (bronze has no prior state for them to delete) and
        # emit the survivors as I events stamped at run_ts.
        full = _to_utc_datetime(full_commits[-1], updated_ts_col)
        survivors = full.filter(~col(is_delete_col).cast(td_dtypes.Boolean))

        erase = TableFrame.from_dict({
            **{c: [None] for c in pk_cols},
            "$m.op": [OP_TOMBSTONE],
            "$m.source_ts": [run_ts],
            "$m.bronze_ts": [run_ts],
            "$m.pk": [pk_cols],
        }).with_columns([col(name).cast(dtype) for name, dtype in pk_schema.items()])

        seed = survivors.with_columns([
            lit(OP_INSERT).alias("$m.op"),
            lit(run_ts).alias("$m.source_ts"),
            lit(run_ts).alias("$m.bronze_ts"),
            lit(pk_cols).alias("$m.pk"),
        ])

        if not upd_commits:
            return concat([erase, seed], how="diagonal_relaxed")

        # Both publishers landed in the same fire. Fold in upd events
        # that happened AFTER the snapshot was taken, using the snapshot's
        # latest source-side mutation time as the cutoff. Classify each
        # such event in priority order: is_delete_col=True → D
        # (regardless of whether the PK was in the seed); unseen PK → I;
        # seen PK → U.
        upd = _to_utc_datetime(
            concat(upd_commits, how="diagonal_relaxed"), updated_ts_col
        )
        seed_max_ts = full.select(col(updated_ts_col).max()).to_polars_df().item()
        upd_after = upd.filter(col(updated_ts_col) > seed_max_ts)

        seen = (
            survivors
            .select(pk_cols)
            .unique(subset=pk_cols)
            .with_columns(lit(True).alias("_seen"))
        )

        classified_upd = (
            upd_after
            .join(seen, on=pk_cols, how="left")
            .with_columns([
                pl
                .when(pl.col(is_delete_col).cast(pl.Boolean))
                .then(pl.lit(OP_DELETE))
                .when(pl.col("_seen").is_null())
                .then(pl.lit(OP_INSERT))
                .otherwise(pl.lit(OP_UPDATE))
                .alias("$m.op"),
                col(updated_ts_col).alias("$m.source_ts"),
                lit(run_ts).alias("$m.bronze_ts"),
                lit(pk_cols).alias("$m.pk"),
            ])
            .drop("_seen")
        )

        return concat([erase, seed, classified_upd], how="diagonal_relaxed")

    # Drop absent commits (``None`` entries) and short-circuit when the
    # update stream had nothing this fire — concat has no frame to build.
    upd_frames = [frame for frame in (upd_commits or []) if frame is not None]
    upd_concat = concat(upd_frames, how="diagonal_relaxed") if upd_frames else None

    if upd_concat is None:
        logger.info("No update data available; returning None")
        return None
    # Incremental: classify each upd row against the prior bronze state.
    upd = _to_utc_datetime(upd_concat, updated_ts_col)

    if prior_bronze is None:
        # First-ever incremental fire — no bootstrap landed yet. Drop
        # soft-deleted rows outright (bronze has no prior state for them
        # to delete) and emit the survivors as I events stamped at run_ts.
        return upd.filter(~col(is_delete_col).cast(td_dtypes.Boolean)).with_columns([
            lit(OP_INSERT).alias("$m.op"),
            lit(run_ts).alias("$m.source_ts"),
            lit(run_ts).alias("$m.bronze_ts"),
            lit(pk_cols).alias("$m.pk"),
        ])

    # Classify each upd row in priority order: is_delete_col=True → D
    # (regardless of whether the PK was previously seen); unseen PK → I;
    # seen PK → U. ``seen`` skips T sentinels (those carry null PKs).
    # ``unique`` must scope on pk_cols only — without a subset, tabsdata's
    # $td.* tracking columns (silently re-added by ``select``) make every
    # row distinct and the dedup is a no-op, which row-multiplies the
    # left-join below.
    seen = (
        prior_bronze
        .filter(col("$m.op") != OP_TOMBSTONE)
        .select(pk_cols)
        .unique(subset=pk_cols)
        .with_columns(lit(True).alias("_seen"))
    )

    return (
        upd
        .join(seen, on=pk_cols, how="left")
        .with_columns([
            # pl.when/then chains need polars-native expressions —
            # passing tabsdata's td_expr.Expr wrapper trips polars'
            # internal literal coercion. Use pl.col / pl.lit only
            # inside the when chain.
            pl
            .when(pl.col(is_delete_col).cast(pl.Boolean))
            .then(pl.lit(OP_DELETE))
            .when(pl.col("_seen").is_null())
            .then(pl.lit(OP_INSERT))
            .otherwise(pl.lit(OP_UPDATE))
            .alias("$m.op"),
            col(updated_ts_col).alias("$m.source_ts"),
            lit(run_ts).alias("$m.bronze_ts"),
            lit(pk_cols).alias("$m.pk"),
        ])
        .drop("_seen")
    )
