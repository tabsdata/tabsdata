#
# Copyright 2026 Tabsdata Inc.
#

import polars as pl

from tabsdatak._medallion.constants import (
    MEDALLION_SYSTEM_COLUMN_ENTITY_SK,
    MEDALLION_SYSTEM_COLUMN_PK,
    MEDALLION_SYSTEM_COLUMN_VALID_FROM,
    MEDALLION_SYSTEM_COLUMN_VALID_TO,
    MEDALLION_SYSTEM_COLUMN_VERSION_SK,
)
from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe.functions import col, concat


def _is_empty(tf: TableFrame) -> bool:
    """Return True when ``tf`` has no rows (gen-2 has no ``is_empty``)."""
    return tf.first()._to_polars_df().is_empty()


def _natural_key(tf: TableFrame) -> list[str]:
    """Natural-key column names, read off the silver rows' ``@td.medallion.pk``.

    ``@td.medallion.pk`` is a List[String] stamped by the bronze helper and preserved
    through silver. All rows in a fire share the same value, so the first row
    is sufficient. ``.item()`` can't be used on a List dtype (it aggregates
    via ``.min()``), so extract through pandas.
    """
    return list(
        tf
        .select(col(MEDALLION_SYSTEM_COLUMN_PK))
        ._to_pandas()[MEDALLION_SYSTEM_COLUMN_PK]
        .iloc[0]
    )


def _surrogate_key(cols: list[str]) -> pl.Expr:
    """Deterministic surrogate-key expression: hash of a struct of ``cols``.

    Hashing a struct (rather than a concatenated string) means there is no
    separator that distinct key tuples could collide on, and non-string
    dtypes (e.g. a datetime ``@td.medallion.valid_from``) are handled without casts.
    """
    return pl.struct(cols).hash()


def _drop_pk(tf: TableFrame) -> TableFrame:
    """Drop the ``@td.medallion.pk`` column from a gold row.

    Only ``@td.medallion.pk`` is dropped — the surrogate keys
    (``@td.medallion.entity_sk`` /
    ``@td.medallion.version_sk``), source time, and SCD2 effective-dating
    (``@td.medallion.valid_from`` / ``@td.medallion.valid_to``) stay on the gold row.
    """
    return (
        tf.drop([MEDALLION_SYSTEM_COLUMN_PK])
        if MEDALLION_SYSTEM_COLUMN_PK in tf.columns
        else tf
    )


def build_type1_dim(silver_head: TableFrame | None) -> TableFrame | None:
    """
    Type 1 gold dimension — current-snapshot projection of a silver table.

    Used for both Entity / Hierarchy (``_valid_silver@HEAD``) and Reference
    data (``_current_silver@HEAD``). Adds ``entity_sk = hash(<natural_key>)``
    and drops the ``@td.medallion.*`` bookkeeping. No history — a Type 1 dimension holds
    exactly one row per natural key, reflecting the latest state. The caller
    points this at the right silver table via its ``@HEAD`` input.

    Args:
        silver_head: the current-state silver table (``_valid_silver`` for
            Entity / Hierarchy, ``_current_silver`` for Reference). Carries
            ``@td.medallion.pk`` so the natural key can be derived. None when the
            upstream
            silver table has never committed (first fire) — returns None.

    Returns:
        ``dim_<entity>`` with ``@td.medallion.entity_sk`` added and ``@td.medallion.pk``
        dropped, or
        None when ``silver_head`` is None.
    """
    if silver_head is None:
        return None
    out = _with_surrogate_key(silver_head, [], MEDALLION_SYSTEM_COLUMN_ENTITY_SK)
    return _drop_pk(out)


def _to_history_rows(silver: TableFrame) -> TableFrame:
    """Shape a silver SCD2 frame (valid or expired) into the gold history
    schema: add ``@td.medallion.version_sk`` and drop ``@td.medallion.pk``. The
    ``@td.medallion.valid_from`` /
    ``@td.medallion.valid_to`` effective-dating columns ride through unchanged."""
    shaped = _with_surrogate_key(
        silver, [MEDALLION_SYSTEM_COLUMN_VALID_FROM], MEDALLION_SYSTEM_COLUMN_VERSION_SK
    )
    return _drop_pk(shaped)


def _with_surrogate_key(
    tf: TableFrame, extra_cols: list[str], alias: str
) -> TableFrame:
    """Add ``alias`` as the surrogate key of the natural key plus
    ``extra_cols``. An empty frame (a reset emptied the table) has no row to
    read ``@td.medallion.pk`` from, so it gets a typed null column instead."""
    if _is_empty(tf):
        return tf.with_columns(pl.lit(None, dtype=pl.UInt64).alias(alias))
    return tf.with_columns(
        _surrogate_key([*_natural_key(tf), *extra_cols]).alias(alias)
    )


def build_type2_dim(
    history_head: TableFrame | None,
    expired_new: list[TableFrame],
    valid_head: TableFrame,
) -> TableFrame:
    """
    Type 2 gold history dimension — full version history for Entity /
    Hierarchy data, rebuilt each fire from three DISJOINT branches:

      1. prior-expired — already-closed versions from
         ``dim_<entity>_history@HEAD`` (``valid_to IS NOT NULL``). Passthrough;
         they already carry ``version_sk`` + ``valid_from`` / ``valid_to``.
      2. newly-expired — versions closed THIS cycle, from
         ``_expired_silver@NEW``. Get ``version_sk`` added.
      3. current-valid — the open version per entity, from
         ``_valid_silver@HEAD``. Replaces the prior open rows (which is why
         branch 1 keeps ONLY closed rows). ``valid_to`` stays null.

    ``version_sk = hash(<natural_key>, valid_from)`` is stable across the
    current-valid → newly-expired transition (``valid_from`` doesn't change
    when a version closes), so a row keeps the same surrogate key as it moves
    from branch 3 to branch 1 over successive runs — no duplicates, no churn.

    The output is a full replace of ``dim_<entity>_history``.

    Args:
        history_head: prior history dimension (``dim_<entity>_history@HEAD``).
            None on the first-ever fire.
        expired_new: newly-closed versions (``_expired_silver@NEW`` commits).
        valid_head: current open versions (``_valid_silver@HEAD``).

    Returns:
        The rebuilt ``dim_<entity>_history``.
    """
    branches: list[TableFrame] = []

    # 1. prior-expired: keep only already-closed rows from the history table.
    #    The old open rows are superseded by branches 2 + 3 below.
    if history_head is not None and not _is_empty(history_head):
        branches.append(
            history_head.filter(col(MEDALLION_SYSTEM_COLUMN_VALID_TO).is_not_null())
        )

    # 2. newly-expired: versions that closed this cycle.
    frames = [frame for frame in (expired_new or []) if frame is not None]
    expired = concat(frames, how="diagonal_relaxed") if frames else None
    if expired is not None and not _is_empty(expired):
        branches.append(_to_history_rows(expired))

    # 3. current-valid: the open version per entity (valid_to = null).
    branches.append(_to_history_rows(valid_head))

    return concat(branches, how="diagonal_relaxed")
