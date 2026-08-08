#
# Copyright 2026 Tabsdata Inc.
#

import tabsdatak.tableframe.datatypes as td_dtypes
from tabsdatak._medallion.constants import (
    MEDALLION_SYSTEM_COLUMN_DATE_SK,
    MEDALLION_SYSTEM_COLUMN_PK,
    MEDALLION_SYSTEM_COLUMN_SOURCE_TS,
    MEDALLION_SYSTEM_COLUMN_VALID_FROM,
    MEDALLION_SYSTEM_COLUMN_VALID_TO,
    MEDALLION_SYSTEM_COLUMN_VERSION_SK,
)
from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe.functions import col, concat


def _pin_version_sk(
    events: TableFrame,
    history: TableFrame,
    key_cols: list[str],
    sk_alias: str,
) -> TableFrame:
    """Pin the dim version active at each event's ``@td.medallion.source_ts``.

    For each event, picks the ``dim_<entity>_history`` version whose
    ``[valid_from, valid_to)`` interval covers the event timestamp, matched on
    the foreign key ``key_cols`` (the FK column name must equal the dim's
    natural-key column name). The chosen version's ``@td.medallion.version_sk`` is
    surfaced as ``sk_alias``.

    tabsdata's ``join`` has no asof / non-equi strategy, so this is an
    equi-join on the key (fanning each event out to every version of its
    entity) + a range filter that keeps the single covering version. For
    contiguous, non-overlapping SCD2 versions exactly one matches. The inner
    join drops events whose FK is unknown to the dim, or whose timestamp
    precedes the entity's first version (orphan-FK / late-dimension events).

    Args:
        events: the in-progress fact frame (carries ``@td.medallion.source_ts``).
        history: a ``dim_<entity>_history`` table.
        key_cols: the FK / natural-key column(s) to match on.
        sk_alias: output column name for the pinned ``@td.medallion.version_sk``.

    Returns:
        ``events`` with ``sk_alias`` added; the dim's ``@td.medallion.valid_from`` /
        ``@td.medallion.valid_to`` are consumed (dropped) so the next pin doesn't
        collide.
    """
    slim = history.select([
        *key_cols,
        MEDALLION_SYSTEM_COLUMN_VALID_FROM,
        MEDALLION_SYSTEM_COLUMN_VALID_TO,
        MEDALLION_SYSTEM_COLUMN_VERSION_SK,
    ])
    pinned = events.join(slim, on=key_cols, how="inner").filter(
        (
            col(MEDALLION_SYSTEM_COLUMN_VALID_FROM)
            <= col(MEDALLION_SYSTEM_COLUMN_SOURCE_TS)
        )
        & (
            col(MEDALLION_SYSTEM_COLUMN_VALID_TO).is_null()
            | (
                col(MEDALLION_SYSTEM_COLUMN_SOURCE_TS)
                < col(MEDALLION_SYSTEM_COLUMN_VALID_TO)
            )
        )
    )
    return pinned.rename({MEDALLION_SYSTEM_COLUMN_VERSION_SK: sk_alias}).drop([
        MEDALLION_SYSTEM_COLUMN_VALID_FROM,
        MEDALLION_SYSTEM_COLUMN_VALID_TO,
    ])


def build_fact(
    events: list[TableFrame],
    dim_versions: list[tuple[TableFrame, list[str], str]],
) -> TableFrame | None:
    """
    Gold fact builder — shared by both append-only event facts and
    periodic-snapshot facts.

    Range-joins each referenced ``dim_<entity>_history`` on ``@td.medallion.source_ts``
    to
    pin the ``version_sk`` that was active at that moment, derives
    ``@td.medallion.date_sk``
    inline (``YYYYMMDD`` from ``@td.medallion.source_ts``), and drops the
    ``@td.medallion.pk``
    bookkeeping. Measures and any other source-borne columns ride through
    unchanged.

    The pin/date column is always ``@td.medallion.source_ts``, which carries the right
    meaning for either source:

      - **Append-only event silver** (``<event>_event_silver``):
      ``@td.medallion.source_ts``
        is the event time, so the range-join pins the dim version active when
        the event happened → grain is the event, output ``fct_<event>``.
      - **Periodic-snapshot silver** (``<entity>_psf_periodic_silver``): periodic
        bronze mirrors ``@td.medallion.period`` into ``@td.medallion.source_ts``, so the
        SAME join is
        the period_end range-join (and ``@td.medallion.date_sk`` becomes the period
        date)
        → grain is (entity, period), output ``fct_<entity>_<period>``.

    Either way the grain is enforced upstream by the silver table; this helper
    only attaches keys, so it needs no special casing for the two flavors.

    Args:
        events: append-only silver commits read at ``@NEW`` — event-silver
            (``<event>_event_silver``) or periodic-snapshot silver
            (``<entity>_psf_periodic_silver``).
        dim_versions: one entry per referenced SCD2 dimension, as
            ``(dim_<entity>_history, key_cols, sk_alias)`` — the history table
            (read at ``@HEAD``), the FK/natural-key column(s) to match on, and
            the output surrogate-key column name (e.g. ``customer_version_sk``).

    Returns:
        The fact rows (``fct_<event>`` or ``fct_<entity>_<period>``), or None
        when there are no new rows.
    """
    frames = [frame for frame in (events or []) if frame is not None]
    fct = concat(frames, how="diagonal_relaxed") if frames else None
    if fct is None:
        return None

    # Pin each FK to the dim version active at @td.medallion.source_ts (event time, or
    # the
    # period for periodic snapshots). Sequential: each pin consumes that dim's
    # valid_from/valid_to, so the next dim's interval columns don't collide.
    for history, key_cols, sk_alias in dim_versions:
        fct = _pin_version_sk(fct, history, key_cols, sk_alias)

    # date_sk = YYYYMMDD, derived inline from @td.medallion.source_ts. No dim_date join
    # —
    # the key is derivable; dim_date is only needed at query time for calendar
    # attributes. Cast the components to Int32 first: dt.month() / dt.day() are
    # Int8, so `month * 100` would overflow (200 wraps to -56).
    fct = fct.with_columns(
        (
            col(MEDALLION_SYSTEM_COLUMN_SOURCE_TS).dt.year().cast(td_dtypes.Int32)
            * 10000
            + col(MEDALLION_SYSTEM_COLUMN_SOURCE_TS).dt.month().cast(td_dtypes.Int32)
            * 100
            + col(MEDALLION_SYSTEM_COLUMN_SOURCE_TS).dt.day().cast(td_dtypes.Int32)
        ).alias(MEDALLION_SYSTEM_COLUMN_DATE_SK)
    )

    return fct.drop([MEDALLION_SYSTEM_COLUMN_PK])
