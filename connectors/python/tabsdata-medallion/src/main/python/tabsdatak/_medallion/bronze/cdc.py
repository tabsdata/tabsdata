#
# Copyright 2026 Tabsdata Inc.
#

from datetime import datetime, timezone

import tabsdatak.tableframe.datatypes as td_dtypes
from tabsdatak._medallion.constants import (
    MEDALLION_SYSTEM_COLUMN_BRONZE_TS,
    MEDALLION_SYSTEM_COLUMN_OP,
    MEDALLION_SYSTEM_COLUMN_PK,
    MEDALLION_SYSTEM_COLUMN_SOURCE_TS,
)
from tabsdatak.conn.common.sql.cdc.constants import (
    CDC_DATA_COL_NEW_PREFIX,
    CDC_DATA_COL_OLD_PREFIX,
    CDC_META_OP,
    CDC_META_TS,
    DOT,
)
from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe.functions import col, concat, lit
from tabsdatak.tableframe.schema import Schema

# ── @td.medallion.op values
# ────────────────────────────────────────────────────────────
# Canonical CDC op codes stamped into the bronze `@td.medallion.op` column. Lowercase
# so downstream comparisons stay case-consistent across producers/consumers.
OP_INSERT = "i"
OP_UPDATE = "u"
OP_DELETE = "d"
# Table emptied at this position: a synthetic bootstrap reset, or a source TRUNCATE
# in a CDC stream.
OP_TOMBSTONE = "t"
# CDC initial load — wipes prior state; the inserts that follow are the full state.
OP_LOAD = "l"
# Ops that empty the table at their position: every version open there closes.
RESET_OPS = (OP_TOMBSTONE, OP_LOAD)


def build_cdc_bronze(
    full_commits: list[TableFrame],
    cdc_commits: list[TableFrame],
    pk: list[dict],
) -> TableFrame:
    """
    Combine CDC commits (and, in legacy pipelines, a bootstrap snapshot) into a
    single bronze stream.

    New pipelines pass ``full_commits=[]`` and take the initial load from the
    CDC stream itself: set ``start_from="full"`` on the ``*CdcStreamSrc``, and
    the stream emits an ``l`` (load) row followed by the snapshot rows as
    inserts, then continues from the snapshot's exact log position with no gap
    and no duplicates. To resync, reset the stream publisher's stage
    (``tdk fn update --reset-stage``) with ``start_from="full"``. CDC rows pass
    through with their op, including ``l`` and ``t`` (truncate); silver treats
    both as resets.

    Args:
        full_commits: Legacy, kept only for existing deployments. Full snapshots
            read by a separate batch publisher, used to bootstrap or resync
            bronze: the latest one is emitted as a ``t`` reset row plus inserts.
            Such a snapshot has no log position, so CDC events landing in the
            same run that are not newer than the bootstrap time are dropped.
            Pass ``[]`` in new pipelines.
        cdc_commits: TableFrames representing CDC commits: the stream's rows
            (``i``/``u``/``d``, and ``l``/``t`` control rows) in log order.
        pk: ordered list of single-key dicts mapping each primary-key column name to its
            tabsdata dtype. Example:
                [{"tenant_id": td.String}, {"entity_id": td.Int64}]
    Returns:
        A single TableFrame representing the bronze stream, with CDC metadata columns
    """
    pk_schema: dict[str, type] = {
        name: dtype for entry in pk for name, dtype in entry.items()
    }
    pk_cols = list(pk_schema.keys())

    # @NEW yields a list of commit-tables. Concat the cdc side (an empty
    # list has no frame to concat — treat it as the no-cdc fallback).
    cdc = concat(cdc_commits, how="diagonal_relaxed") if cdc_commits else None
    if cdc is None:
        cdc = TableFrame.empty(
            schema=Schema({
                **pk_schema,
                MEDALLION_SYSTEM_COLUMN_OP: td_dtypes.String,
                MEDALLION_SYSTEM_COLUMN_SOURCE_TS: td_dtypes.Datetime(
                    time_zone=timezone.utc
                ),
                MEDALLION_SYSTEM_COLUMN_BRONZE_TS: td_dtypes.Datetime,
                MEDALLION_SYSTEM_COLUMN_PK: td_dtypes.List(td_dtypes.String),
            })
        )
    else:
        # Rename the upstream CDC metadata columns into local @td.medallion.* namespace.
        rename_map = {
            CDC_META_OP: MEDALLION_SYSTEM_COLUMN_OP,
            CDC_META_TS: MEDALLION_SYSTEM_COLUMN_SOURCE_TS,
        }
        # Payload columns arrive prefixed as `@td.cdc.data.col.new.<name>`.
        # Strip the prefix so downstream sees plain `<name>` columns.
        # Pre-image columns (`@td.cdc.data.col.old.<name>`) are dropped —
        # bronze is post-image only.
        new_col_prefix = CDC_DATA_COL_NEW_PREFIX + DOT
        old_col_prefix = CDC_DATA_COL_OLD_PREFIX + DOT
        drop_cols: list[str] = []
        for c in cdc.columns:
            if c.startswith(new_col_prefix):
                rename_map[c] = c[len(new_col_prefix) :]
            elif c.startswith(old_col_prefix):
                drop_cols.append(c)
        cdc = cdc.rename(rename_map)
        if drop_cols:
            cdc = cdc.drop(drop_cols)

    # Keep every column from the cdc input (including any @td.cdc.data.* payload
    # columns); stamp a bronze-side ingest timestamp and the composite-key column list.
    stream = cdc.with_columns([
        lit(datetime.now(tz=timezone.utc)).alias(MEDALLION_SYSTEM_COLUMN_BRONZE_TS),
        lit(pk_cols).alias(MEDALLION_SYSTEM_COLUMN_PK),
    ])

    if not full_commits:
        # Pure incremental — no bootstrap landed.
        return stream

    # Bootstrap landed (initial onboarding, operational resync, or refresh).
    # Take ONLY the latest commit; older bootstraps are superseded.
    full = full_commits[-1]
    bootstrap_ts = datetime.now(tz=timezone.utc)

    # Erase sentinel: first row out, all key columns null with the correct dtype.
    erase = TableFrame.from_dict({
        **{col: [None] for col in pk_cols},
        MEDALLION_SYSTEM_COLUMN_OP: [OP_TOMBSTONE],
        MEDALLION_SYSTEM_COLUMN_SOURCE_TS: [bootstrap_ts],
        MEDALLION_SYSTEM_COLUMN_BRONZE_TS: [bootstrap_ts],
        MEDALLION_SYSTEM_COLUMN_PK: [pk_cols],
    }).with_columns([col(name).cast(dtype) for name, dtype in pk_schema.items()])

    # Bootstrap rows → synthetic I events stamped at bootstrap_ts.
    # Keep every column from the snapshot; just append the metadata cols.
    seed = full.with_columns([
        lit(OP_INSERT).alias(MEDALLION_SYSTEM_COLUMN_OP),
        lit(bootstrap_ts).alias(MEDALLION_SYSTEM_COLUMN_SOURCE_TS),
        lit(bootstrap_ts).alias(MEDALLION_SYSTEM_COLUMN_BRONZE_TS),
        lit(pk_cols).alias(MEDALLION_SYSTEM_COLUMN_PK),
    ])

    # Filter CDC: drop anything same-or-older than the bootstrap.
    seed_max_ts = (
        seed.select(col(MEDALLION_SYSTEM_COLUMN_SOURCE_TS).max())._to_polars_df().item()
    )
    filtered_stream = stream.filter(
        col(MEDALLION_SYSTEM_COLUMN_SOURCE_TS) > seed_max_ts
    )

    return concat([erase, seed, filtered_stream], how="diagonal_relaxed")
