#
# Copyright 2026 Tabsdata Inc.
#

from datetime import datetime, timezone

import tabsdatak.tableframe.datatypes as td_dtypes
from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe.functions import col, concat, lit
from tabsdatak.tableframe.schema import Schema

# ── $m.op values ────────────────────────────────────────────────────────────
# Canonical CDC op codes stamped into the bronze `$m.op` column. Lowercase
# so downstream comparisons stay case-consistent across producers/consumers.
OP_INSERT = "i"
OP_UPDATE = "u"
OP_DELETE = "d"
OP_TOMBSTONE = "t"  # erase sentinel — wipes prior state


def build_cdc_bronze(
    full_commits: list[TableFrame],
    cdc_commits: list[TableFrame],
    pk: list[dict],
) -> TableFrame:
    """
    Combine CDC commits (and an optional bootstrap snapshot) into a single bronze
    stream.

    Args:
        full_commits: Tableframes representing full commits (bootstraps). These are
            complete snapshots of the source data at a point in time. They are used for
            initial onboarding or full resyncs.
        cdc_commits: TableFrames representing CDC commits. These are incremental change
            logs that capture inserts, updates, and deletes after the initial bootstrap.
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
                "$m.op": td_dtypes.String,
                "$m.source_ts": td_dtypes.Datetime(time_zone=timezone.utc),
                "$m.bronze_ts": td_dtypes.Datetime,
                "$m.pk": td_dtypes.List(td_dtypes.String),
            })
        )
    else:
        # Rename the upstream CDC metadata columns into local $m.* namespace.
        rename_map = {
            "@td..cdc.meta.op": "$m.op",
            "@td..cdc.meta.ts": "$m.source_ts",
        }
        # Payload columns arrive prefixed as `@td..cdc.data.col.new.<name>`.
        # Strip the prefix so downstream sees plain `<name>` columns.
        # Pre-image columns (`@td..cdc.data.col.old.<name>`) are dropped —
        # bronze is post-image only.
        new_col_prefix = "@td..cdc.data.col.new."
        old_col_prefix = "@td..cdc.data.col.old."
        drop_cols: list[str] = []
        for c in cdc.columns:
            if c.startswith(new_col_prefix):
                rename_map[c] = c[len(new_col_prefix) :]
            elif c.startswith(old_col_prefix):
                drop_cols.append(c)
        cdc = cdc.rename(rename_map)
        if drop_cols:
            cdc = cdc.drop(drop_cols)

    # Keep every column from the cdc input (including any @td..cdc.data.* payload
    # columns); stamp a bronze-side ingest timestamp and the composite-key column list.
    stream = cdc.with_columns([
        lit(datetime.now(tz=timezone.utc)).alias("$m.bronze_ts"),
        lit(pk_cols).alias("$m.pk"),
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
        "$m.op": [OP_TOMBSTONE],
        "$m.source_ts": [bootstrap_ts],
        "$m.bronze_ts": [bootstrap_ts],
        "$m.pk": [pk_cols],
    }).with_columns([col(name).cast(dtype) for name, dtype in pk_schema.items()])

    # Bootstrap rows → synthetic I events stamped at bootstrap_ts.
    # Keep every column from the snapshot; just append the metadata cols.
    seed = full.with_columns([
        lit(OP_INSERT).alias("$m.op"),
        lit(bootstrap_ts).alias("$m.source_ts"),
        lit(bootstrap_ts).alias("$m.bronze_ts"),
        lit(pk_cols).alias("$m.pk"),
    ])

    # Filter CDC: drop anything same-or-older than the bootstrap.
    seed_max_ts = seed.select(col("$m.source_ts").max()).to_polars_df().item()
    filtered_stream = stream.filter(col("$m.source_ts") > seed_max_ts)

    return concat([erase, seed, filtered_stream], how="diagonal_relaxed")
