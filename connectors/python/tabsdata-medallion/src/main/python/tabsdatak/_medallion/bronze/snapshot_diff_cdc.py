#
# Copyright 2026 Tabsdata Inc.
#

import logging
from datetime import datetime, timezone
from functools import reduce
from operator import or_

from tabsdatak._medallion.bronze.cdc import (
    OP_DELETE,
    OP_INSERT,
    OP_TOMBSTONE,
    OP_UPDATE,
)
from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe.functions import col, concat, lit

logger = logging.getLogger(__name__)


def build_snapshot_diff_cdc_bronze(
    prev: TableFrame | None,
    head: TableFrame,
    new: list[TableFrame],
    pk: list[dict],
) -> TableFrame:
    """
    Generate synthetic I/U/D CDC bronze layer by diffing snapshot states.

    Args:
        prev: prior committed snapshot. None on initial onboarding.
        head: current snapshot to diff against.
        new: table versions created since the last execution of the transformer. When
            empty, treat the commit as a refresh and re-bootstrap from `head`.
        pk: ordered list of single-key dicts mapping each primary-key column name to its
            tabsdata dtype. Example:
                [{"tenant_id": td.String}, {"entity_id": td.Int64}]

    Returns:
        A TableFrame with synthetic CDC events representing the diff from prev → head.
    """
    if head is None:
        raise ValueError("Current snapshot (head) cannot be None")
    pk_schema: dict[str, type] = {
        name: dtype for entry in pk for name, dtype in entry.items()
    }
    pk_cols = list(pk_schema.keys())

    run_ts = datetime.now(tz=timezone.utc)

    # Derive value columns from the latest snapshot (skip PK + tabsdata system cols).
    value_cols = [
        c for c in head.columns if c not in pk_cols and not c.startswith("$td.")
    ]
    last_schema = head.schema
    all_dtypes = {**pk_schema, **{c: last_schema[c] for c in value_cols}}

    if prev is None or prev.first().to_polars_df().is_empty() or len(new) == 0:
        logger.debug(
            "No prior snapshot or no new data; treating as initial onboarding or "
            "refresh"
        )
        # Initial onboarding (no prev) OR refresh trigger (no new arrivals).
        erase = TableFrame.from_dict({
            **{c: [None] for c in pk_cols + value_cols},
            "$m.op": [OP_TOMBSTONE],
            "$m.source_ts": [run_ts],
            "$m.bronze_ts": [run_ts],
            "$m.pk": [pk_cols],
        }).with_columns([col(name).cast(dt) for name, dt in all_dtypes.items()])

        seed = head.select(pk_cols + value_cols).with_columns([
            lit(OP_INSERT).alias("$m.op"),
            lit(run_ts).alias("$m.source_ts"),
            lit(run_ts).alias("$m.bronze_ts"),
            lit(pk_cols).alias("$m.pk"),
        ])
        return concat([erase, seed], how="diagonal_relaxed")

    # Incremental: diff prev vs. head → synthetic I/U/D.
    inserts = head.join(prev, on=pk_cols, how="anti").with_columns(
        lit(OP_INSERT).alias("$m.op")
    )
    deletes = prev.join(head, on=pk_cols, how="anti").with_columns(
        lit(OP_DELETE).alias("$m.op")
    )

    joined = head.join(prev, on=pk_cols, how="inner", suffix="_prev")
    diff_expr = reduce(
        or_,
        [col(c).ne_missing(col(f"{c}_prev")) for c in value_cols],
    )
    # Drop the prev-side value columns now that the diff has been
    # computed — downstream consumers expect the bronze stream to
    # carry one column per value name, not two.
    prev_cols = [f"{c}_prev" for c in value_cols]
    updates = (
        joined
        .filter(diff_expr)
        .drop(prev_cols)
        .with_columns(lit(OP_UPDATE).alias("$m.op"))
    )

    return concat([inserts, updates, deletes], how="diagonal_relaxed").with_columns([
        lit(run_ts).alias("$m.source_ts"),
        lit(run_ts).alias("$m.bronze_ts"),
        lit(pk_cols).alias("$m.pk"),
    ])
