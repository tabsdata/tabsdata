#
# Copyright 2026 Tabsdata Inc.
#

import logging
from datetime import datetime, timezone

import polars as pl

from tabsdatak._medallion.bronze.cdc import OP_INSERT
from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe.functions import col, concat, lit

logger = logging.getLogger(__name__)


def build_append_only_cdc_bronze(
    new: list[TableFrame],
    pk: list[dict],
    event_ts_col: str,
) -> TableFrame:
    """
    Generate synthetic I-only CDC bronze layer by treating every new arrival as an
    insert.

    Args:
        new: table versions created since the last execution of the transformer.
        pk: ordered list of single-key dicts mapping each primary-key column name to its
            tabsdata dtype. Example:
                [{"tenant_id": td.String}, {"entity_id": td.Int64}]
        event_ts_col: name of the timestamp column in `new` that indicates the event
            time.
    Returns:
        A TableFrame with synthetic I-only CDC events representing the new arrivals.
    """
    pk_schema: dict[str, type] = {
        name: dtype for entry in pk for name, dtype in entry.items()
    }
    pk_cols = list(pk_schema.keys())
    raw = concat(new, how="diagonal_relaxed") if new else None

    if raw is None:
        logger.debug("No new data; returning None")
        return None

    # Validate event_ts_col references a Datetime column.
    ts_dtype = raw.schema[event_ts_col]
    if not isinstance(ts_dtype, pl.Datetime):
        raise TypeError(
            f"event_ts_col {event_ts_col!r} must reference a Datetime "
            f"column; got dtype {ts_dtype!r}"
        )

    # Normalize $m.source_ts to UTC based on the input's time_zone:
    #  - None (naive)         → ``dt.replace_time_zone("UTC")`` attaches UTC,
    #                           wall-clock preserved.
    #  - "UTC"                → no-op.
    #  - Any other tz-aware   → ``dt.convert_time_zone("UTC")`` preserves the
    #                           instant, wall-clock changes to UTC.
    # Use the renamed name "$m.source_ts" inside with_columns — the rename
    # has already executed by the time these expressions evaluate.
    ts_tz = ts_dtype.time_zone
    if ts_tz is None:
        ts_expr = col("$m.source_ts").dt.replace_time_zone("UTC")
    elif str(ts_tz) == "UTC":
        ts_expr = col("$m.source_ts")
    else:
        ts_expr = col("$m.source_ts").dt.convert_time_zone("UTC")

    return raw.rename({event_ts_col: "$m.source_ts"}).with_columns([
        lit(OP_INSERT).alias("$m.op"),
        ts_expr,
        lit(datetime.now(tz=timezone.utc)).alias("$m.bronze_ts"),
        lit(pk_cols).alias("$m.pk"),
    ])
