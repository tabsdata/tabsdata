#
# Copyright 2026 Tabsdata Inc.
#

import logging
from datetime import datetime, timezone

from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe.functions import col, concat, lit

logger = logging.getLogger(__name__)


def build_period_snapshot_bronze(
    new: list[TableFrame],
    pk: list[dict],
) -> TableFrame:
    """
    Generate synthetic bronze layer by treating every new arrival as a full snapshot for
    a specific period.

    Args:
        new: table versions created since the last execution of the transformer.
        pk: ordered list of single-key dicts mapping each primary-key column name to its
            tabsdata dtype. Example:
                [{"tenant_id": td.String}, {"entity_id": td.Int64}]
    Returns:
        A TableFrame representing the new arrivals as full snapshots for a specific
        period.
    """
    pk_schema: dict[str, type] = {
        name: dtype for entry in pk for name, dtype in entry.items()
    }
    pk_cols = list(pk_schema.keys())
    # Drop absent commits (``None`` entries) and short-circuit when nothing
    # landed this period — concat has no frame to build from.
    frames = [frame for frame in (new or []) if frame is not None]
    raw = concat(frames, how="diagonal_relaxed") if frames else None

    if raw is None:
        logger.debug("No new data; returning None")
        return None

    run_ts = datetime.now(tz=timezone.utc)
    return raw.with_columns([
        # The periodic publisher stamps "$m.period" on every row with the
        # period the snapshot belongs to (see Pattern 5). Mirror that into
        # "$m.source_ts" so downstream consumers can use the same column
        # name conventions as the CDC / snapshot-diff / append-only bronze
        # flavors. Original "$m.period" stays on the row.
        col("$m.period").alias("$m.source_ts"),
        lit(run_ts).alias("$m.bronze_ts"),
        lit(pk_cols).alias("$m.pk"),
    ])
