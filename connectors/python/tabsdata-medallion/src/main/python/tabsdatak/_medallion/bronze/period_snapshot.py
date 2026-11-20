#
# Copyright 2026 Tabsdata Inc.
#

import logging
from datetime import datetime, timezone

from tabsdatak._medallion.constants import (
    MEDALLION_SYSTEM_COLUMN_BRONZE_TS,
    MEDALLION_SYSTEM_COLUMN_PERIOD,
    MEDALLION_SYSTEM_COLUMN_PK,
    MEDALLION_SYSTEM_COLUMN_SOURCE_TS,
)
from tabsdatak._medallion.landed import landed
from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe.functions import col, lit

logger = logging.getLogger(__name__)


def build_period_snapshot_bronze(
    new: list[TableFrame],
    pk: list[dict],
    head: TableFrame | None = None,
) -> TableFrame | None:
    """
    Generate synthetic bronze layer by treating every new arrival as a full snapshot for
    a specific period.

    Args:
        new: table versions created since the last execution of the transformer.
        pk: ordered list of single-key dicts mapping each primary-key column name to its
            tabsdata dtype. Example:
                [{"tenant_id": td.String}, {"entity_id": td.Int64}]
        head: the table of `new` read at ``@HEAD``, None before it has data.
    Returns:
        A TableFrame representing the new arrivals as full snapshots for a specific
        period. When no version came, no rows with the bronze columns, and None
        while there is no `head`.
    """
    pk_schema: dict[str, type] = {
        name: dtype for entry in pk for name, dtype in entry.items()
    }
    pk_cols = list(pk_schema.keys())
    raw = landed(new, head)
    if raw is None:
        logger.debug("No new data and no head; returning None")
        return None

    run_ts = datetime.now(tz=timezone.utc)
    return raw.with_columns([
        # The periodic publisher stamps MEDALLION_SYSTEM_COLUMN_PERIOD on every row with
        # the
        # period the snapshot belongs to (see Pattern 5). Mirror that into
        # MEDALLION_SYSTEM_COLUMN_SOURCE_TS so downstream consumers can use the same
        # column
        # name conventions as the CDC / snapshot-diff / append-only bronze
        # flavors. Original MEDALLION_SYSTEM_COLUMN_PERIOD stays on the row.
        col(MEDALLION_SYSTEM_COLUMN_PERIOD).alias(MEDALLION_SYSTEM_COLUMN_SOURCE_TS),
        lit(run_ts).alias(MEDALLION_SYSTEM_COLUMN_BRONZE_TS),
        lit(pk_cols).alias(MEDALLION_SYSTEM_COLUMN_PK),
    ])
