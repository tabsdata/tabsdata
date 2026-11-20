#
# Copyright 2026 Tabsdata Inc.
#

from tabsdatak._medallion.constants import (
    MEDALLION_SYSTEM_COLUMN_BRONZE_TS,
    MEDALLION_SYSTEM_COLUMN_OP,
    MEDALLION_SYSTEM_COLUMN_PK,
)
from tabsdatak._medallion.landed import landed
from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe.functions import col


def build_append_only_silver(
    bronze_new: list[TableFrame],
    silver_head: TableFrame | None,
) -> TableFrame | None:
    """
    Cleanse + dedup append-only bronze rows for the silver layer.

    Drops bronze-only metadata columns (``@td.medallion.op`` /
    ``@td.medallion.bronze_ts`` /
    ``@td.medallion.pk``; ``@td.medallion.source_ts`` is preserved so downstream time-
    travel
    queries keep the source event time), dedups within the incoming
    batch on the composite PK, then anti-joins against the existing
    silver version to drop any PK it holds. Each silver version holds
    only its fire's new rows, so the anti-join catches a duplicate
    only when it landed in the previous fire's bronze commits, not a
    PK written to silver earlier.

    The PK column list is read off the bronze rows' ``@td.medallion.pk`` column
    (stamped by ``build_append_only_cdc_bronze`` from the bronze
    transformer's `pk` arg). All bronze rows in a single fire share
    the same value, so reading from the first row is sufficient. This
    removes the need for the silver transformer to re-declare the PK.
    ``@td.medallion.pk`` is preserved on the output so downstream consumers can
    keep reading the PK column list from the row.

    Append-only silver assumes each composite PK is observed at most
    once across the lifetime of the event stream (transaction IDs,
    audit row IDs, IoT observation keys, etc.). The bronze publisher
    is expected to deliver non-null PK columns.

    Args:
        bronze_new: bronze commits landed since the last silver fire
            (typically read via ``<bronze_table>@NEW``). Must carry
            the ``@td.medallion.pk`` column the bronze helper stamps.
        silver_head: latest committed silver state, used for cross-
            commit dedup. None on the first-ever silver fire.

    Returns:
        A TableFrame of new silver rows to append — bronze-only
        op/timestamp metadata dropped (``@td.medallion.op`` /
        ``@td.medallion.bronze_ts``),
        deduped within the batch, free of any PK already present in
        ``silver_head``. ``@td.medallion.source_ts`` and ``@td.medallion.pk`` are
        preserved. When no bronze version came, no rows with the columns of
        ``silver_head``, and None while there is no ``silver_head``: a None
        version would hand its ``@NEW`` readers the previous rows again.
    """
    raw = landed(bronze_new)
    if raw is None:
        return None if silver_head is None else silver_head.clear()

    cleaned = raw.drop([MEDALLION_SYSTEM_COLUMN_OP, MEDALLION_SYSTEM_COLUMN_BRONZE_TS])
    if raw.first()._to_polars_df().is_empty():
        return cleaned

    # ``@td.medallion.pk`` is stamped by the bronze helper as a List[String]
    # column carrying the PK column names on every row. Read the
    # first row's value as the dedup key for this fire. ``.item()``
    # can't be used here — its implementation aggregates via
    # ``.min()``, which polars rejects for List dtypes — so we go
    # through pandas to extract the cell value.
    pk_cols = list(
        raw
        .select(col(MEDALLION_SYSTEM_COLUMN_PK))
        ._to_pandas()[MEDALLION_SYSTEM_COLUMN_PK]
        .iloc[0]
    )

    deduped = cleaned.unique(subset=pk_cols)

    if silver_head is None or silver_head.first()._to_polars_df().is_empty():
        return deduped

    # Project silver_head to PK cols only so the join doesn't drag the
    # full silver row into the shuffle. Explicit beats relying on the
    # optimizer for a join that runs every silver fire.
    return deduped.join(
        silver_head.select(pk_cols),
        on=pk_cols,
        how="anti",
    )
