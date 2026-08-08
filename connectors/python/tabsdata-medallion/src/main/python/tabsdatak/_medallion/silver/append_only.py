#
# Copyright 2026 Tabsdata Inc.
#

from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe.functions import col, concat


def build_append_only_silver(
    bronze_new: list[TableFrame],
    silver_head: TableFrame | None,
) -> TableFrame:
    """
    Cleanse + dedup append-only bronze rows for the silver layer.

    Drops bronze-only metadata columns (``$m.op`` / ``$m.bronze_ts`` /
    ``$m.pk``; ``$m.source_ts`` is preserved so downstream time-travel
    queries keep the source event time), dedups within the incoming
    batch on the composite PK, then anti-joins against the existing
    silver state to drop any PK already written to silver. The
    anti-join catches at-least-once cross-commit duplicates — the
    same event landing in two bronze commits across two separate
    silver fires.

    The PK column list is read off the bronze rows' ``$m.pk`` column
    (stamped by ``build_append_only_cdc_bronze`` from the bronze
    transformer's `pk` arg). All bronze rows in a single fire share
    the same value, so reading from the first row is sufficient. This
    removes the need for the silver transformer to re-declare the PK.
    ``$m.pk`` is preserved on the output so downstream consumers can
    keep reading the PK column list from the row.

    Append-only silver assumes each composite PK is observed at most
    once across the lifetime of the event stream (transaction IDs,
    audit row IDs, IoT observation keys, etc.). The bronze publisher
    is expected to deliver non-null PK columns.

    Args:
        bronze_new: bronze commits landed since the last silver fire
            (typically read via ``<bronze_table>@NEW``). Must carry
            the ``$m.pk`` column the bronze helper stamps.
        silver_head: latest committed silver state, used for cross-
            commit dedup. None on the first-ever silver fire.

    Returns:
        A TableFrame of new silver rows to append — bronze-only
        op/timestamp metadata dropped (``$m.op`` / ``$m.bronze_ts``),
        deduped within the batch, free of any PK already present in
        ``silver_head``. ``$m.source_ts`` and ``$m.pk`` are preserved.
    """
    # Drop absent commits (``None`` entries) and short-circuit when
    # nothing landed this fire — concat has no frame to build from.
    frames = [frame for frame in (bronze_new or []) if frame is not None]
    raw = concat(frames, how="diagonal_relaxed") if frames else None

    if raw is None:
        return None

    # ``$m.pk`` is stamped by the bronze helper as a List[String]
    # column carrying the PK column names on every row. Read the
    # first row's value as the dedup key for this fire. ``.item()``
    # can't be used here — its implementation aggregates via
    # ``.min()``, which polars rejects for List dtypes — so we go
    # through pandas to extract the cell value.
    pk_cols = list(raw.select(col("$m.pk"))._to_pandas()["$m.pk"].iloc[0])

    cleaned = raw.drop(["$m.op", "$m.bronze_ts"])

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
