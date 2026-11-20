#
# Copyright 2026 Tabsdata Inc.
#

from tabsdatak._medallion.landed import landed
from tabsdatak._medallion.silver.scd import _strip_bronze_metadata
from tabsdatak.tableframe import TableFrame


def build_periodic_snapshot_silver(
    bronze: list[TableFrame | None] | None,
    head: TableFrame | None = None,
) -> TableFrame | None:
    """
    Periodic snapshot silver — pure projection: drops bronze metadata from the
    input.

    Args:
        bronze: periodic snapshot bronze commits.
        head: the bronze table read at ``@HEAD``, None before it has data.

    Returns:
        Cleansed periodic snapshot silver TableFrame. When no bronze version
        came, no rows with the silver columns, and None while there is no
        ``head``.
    """
    raw = landed(bronze, head)
    if raw is None:
        return None
    return _strip_bronze_metadata(raw)
